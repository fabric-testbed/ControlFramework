# FABRIC Control Framework — Architecture

## System Overview

The FABRIC Control Framework (`fabric_cf`) is a distributed resource management system built on an **actor model**. Three actor types collaborate over Kafka to manage the lifecycle of user experiments (slices) on the FABRIC testbed:

```
                         ┌──────────────┐
                         │    Portal /   │
                         │  FABlib / AI  │
                         │    Agents     │
                         └──────┬───────┘
                                │ REST API
                         ┌──────▼───────┐
                         │ Orchestrator  │
                         │  (Controller) │
                         └──┬────────┬──┘
                  Kafka     │        │     Kafka
               ┌────────────▼─┐  ┌───▼────────────┐
               │    Broker    │  │   Authority     │
               │              │  │ (Aggregate Mgr) │
               └──────────────┘  └────────┬────────┘
                                          │ Ansible
                                   ┌──────▼──────┐
                                   │  Physical    │
                                   │  Resources   │
                                   └─────────────┘
```

- **Orchestrator** — User-facing. Accepts slice requests, performs topology embedding, coordinates with Broker and Authority.
- **Broker** — Aggregates resource availability from multiple Authorities. Issues resource tickets (promises).
- **Authority (Aggregate Manager)** — Manages physical resources at a site. Provisions VMs, networking, and components via Ansible handlers.

Each actor runs as a separate container. All inter-actor communication is asynchronous over Kafka. State is persisted in PostgreSQL. Topology/resource models are stored in Neo4j.

## Actor Model and Kernel

### Event Loop

The core of each actor is the **Kernel** (`actor/core/kernel/kernel.py`). It processes three event types in a tick-driven loop:

| Event Type | Source | Purpose |
|---|---|---|
| **TickEvent** | Internal timer | Periodic work — probe reservations, process queues |
| **InterActorEvent** | Kafka messages | RPC from other actors (ticket requests, lease updates) |
| **SyncEvent** | Internal | Synchronization between threads |

The tick interval is configurable per actor (milliseconds). Each tick, the kernel:
1. Processes incoming Kafka messages
2. Probes pending reservations for state transitions
3. Runs policy-specific periodic work (e.g., broker allocation cycles)

### Singletons

| Singleton | Location | Purpose |
|---|---|---|
| `GlobalsSingleton` | `actor/core/container/globals.py` | Container-wide state: config, logger, JWT validator, timer |
| `ActorRegistrySingleton` | `actor/core/registry/actor_registry.py` | Registers actors, provides lookup by GUID/name |
| `OrchestratorKernelSingleton` | `orchestrator/core/orchestrator_kernel.py` | Orchestrator-specific: broker proxy, CBM, scheduling threads |
| `EventLoggerSingleton` | `actor/core/common/event_logger.py` | Audit logging for slice/sliver lifecycle events |

## Slice Lifecycle

A **Slice** groups related **Reservations**. Each reservation tracks the state machine for one resource request. Reservations produce **Units** — provisioned resources with attached slivers (FIM models).

### Creation Flow

```
User POST /slices/create
  │
  ▼
OrchestratorHandler.create_slice()
  ├── Validate topology (ExperimentTopology)
  ├── Store ASM graph in Neo4j
  ├── Authorize via PDP (AccessChecker)
  ├── Create OrchestratorSliceWrapper
  ├── Compute reservations from slivers
  │
  ├── [Future lease?] → AdvanceSchedulingThread
  │     ├── determine_future_lease_time()
  │     │     ├── ResourceTracker per candidate host
  │     │     ├── find_next_available() sliding window
  │     │     └── find_common_start_time() across all reservations
  │     ├── Update lease start/end
  │     └── Queue on SliceDeferThread
  │
  └── [Immediate?] → SliceDeferThread
        └── Demand reservations from Broker
```

### Reservation State Machine

```
Nascent ──► Ticketing ──► Ticketed ──► Redeeming ──► Active
   │                         │                          │
   └──► Failed               └──► Failed                └──► Closing ──► Closed
                                                        └──► Failed
```

| State | Meaning |
|---|---|
| **Nascent** | Created, not yet sent to broker |
| **Ticketed** | Broker issued a ticket (resource promise) |
| **Active** | Authority provisioned the resource |
| **ActiveTicketed** | Active but being extended/modified |
| **Closed** | Resource released |
| **Failed** | Error at any stage |

**Key classes**: `Reservation` (`actor/core/kernel/reservation.py`), `ReservationClient`, `ReservationServer`

## Inter-Actor Communication

### Kafka Message Bus

Actors communicate via Kafka topics using Avro-serialized messages. Each actor has an inbound topic; proxies send to remote actors' topics.

```
Orchestrator                    Broker                      Authority
     │                            │                            │
     │──── TicketRequest ────────►│                            │
     │◄─── Ticket ────────────────│                            │
     │                            │                            │
     │──── RedeemRequest ─────────┼───────────────────────────►│
     │◄─── Lease ─────────────────┼────────────────────────────│
```

**Proxy layer** (`actor/core/proxies/kafka/`):
- `KafkaBrokerProxy` — Orchestrator→Broker calls (ticket, extend, query)
- `KafkaAuthorityProxy` — Broker/Orchestrator→Authority calls (redeem, close)
- `translate.py` — Avro message serialization/deserialization

**RPC types**: Delegation, Query, Reservation, POA (Proof of Authority)

**Schema definitions**: `/MessageBusSchema/fabric_mb/message_bus/schema/` (Avro `.avsc` files)

## Resource Model (FIM)

The **Fabric Information Model** (FIM) represents resources as typed graph models stored in Neo4j.

### Model Hierarchy

| Model | Owner | Purpose |
|---|---|---|
| **ARM** (Aggregate Resource Model) | Authority | Physical site resources — nodes, components, links |
| **BQM** (Broker Query Model) | Broker | Available resources delegated from one Authority |
| **CBM** (Combined Broker Model) | Broker | Merged BQMs from all sites — global resource view |
| **ASM** (Allocated Slice Model) | Orchestrator | User slice topology |

**Flow**: Authority advertises ARM → Broker aggregates into CBM → Orchestrator queries CBM for embedding → creates ASM.

**Key class**: `FimHelper` (`actor/fim/fim_helper.py`) — graph operations, candidate node selection, embedding.

**BQM aggregation plugin**: `AggregatedBQMPlugin` (`actor/fim/plugins/broker/aggregate_bqm_plugin.py`) — merges BQMs from multiple sites into the CBM.

### Sliver Types

| Sliver | FIM Class | Description |
|---|---|---|
| VM/Bare Metal | `NodeSliver` | Compute node with cores, RAM, disk |
| GPU, SmartNIC, FPGA, NVME | `ComponentSliver` | Attached components |
| L2PTP, L2STS, FABNet, etc. | `NetworkServiceSliver` | Network services |
| NIC port | `InterfaceSliver` | Network interface with VLAN, BDF |

## Policy System

Policies implement `ABCPolicy` and make allocation decisions per actor type.

### Broker Policy

`BrokerSimplerUnitsPolicy` (`actor/core/policy/broker_simpler_units_policy.py`):
- Extends `BrokerCalendarPolicy` for time-aware allocation
- Maintains the CBM and allocation queues
- **Allocation algorithms**: FirstFit, BestFit, WorstFit, Random
- **Time-aware**: Queries overlapping reservations within the requested `[lease_start, lease_end]` window
- **Calendar-based scheduling**: `BrokerCalendar` indexes requests by allocation cycle

### Authority Policy

`AuthorityCalendarPolicy` (`actor/core/policy/authority_calendar_policy.py`):
- Manages physical resource allocation from the ARM
- Calendar-based reservation tracking
- Integrates with handlers for provisioning

### Orchestrator Policy

`ControllerTicketReviewPolicy` (`actor/core/policy/controller_ticket_review_policy.py`):
- Reviews tickets received from broker
- Makes embedding decisions
- Coordinates redeem calls to authority

## Database Layer

### PostgreSQL

Each actor persists state in PostgreSQL via SQLAlchemy (`actor/db/psql_database.py`).

**Core tables**: Actors, Slices, Reservations, Delegations, Units, Proxies, Poas, Metrics, Sites, Components, Links

**Session management**: Thread-local `scoped_session` for concurrent request handling.

### Neo4j

Topology graphs (ARM, CBM, BQM, ASM) are stored in Neo4j. Each actor type may use a separate Neo4j instance. The test environment runs 5 instances for isolation.

## Orchestrator REST API

### Framework

Connexion/Flask-based OpenAPI 3.0 server (`orchestrator/swagger_server/`).

**Layers**:
```
swagger.yaml (OpenAPI spec)
  └── controllers/ (connexion routing — thin delegation)
        └── response/ (business logic — auth, validation, handler calls)
              └── OrchestratorHandler (core operations)
```

### Key Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/slices/create` | Create a new slice |
| GET | `/slices` | List slices |
| GET | `/slices/{slice_id}` | Get slice details |
| DELETE | `/slices/delete/{slice_id}` | Delete a slice |
| PUT | `/slices/renew/{slice_id}` | Renew slice lease |
| PUT | `/slices/modify/{slice_id}` | Modify a slice |
| GET | `/resources` | Get available resources (FIM graph) |
| GET | `/resources/summary` | Get resource summary (JSON) |
| GET | `/resources/calendar` | Get resource availability calendar |
| POST | `/resources/find-slot` | Find available time windows for resources |
| GET | `/slivers` | List slivers |
| POST | `/poas/create/{sliver_id}` | Perform operational action on a sliver |
| GET | `/metrics/overview` | Get usage metrics |

### Authentication

1. **JWT validation**: Incoming Bearer token verified against CILogon JWKS URL (`fss_utils.jwt_validate.JWTValidator`)
2. **PDP authorization**: Resource-level access control via AuthZForce PDP (`actor/security/pdp_auth.py`)
   - Builds authorization request with resource attributes from FIM slivers
   - Actions: query, create, modify, delete, renew, POA
   - Project tags control component access (e.g., `Component.FPGA`)

## Handler / Provisioning System

The Authority provisions resources via Ansible playbooks.

**Handler interface** (`actor/handlers/handler_base.py`):
```
create(unit)  → provisions resource
modify(unit)  → reconfigures resource
delete(unit)  → tears down resource
poa(unit)     → operational action (reboot, etc.)
```

**Handler processor** (`actor/core/plugins/handlers/ansible_handler_processor.py`):
- Invokes playbooks with configuration tokens
- Manages async provisioning results
- Reports success/failure back to reservation state machine

**Unit** (`actor/core/core/unit.py`): Represents a single provisioned resource. Contains the sliver (FIM model) and tracks provisioning state.

## Advance Scheduling

When a user submits a slice with a future `lease_start_time`, the orchestrator determines the best start time:

### Components

| Component | File | Purpose |
|---|---|---|
| `AdvanceSchedulingThread` | `orchestrator/core/advance_scheduling_thread.py` | Async thread that processes future slices |
| `SliceDeferThread` | `orchestrator/core/slice_defer_thread.py` | Defers reservation demand until start time determined |
| `ResourceTracker` | `orchestrator/core/resource_tracker.py` | Per-host hourly capacity tracker |

### Flow

1. `AdvanceSchedulingThread` receives slice with future lease range
2. Calls `determine_future_lease_time()` on `OrchestratorKernel`
3. For each compute reservation, queries candidate hosts from CBM
4. Builds `ResourceTracker` per host — loads existing reservations from broker
5. `find_next_available()` — hourly sliding window checks capacity + components
6. `find_common_start_time()` — intersects available windows across all reservations
7. Updates slice lease times, queues on `SliceDeferThread`

**Limitation**: Currently only handles compute (`NodeSliver`). Network slivers and facility ports are skipped. See [ROADMAP.md](ROADMAP.md) items 1-2.

## Reports System

A separate analytics server (`/reports/`) with its own PostgreSQL database, refreshed hourly from the orchestrator/broker.

### Architecture

```
Orchestrator ──hourly sync──► Reports PostgreSQL ◄── Reports API Server
                                                        │
                                                   /calendar
                                                   /calendar/find-slot
                                                   /slices, /slivers, /users, /projects
```

### Database

**Tables**: Slices, Slivers, Hosts, Sites, Users, Projects, Components, Interfaces, HostCapacities, LinkCapacities, FacilityPortCapacities, Membership

### Calendar Feature

`GET /calendar` — Returns per-slot resource availability (capacity, allocated, available) for hosts, links, and facility ports over a time range with configurable intervals (hour, day, week).

### Find-Slot Feature

`POST /calendar/find-slot` — Given a resource request (compute + links + facility ports), finds the earliest time windows where all resources are simultaneously available for a given duration.

**Algorithm**: Hourly sliding window with greedy bin-packing for compute. Multiple compute requests are packed across hosts cumulatively — two 32-core requests won't both "see" the same 32 free cores.

**Orchestrator proxy**: `POST /resources/find-slot` forwards to the reports API. Users interact via the orchestrator.

## Configuration

YAML-based per-actor configuration with sections:

| Section | Contents |
|---|---|
| `runtime` | Kafka broker URLs, message bus topics |
| `logging` | Log directory, level, rotation |
| `oauth` | JWKS URL, token validation, CILogon settings |
| `database` | PostgreSQL connection parameters |
| `neo4j` | Neo4j connection array (host, user, password, import dir) |
| `pdp` | AuthZForce PDP URL for authorization |
| `actor` | Actor type, name, GUID, policy class, resource substrates, handler controls |
| `peers` | Remote actor Kafka topic mappings |

## Docker Deployment

### Images

| Dockerfile | Image |
|---|---|
| `Dockerfile-orchestrator` | Orchestrator |
| `Dockerfile-broker` | Broker |
| `Dockerfile-auth` | Authority / Aggregate Manager |

### Test Infrastructure (`docker-compose-test.yaml`)

| Service | Port | Purpose |
|---|---|---|
| broker1 | 19092 | Kafka (KRaft mode) |
| schemaregistry | 8081 | Confluent Schema Registry |
| database | 5432 | PostgreSQL 12.3 |
| neo4j1–neo4j5 | 7687, 8687, 9687, 6687, 10687 | Neo4j instances (one per actor) |
| pdp | — | AuthZForce PDP |

## Key File Reference

| Component | Path |
|---|---|
| Kernel event loop | `actor/core/kernel/kernel.py` |
| Reservation state machine | `actor/core/kernel/reservation.py` |
| Orchestrator handler | `orchestrator/core/orchestrator_handler.py` |
| Broker allocation policy | `actor/core/policy/broker_simpler_units_policy.py` |
| Authority allocation policy | `actor/core/policy/authority_calendar_policy.py` |
| Kafka proxies | `actor/core/proxies/kafka/` |
| FIM helper | `actor/fim/fim_helper.py` |
| BQM aggregation plugin | `actor/fim/plugins/broker/aggregate_bqm_plugin.py` |
| PostgreSQL persistence | `actor/db/psql_database.py` |
| Ansible handler processor | `actor/core/plugins/handlers/ansible_handler_processor.py` |
| REST API controllers | `orchestrator/swagger_server/controllers/` |
| Access control | `actor/security/access_checker.py`, `actor/security/pdp_auth.py` |
| Advance scheduling | `orchestrator/core/advance_scheduling_thread.py` |
| Resource tracker | `orchestrator/core/resource_tracker.py` |
| Reports DB manager | `reports/reports_api/database/db_manager.py` |
| Reports calendar controller | `reports/reports_api/response_code/calendar_controller.py` |
| Management CLI | `ManagementCli/fabric_mgmt_cli/managecli/` |
| Message bus schemas | `MessageBusSchema/fabric_mb/message_bus/schema/` |
