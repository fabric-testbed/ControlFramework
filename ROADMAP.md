# ROADMAP — Resource Calendar & Scheduling

Future improvements related to resource availability calendar, find-slot, and advance scheduling.

## 1. Extend `determine_future_lease_time` to handle links and facility ports

**Current state**: `determine_future_lease_time` in `orchestrator_kernel.py` only processes `NodeSliver` (compute). Network slivers and facility port slivers are silently skipped (`if not isinstance(requested_sliver, NodeSliver): continue`).

**Improvement**: Extend the `ResourceTracker` and `determine_future_lease_time` to also check link bandwidth availability and facility port VLAN availability when auto-adjusting lease times for future slices. This would prevent the orchestrator from picking a start time where compute fits but a required link is saturated.

**Files**: `orchestrator/core/resource_tracker.py`, `orchestrator/core/orchestrator_kernel.py`

## 2. Add bin-packing to `determine_future_lease_time`

**Current state**: Each compute reservation is checked independently against candidate hosts. Two VMs that each need 32 cores could both "see" the same 32 free cores on the same host, leading to a false positive.

**Improvement**: Track cumulative demand across compute reservations (greedy bin-packing), similar to what `find_slot()` in the reports DB does. This ensures the auto-adjusted start time actually has enough capacity for all VMs simultaneously.

**Files**: `orchestrator/core/orchestrator_kernel.py`, `orchestrator/core/resource_tracker.py`

## 3. FABlib integration for `find-slot`

**Current state**: Users must craft raw HTTP POST requests to `POST /resources/find-slot` via the orchestrator.

**Improvement**: Add a `find_available_slot()` convenience method to `fabrictestbed-extensions` (FABlib) that wraps the orchestrator endpoint. This lets users discover available windows directly from their Jupyter notebooks before submitting slices.

**Repo**: `fabrictestbed-extensions` (external)

## 4. OrchestratorProxy client method for `find-slot`

**Current state**: The orchestrator exposes `POST /resources/find-slot` but the Python `OrchestratorProxy` client class (used by FABlib) does not have a corresponding method.

**Improvement**: Add `find_slot()` to `OrchestratorProxy` so FABlib and other Python clients can call it without raw HTTP.

**Files**: `orchestrator/orchestrator_proxy.py` (if applicable)

## 5. Portal integration for find-slot

**Current state**: The calendar UI shows per-slot availability. Users must manually inspect it to find windows.

**Improvement**: Add a "Find Available Window" form in the portal that calls `POST /resources/find-slot` and displays matching time windows. Users could then click to pre-fill slice creation with the selected window.

## 6. Unified scheduling: replace `determine_future_lease_time` with reports-backed find-slot

**Current state**: Two parallel systems find available time windows — `determine_future_lease_time` (live broker data, compute only) and `find_slot` (reports DB, compute + links + facility ports).

**Improvement**: Once `find_slot` is proven reliable and the reports DB refresh cadence is sufficient, consider having the orchestrator's advance scheduling thread use the reports API instead of querying the broker directly. This would unify the logic and extend coverage to all resource types. Requires ensuring reports data freshness is acceptable for server-side scheduling decisions.

**Trade-off**: Reports data is ~1 hour stale vs live broker queries. May not be suitable for time-critical allocation decisions.

## 7. Smarter sliding window with skip-ahead optimization

**Current state**: `find_slot()` in reports `db_manager.py` checks every hour sequentially. For a 30-day range that's 720 iterations, each checking `duration` hours.

**Improvement**: When a window fails at hour `dh` within the duration, skip ahead to the next hour where the blocking sliver ends rather than advancing by 1. This reduces iterations significantly for heavily loaded time ranges.

**Files**: `reports_api/database/db_manager.py` (`_check_window`, `find_slot`)

## 8. Async and scalability improvements for high-concurrency workloads

**Context**: As AI agents and automated workflows increasingly interact with FABRIC, the orchestrator and reports APIs may face high concurrent request volumes. The current synchronous, single-threaded-per-request architecture will become a bottleneck.

### 8a. Orchestrator API — async framework and connection pooling

**Current state**: The orchestrator uses Connexion/Flask (synchronous WSGI). Each incoming request blocks a worker thread for the entire duration, including downstream calls to broker, reports API, and database. Under high concurrency from AI agents making rapid slice/find-slot/resource queries, worker threads will be exhausted.

**Improvements**:
- Migrate to an async-capable framework (e.g., Connexion 3.x with ASGI, or FastAPI) so request handlers can `await` downstream calls without blocking threads
- Use `aiohttp` or `httpx.AsyncClient` with connection pooling for outbound calls to reports API and broker
- Add request queuing and backpressure (return `429 Too Many Requests` with `Retry-After` header) to protect downstream services
- Consider async SQLAlchemy (`asyncio` extension) for database operations

**Files**: `orchestrator/swagger_server/`, `orchestrator/core/orchestrator_handler.py`

### 8b. Reports API — async queries and caching

**Current state**: Reports API uses Flask (synchronous). Complex queries like `find_slot` with 30-day ranges scan 720+ hourly slots against multiple DB queries. Under concurrent load, DB connection pool and CPU become bottlenecks.

**Improvements**:
- Parallelize independent DB queries (host capacities, link capacities, facility port capacities, slivers) using `concurrent.futures.ThreadPoolExecutor` or async SQLAlchemy
- Add short-lived response caching (e.g., 5-minute TTL) for `find_slot` results — since reports DB only refreshes hourly, identical queries within minutes will return the same result
- Pre-compute and cache hourly allocation arrays on reports DB refresh rather than computing on every request
- Consider read replicas for the reports PostgreSQL to distribute query load

**Files**: `reports_api/database/db_manager.py`, `reports_api/response_code/calendar_controller.py`

### 8c. Orchestrator advance scheduling — parallel host queries

**Current state**: `determine_future_lease_time` queries broker reservations per-host sequentially. For slices with many candidate nodes across multiple sites, this is O(nodes) sequential round-trips.

**Improvements**:
- Query candidate host reservations in parallel using `concurrent.futures.ThreadPoolExecutor` — each `get_reservations(node_id=c, ...)` call is independent
- Batch reservation queries where possible (query all candidate nodes in one DB call with `node_id IN (...)`)

**Files**: `orchestrator/core/orchestrator_kernel.py`, `orchestrator/core/resource_tracker.py`

### 8d. Client-side — async and rate-limited clients

**Current state**: `ReportsApi` and `OrchestratorProxy` clients use synchronous `requests` library. AI agents making many concurrent calls will block on each.

**Improvements**:
- Provide async client variants using `httpx.AsyncClient` for both `ReportsApi` and `OrchestratorProxy`
- Add client-side rate limiting and retry with exponential backoff to be a good citizen under load
- FABlib: provide both `find_available_slot()` (sync, for notebooks) and `async_find_available_slot()` (async, for agent frameworks)

**Files**: `reports_client/fabric_reports_client/reports_api.py`, `orchestrator/orchestrator_proxy.py`

### 8e. Long-running find-slot — async job pattern

**Current state**: Large find-slot queries (30-day range, many resources) may take seconds. Under high concurrency, these tie up worker threads.

**Improvements**:
- For requests exceeding a threshold (e.g., >7 day range with >3 resources), return `202 Accepted` with a job ID
- Process in background worker (Celery, or simple thread pool)
- Client polls `GET /calendar/find-slot/{job_id}` for result
- Add TTL-based cleanup for completed jobs

**Files**: `reports_api/response_code/calendar_controller.py`, `orchestrator/core/orchestrator_handler.py`
