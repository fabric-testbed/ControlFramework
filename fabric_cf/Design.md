# Design
## Create Slice
This section captures the complete flow of a simple Create Slice Request provisioning a single VM with couple of PCI 
devices.
### Orchestrator
Complete Flow for all messages and processing at Orchestrator is described below:
- OrchestratorHandler class:: create_slice processes incoming Create Slice as follows:
  - Query Resources via Broker to get BQM by sending Query/QueryResponse exchange in OrchestratorHandler::discover_types()
  - Creates RequestWorkflow class (Place holder for any future embedding)
  - Invokes RequestWorkflow::run responsible for translating ASM into individual reservations representing slivers using BQM
  - Adds the computed reservations to queue of SliceDeferThread class
  - Responds back to REST API with the list of computed reservations 
- SliceDeferThread class waits for
  - For each slice:
    - For each reservation:
      - Send Ticket Message to Broker
      - On reciept of Ticket Response Message from Broker
      - Send Redeem Message to AM
      - On receipt of Redeem Response from AM, update the reservation status
    - Update the Slice Status
### Broker
Complete Flow for all messages and processing at Broker is described below:
- Broker::ticket validates incoming message and adds it to bids_pending queue
- Broker::tick_handler processes the messages from bids_pending queue
  - Invokes KernelWrapper::ticket which does following
    - Registers Reservation
    - Reserve the reservation by invoking BrokerSimplerUnitsPolicy::allocate which is responsible for annotating 
    the reservation by querying CBM and Relational Database and updating the reservation
    - Send Updated Reservation back to Orchestrator  
#### Policy (Only doing compute; work for PCI devices in progress)
- Lookup graph node in CBM using the graph_node_id received in the reservation
- Fetch Capacity Delegations, Label Delegations from the graph Node
- Fetch all the Ticketed Reservations from Database with the graph_node_id
- Compute the available RAM, Disk and Cores by using the values in Capacity Delegations and excluding any values assigned to active Reservations
- Verify that the requested RAM, Disk and Cores can be satisfied by the computed available RAM, Disk and Cores
- Grant a ticket by Sending Update Ticket Message to Orchestrator
- Update ReservationInfo in the CBM for the Graph Node to include Reservation Id and Slice Id
### Aggregate Manager
Complete Flow for all messages and processing at AM is described below:
- Incoming Add Slice Message results in invocation of core.Actor::register_slice method which creates a slice and 
invokes the handler callback plugins.Config::create_slice to perform any site level initialization. 
NOTE: plugins.Config class defines the various callback hooks that need to be implemented by Handler. 
- Incoming Redeem Message is processed by core.Authority::redeem method which in turn results in following sequence 
- kernel.KernelWrapper::redeem_request
  - validates the incoming message
  - registers the reservation
  - reserves the reservation by invoking kernel.Kernel::reserve
- kernel.Kernel::reserve
  - Invokes the AuthorityCalendarPolicy::bind which validates the incoming reservation, leases, resources 
    requested; if successful adds it to the redeeming queue and to the calendar
- core.Authority::tick_handler periodically checks for the reservations which need to be assigned resources and performs 
following action for each redeeming reservation:
- Looks the ResourceControl based on the resource type and invokes it's assign method
  e.g. ResourceType=VM, policy.SimpleVmControl::assign is invoked
- Assign method for each ResourceControl Looks up ARM and verifies the availability of the assigned resource
  for the specific resource type; Updates the Reservation in database
- Invokes the Handler via Plugin to provision the resource on the Substrate
- Handler on completion; updates the status of the reservation which in turn is passed to the Broker and Orchestrator
#### Policy (Only doing compute; work for PCI devices in progress)
- Lookup graph node in ARM using the graph_node_id received in the reservation
- Fetch Capacities, Capacity Delegations, Labels, Label Delegations and Reservation Info from the graph Node
- Verify that the Requested RAM, Disk and Cores can be satisfied by the Delegated RAM, Disk and Cores
- Fetch all the Active Reservations from Database with the graph_node_id
- Compute the available RAM, Disk and Cores by using the values in Capacities and excluding any values assigned to active reservations
- Verify that the requested RAM, Disk and Cores can be satisfied by the computed available RAM, Disk and Cores
- Redeem the reservation