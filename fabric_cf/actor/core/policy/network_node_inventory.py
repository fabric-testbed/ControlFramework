#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)
from typing import Tuple, List, Dict

from fim.slivers.attached_components import AttachedComponentsInfo, ComponentSliver, ComponentType
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Capacities, Labels
from fim.slivers.delegations import Delegations
from fim.slivers.instance_catalog import InstanceCatalog
from fim.slivers.interface_info import InterfaceSliver
from fim.slivers.network_node import NodeSliver, NodeType
from fim.slivers.network_service import NSLayer

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.kernel.reservation_states import ReservationOperation
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
from fabric_cf.actor.core.util.id import ID


class NetworkNodeInventory(InventoryForType):
    def __check_capacities(self, *, rid: ID, requested_capacities: Capacities, delegated_capacities: Delegations,
                           existing_reservations: List[ABCReservationMixin]) -> str or None:
        """
        Check if the requested capacities can be satisfied with the available capacities
        :param rid: reservation id of the reservation being served
        :param requested_capacities: Requested Capacities
        :param delegated_capacities: Delegated Capacities
        :param existing_reservations: Existing Reservations served by the same BQM node
        :return: Delegation Id of the delegation which satisfies the request
        :raises: BrokerException in case the request cannot be satisfied
        """
        self.logger.debug(f"requested_capacities: {requested_capacities} for reservation# {rid}")

        delegation_id, delegated_capacity = self.get_delegations(lab_cap_delegations=delegated_capacities)

        # Remove allocated capacities to the reservations
        if existing_reservations is not None:
            for reservation in existing_reservations:
                if rid == reservation.get_reservation_id():
                    continue
                # For Active or Ticketed or Ticketing reservations; reduce the counts from available
                resource_sliver = None
                if reservation.is_ticketing() and reservation.get_approved_resources() is not None:
                    resource_sliver = reservation.get_approved_resources().get_sliver()

                if (reservation.is_active() or reservation.is_ticketed()) and \
                        reservation.get_resources() is not None:
                    resource_sliver = reservation.get_resources().get_sliver()

                if resource_sliver is not None and isinstance(resource_sliver, NodeSliver):
                    self.logger.debug(
                        f"Excluding already assigned resources {resource_sliver.get_capacity_allocations()} to "
                        f"reservation# {reservation.get_reservation_id()}")
                    delegated_capacity = delegated_capacity - resource_sliver.get_capacity_allocations()

        # Compare the requested against available
        delegated_capacity = delegated_capacity - requested_capacities
        negative_fields = delegated_capacity.negative_fields()
        if len(negative_fields) > 0:
            raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                  msg=f"{negative_fields}")

        return delegation_id

    def __set_ips(self, *, req_ifs: InterfaceSliver, lab: Labels):
        if req_ifs.labels is not None and req_ifs.labels.ipv4 is not None:
            lab.ipv4 = req_ifs.labels.ipv4
            if req_ifs.labels.ipv4_subnet is not None:
                lab.ipv4_subnet = req_ifs.labels.ipv4_subnet
        if req_ifs.labels is not None and req_ifs.labels.ipv6 is not None:
            lab.ipv6 = req_ifs.labels.ipv6
            if req_ifs.labels.ipv6_subnet is not None:
                lab.ipv6_subnet = req_ifs.labels.ipv6_subnet
        return lab

    def __update_shared_nic_labels_and_capacities(self, *, available_component: ComponentSliver,
                                                  requested_component: ComponentSliver) -> ComponentSliver:
        """
        Update the shared NIC Labels and Capacities. Assign the 1st available PCI address/bdf to the requested component
        Traverse the available component's labels to find the index for bdf assigned
        Using the found labels, assign BDF, MAC and VLAN address to the IFS on the Requested component
        In case of L2 service, also copy the requested IP address so it can be used by the AMHandler to configure the
        interface post VM creation
        :param available_component: Available Component
        :param requested_component: Requested Component
        :return updated requested component with VLAN, MAC and IP information
        """
        # Check labels
        delegation_id, delegated_label = self.get_delegations(
            lab_cap_delegations=available_component.get_label_delegations())

        if delegated_label.bdf is None or len(delegated_label.bdf) < 1:
            message = "No PCI devices available in the delegation"
            self.logger.error(message)
            raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                  msg=f"{message}")

        # Find the VLAN from the BQM Component
        if available_component.network_service_info is None or \
                len(available_component.network_service_info.network_services) != 1:
            message = "Shared NIC Card must have one Network Service"
            self.logger.error(message)
            raise BrokerException(error_code=ExceptionErrorCode.FAILURE,
                                  msg=f"{message}")

        ns_name = next(iter(available_component.network_service_info.network_services))
        ns = available_component.network_service_info.network_services[ns_name]

        if ns.interface_info is None or len(ns.interface_info.interfaces) != 1:
            message = "Shared NIC Card must have one Connection Point"
            self.logger.error(message)
            raise BrokerException(error_code=ExceptionErrorCode.FAILURE,
                                  msg=f"{message}")

        ifs_name = next(iter(ns.interface_info.interfaces))
        ifs = ns.interface_info.interfaces[ifs_name]

        delegation_id, ifs_delegated_labels = self.get_delegations(lab_cap_delegations=ifs.get_label_delegations())

        assigned_bdf = delegated_label.bdf[0]
        assigned_numa = delegated_label.numa[0]

        # Check if the requested component's VLAN exists in the delegated labels
        if requested_component.labels and requested_component.labels.vlan and \
                requested_component.labels.vlan in ifs_delegated_labels.vlan:
            vlan_index = ifs_delegated_labels.vlan.index(requested_component.labels.vlan)
            bdf_for_requested_vlan = ifs_delegated_labels.bdf[vlan_index]
            
            if bdf_for_requested_vlan in delegated_label.bdf:
                bdf_index = delegated_label.bdf.index(bdf_for_requested_vlan)
                assigned_bdf = bdf_for_requested_vlan
                assigned_numa = delegated_label.numa[bdf_index]

        # Assign the first PCI Id from the list of available PCI slots
        requested_component.label_allocations = Labels(bdf=assigned_bdf, numa=assigned_numa)

        # Find index of assigned BDF in the interface delegated labels
        assigned_index = ifs_delegated_labels.bdf.index(assigned_bdf)

        # Updated the Requested component with VLAN, BDF, MAC
        req_ns_name = next(iter(requested_component.network_service_info.network_services))
        req_ns = requested_component.network_service_info.network_services[req_ns_name]
        req_ifs_name = next(iter(req_ns.interface_info.interfaces))
        req_ifs = req_ns.interface_info.interfaces[req_ifs_name]

        # Do not copy VLAN for OpenStack-vNIC
        if requested_component.get_model() == Constants.OPENSTACK_VNIC_MODEL:
            lab = Labels(bdf=ifs_delegated_labels.bdf[assigned_index], mac=ifs_delegated_labels.mac[assigned_index],
                         local_name=ifs_delegated_labels.local_name[assigned_index])
        else:
            lab = Labels(bdf=ifs_delegated_labels.bdf[assigned_index], mac=ifs_delegated_labels.mac[assigned_index],
                         vlan=ifs_delegated_labels.vlan[assigned_index],
                         local_name=ifs_delegated_labels.local_name[assigned_index])

        # For the Layer 2 copying the IP address to the label allocations
        # This is to be used by AM Handler to configure Network Interface
        if req_ns.layer == NSLayer.L2:
            lab = self.__set_ips(req_ifs=req_ifs, lab=lab)

        req_ifs.set_label_allocations(lab=lab)

        self.logger.info(f"Assigned Interface Sliver: {req_ifs}")
        return requested_component

    def __update_smart_nic_labels_and_capacities(self, *, available_component: ComponentSliver,
                                                 requested_component: ComponentSliver) -> ComponentSliver:
        """
        Update the IFS for the Smart NIC with VLAN, MAC and IP Address information
        This is to enable AM handler to configure network interfaces at VM creation.
        This is only done for Layer 2 services
        :param available_component: Available Component
        :param requested_component: Requested Component
        :return updated requested component with VLAN, MAC and IP information
        """

        # Find the VLAN from the BQM Component
        if available_component.network_service_info is None or \
                len(available_component.network_service_info.network_services) != 1:
            message = "Smart NIC Card must have at one Network Service"
            self.logger.error(message)
            raise BrokerException(error_code=ExceptionErrorCode.FAILURE,
                                  msg=f"{message}")

        ns_name = next(iter(available_component.network_service_info.network_services))
        ns = available_component.network_service_info.network_services[ns_name]

        if ns.interface_info is None or len(ns.interface_info.interfaces) < 0:
            message = "Smart NIC Card must have at least one Connection Point"
            self.logger.error(message)
            raise BrokerException(error_code=ExceptionErrorCode.FAILURE,
                                  msg=f"{message}")

        for ifs in ns.interface_info.interfaces.values():
            delegation_id, ifs_delegated_labels = self.get_delegations(lab_cap_delegations=ifs.get_label_delegations())

            for requested_ns in requested_component.network_service_info.network_services.values():
                if requested_ns.interface_info is not None and requested_ns.interface_info.interfaces is not None:
                    for requested_ifs in requested_ns.interface_info.interfaces.values():
                        if requested_ifs.labels.local_name == ifs_delegated_labels.local_name:
                            lab = Labels()
                            lab.mac = ifs_delegated_labels.mac
                            lab.local_name = ifs_delegated_labels.local_name

                            # Update the VLAN and IP address to be used for configuration at AM only for L2 services
                            # Information for L3 services is updated later after NetworkService has been ticketed
                            if requested_ns.layer == NSLayer.L2:
                                if requested_ifs.labels is not None and requested_ifs.labels.vlan is not None:
                                    lab.vlan = requested_ifs.labels.vlan

                                lab = self.__set_ips(req_ifs=requested_ifs, lab=lab)

                            requested_ifs.set_label_allocations(lab=lab)
                        self.logger.info(f"Assigned Interface Sliver: {requested_ifs}")
        return requested_component

    def __check_component_labels_and_capacities(self, *, available_component: ComponentSliver, graph_id: str,
                                                requested_component: ComponentSliver,
                                                operation: ReservationOperation = ReservationOperation.Create) -> ComponentSliver:
        """
        Check if available component capacities, labels to match requested component
        :param available_component: available component
        :param graph_id: BQM graph id
        :param requested_component: requested component
        :param operation: operation
        :return: requested component annotated with properties in case of success, None otherwise
        """
        if requested_component.get_model() is not None and \
                requested_component.get_model() != available_component.get_model():
            return requested_component

        # Checking capacity for component
        delegation_id, delegated_capacity = self.get_delegations(
            lab_cap_delegations=available_component.get_capacity_delegations())

        # Delegated capacity would have been decremented already to exclude allocated shared NICs
        if delegated_capacity.unit < 1:
            message = f"Insufficient Capacities for component: {requested_component}"
            self.logger.error(message)
            raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                  msg=f"{message}")

        requested_component.capacity_allocations = Capacities(unit=1)

        # Check labels
        delegation_id, delegated_label = self.get_delegations(
            lab_cap_delegations=available_component.get_label_delegations())

        if requested_component.get_type() == ComponentType.SharedNIC:
            requested_component = self.__update_shared_nic_labels_and_capacities(available_component=available_component,
                                                                                 requested_component=requested_component)
        else:
            requested_component.label_allocations = delegated_label
            if requested_component.get_type() == ComponentType.SmartNIC:
                requested_component = self.__update_smart_nic_labels_and_capacities(available_component=available_component,
                                                                                    requested_component=requested_component)

        node_map = tuple([graph_id, available_component.node_id])
        requested_component.set_node_map(node_map=node_map)
        if requested_component.labels is None or operation == ReservationOperation.Create:
            requested_component.labels = Labels.update(lab=requested_component.get_label_allocations())

        return requested_component

    def __exclude_allocated_pci_device_from_shared_nic(self, shared_nic: ComponentSliver,
                                                       allocated_nic: ComponentSliver) -> Tuple[ComponentSliver, bool]:
        """
        For Shared NIC cards, exclude the already assigned PCI addresses from the available PCI addresses in
        BQM Component Sliver for the NIC Card
        @param shared_nic: Available Shared NIC
        @param allocated_nic: Allocated NIC
        @return Available NIC updated to exclude the Allocated PCI addresses and True/False indicating if Available
        Shared NIC has any available PCI addresses
        """

        if shared_nic.get_type() != ComponentType.SharedNIC and allocated_nic.get_type() != ComponentType.SharedNIC:
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT,
                                  msg=f"shared_nic: {shared_nic} allocated_nic: {allocated_nic}")

        # Reduce capacity for component
        delegation_id, delegated_capacity = self.get_delegations(
            lab_cap_delegations=shared_nic.get_capacity_delegations())

        self.logger.debug(f"Allocated NIC: {allocated_nic} labels: {allocated_nic.get_labels()}")

        # Get the Allocated PCI address
        allocated_labels = allocated_nic.get_labels()

        delegation_id, delegated_label = self.get_delegations(lab_cap_delegations=shared_nic.get_label_delegations())

        # Remove allocated PCI address from delegations
        excluded_labels = []

        if isinstance(allocated_labels.bdf, list):
            excluded_labels = allocated_labels.bdf
        else:
            excluded_labels = [allocated_labels.bdf]

        exists = False
        for e in excluded_labels:
            if e in delegated_label.bdf:
                self.logger.debug(f"Excluding PCI device {e}")
                delegated_label.bdf.remove(e)
                exists = True

        # Exclude already allocated Shared NIC cards
        if exists:
            delegated_capacity -= allocated_nic.get_capacity_allocations()

        return shared_nic, (delegated_capacity.unit < 1)

    def __exclude_allocated_component(self, *, graph_node: NodeSliver, available_component: ComponentSliver,
                                      allocated_component: ComponentSliver):
        """
        Remove the allocated component from the candidate Node. For dedicated components, the whole component is removed,
        for Shared NIC, only the allocated PCI address is removed and the number of units is reduced by 1.
        If all the PCIs are allocated for a Shared NIC, the complete Shared NIC is removed

        @param graph_node candidate node identified to satisfy the reservation
        @param available_component available component
        @param allocated_component allocated component
        """
        exclude = True
        if allocated_component.get_type() == ComponentType.SharedNIC:
            available_component, exclude = self.__exclude_allocated_pci_device_from_shared_nic(
                shared_nic=available_component, allocated_nic=allocated_component)
        if exclude:
            graph_node.attached_components_info.remove_device(name=available_component.get_name())

    def __exclude_components_for_existing_reservations(self, *, rid: ID, graph_node: NodeSliver,
                                                       existing_reservations: List[ABCReservationMixin],
                                                       operation: ReservationOperation = ReservationOperation.Create) -> NodeSliver:
        """
        Remove already assigned components to existing reservations from the candidate node
        @param rid reservation ID
        @param graph_node candidate node identified to satisfy the reservation
        @param existing_reservations Existing Ticketed Reservations
        @return Return the updated candidate node
        """
        for reservation in existing_reservations:
            # Requested reservation should be skipped only when new i.e. not ticketed
            if rid == reservation.get_reservation_id() and \
                    (operation == ReservationOperation.Extend or not reservation.is_ticketed()):
                continue
            # For Active or Ticketed or Ticketing reservations; reduce the counts from available
            allocated_sliver = None
            if reservation.is_ticketing() and reservation.get_approved_resources() is not None:
                allocated_sliver = reservation.get_approved_resources().get_sliver()

            if (reservation.is_active() or reservation.is_ticketed()) and reservation.get_resources() is not None:
                allocated_sliver = reservation.get_resources().get_sliver()

            if reservation.is_extending_ticket() and reservation.get_requested_resources() is not None and \
                    reservation.get_requested_resources().get_sliver() is not None:
                allocated_sliver = reservation.get_requested_resources().get_sliver()

            if allocated_sliver is None or not isinstance(allocated_sliver, NodeSliver) or \
                    allocated_sliver.attached_components_info is None:
                continue

            for allocated in allocated_sliver.attached_components_info.devices.values():
                allocated_node_map = allocated.get_node_map()

                if allocated_node_map is None:
                    continue

                resource_type = allocated.get_type()

                self.logger.debug(f"Already allocated components {allocated} of resource_type "
                                  f"{resource_type} to reservation# {reservation.get_reservation_id()}")

                for av in graph_node.attached_components_info.devices.values():
                    if av.node_id == allocated_node_map[1]:
                        self.__exclude_allocated_component(graph_node=graph_node, available_component=av,
                                                           allocated_component=allocated)
                        break
        return graph_node

    def __check_components(self, *, rid: ID, requested_components: AttachedComponentsInfo, graph_id: str,
                           graph_node: NodeSliver, existing_reservations: List[ABCReservationMixin],
                           existing_components: Dict[str, List[str]],
                           operation: ReservationOperation = ReservationOperation.Create) -> AttachedComponentsInfo:
        """
        Check if the requested capacities can be satisfied with the available capacities
        :param rid: reservation id of the reservation being served
        :param requested_components: Requested components
        :param graph_id: BQM graph id
        :param graph_node: BQM graph node identified to serve the reservation
        :param existing_reservations: Existing Reservations served by the same BQM node
        :param operation: Flag indicating if this is create or modify
        :return: Components updated with the corresponding BQM node ids
        :raises: BrokerException in case the request cannot be satisfied
        """
        self.logger.debug(f"Available on {graph_node.node_id} components: {graph_node.attached_components_info.devices.keys()}")

        self.__exclude_components_for_existing_reservations(rid=rid, graph_node=graph_node,
                                                            existing_reservations=existing_reservations,
                                                            operation=operation)

        self.logger.debug(f"Excluding components connected to Network Services: {existing_components}")

        if existing_components and len(existing_components):
            comps_to_remove = []
            for av in graph_node.attached_components_info.devices.values():
                # Skip if not in allocated comps attached to Network Services
                if av.node_id not in existing_components.keys():
                    continue
                exclude = True
                if av.get_type() == ComponentType.SharedNIC:
                    bdfs = existing_components.get(av.node_id)
                    allocated_component = ComponentSliver()
                    allocated_component.set_type(ComponentType.SharedNIC)
                    allocated_component.set_name(resource_name=av.get_name())
                    allocated_component.set_capacity_allocations(cap=Capacities(unit=len(bdfs)))
                    allocated_component.set_labels(Labels(bdf=bdfs))
                    self.logger.debug(f"Excluding Shared NICs connected to Network Services: {allocated_component}")
                    av, exclude = self.__exclude_allocated_pci_device_from_shared_nic(shared_nic=av,
                                                                                      allocated_nic=allocated_component)
                if exclude:
                    comps_to_remove.append(av)

            for c in comps_to_remove:
                self.logger.debug(f"Excluding component: {c.get_name()}")
                print(f"Excluding component: {c.get_name()}")
                graph_node.attached_components_info.remove_device(name=c.get_name())

        self.logger.debug(f"requested_components: {requested_components.devices.values()} for reservation# {rid}")
        for name, requested_component in requested_components.devices.items():
            if operation == ReservationOperation.Modify and requested_component.get_node_map() is not None:
                self.logger.debug(f"Modify: Ignoring Allocated component: {requested_component}")
                continue

            if operation == ReservationOperation.Extend and requested_component.get_node_map() is not None:
                bqm_id, node_id = requested_component.get_node_map()

                if requested_component.get_type() == ComponentType.SharedNIC:
                    allocated_bdfs = existing_components.get(node_id)
                    if allocated_bdfs and requested_component.labels and requested_component.labels.bdf:
                        bdfs = requested_component.labels.bdf
                        if isinstance(requested_component.labels.bdf, str):
                            bdfs = [requested_component.labels.bdf]

                        self.logger.debug(f"Allocated BDFs: {allocated_bdfs}")
                        for x in bdfs:
                            if x in allocated_bdfs:
                                raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                                      msg=f"Renew failed: Component of type: {requested_component.get_model()} with PCI Address: {x}"
                                                          f"already in use by another reservation for node: {graph_node.node_id}")
                else:
                    if node_id in existing_components.keys():
                        raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                              msg=f"Renew failed: Component of type: {requested_component.get_model()} "
                                                  f"already in use by another reservation for node: {graph_node.node_id}")

                self.logger.debug(f"Renew: Component {requested_component} still available")
                continue

            self.logger.debug(f"Create: Allocating component: {requested_component}")
            resource_type = requested_component.get_type()
            resource_model = requested_component.get_model()
            if resource_type == ComponentType.Storage:
                requested_component.capacity_allocations = Capacities(unit=1)
                requested_component.label_allocations = Labels()
                requested_component.label_allocations = Labels.update(lab=requested_component.get_labels())
                continue
            available_components = graph_node.attached_components_info.get_devices_by_type(resource_type=resource_type)
            self.logger.debug(f"Available components of type: {resource_type} after excluding "
                              f"allocated components: {available_components}")

            if available_components is None or len(available_components) == 0:
                raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                      msg=f"Component of type: {resource_model} not available in "
                                          f"graph node: {graph_node.node_id}")

            for component in available_components:
                # check model matches the requested model
                requested_component = self.__check_component_labels_and_capacities(
                    available_component=component, graph_id=graph_id,
                    requested_component=requested_component,
                    operation=operation)

                if requested_component.get_node_map() is not None:
                    self.logger.info(f"Assigning {component.node_id} to component# "
                                     f"{requested_component} in reservation# {rid} ")

                    # Remove the component from available components as it is assigned
                    self.__exclude_allocated_component(graph_node=graph_node, available_component=component,
                                                       allocated_component=requested_component)
                    break

            if requested_component.get_node_map() is None:
                raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                      msg=f"Component of type: {resource_model} not available in "
                                          f"graph node: {graph_node.node_id}")

        return requested_components

    def __allocate_p4_switch(self, *, rid: ID, requested_sliver: NodeSliver, graph_id: str, graph_node: NodeSliver,
                             existing_reservations: List[ABCReservationMixin], existing_components: Dict[str, List[str]],
                             operation: ReservationOperation = ReservationOperation.Create) -> Tuple[str, BaseSliver]:
        """
        Allocate an extending or ticketing reservation for a P4 switch

        :param rid: reservation id of the reservation to be allocated
        :param requested_sliver: requested sliver
        :param graph_id: BQM graph id
        :param graph_node: BQM graph node identified to serve the reservation
        :param existing_components: Existing Components
        :param existing_reservations: Existing Reservations served by the same BQM node
        :param operation: Indicates if this is create or modify

        :return: Tuple of Delegation Id and the Requested Sliver annotated with BQM Node Id and other properties
        :raises: BrokerException in case the request cannot be satisfied
        """
        delegation_id = None

        if operation == ReservationOperation.Create:
            # In case of modify, directly get delegation_id
            if len(graph_node.get_capacity_delegations().get_delegation_ids()) > 0:
                delegation_id = next(iter(graph_node.get_capacity_delegations().get_delegation_ids()))

        # Handle allocation to account for leaked Network Services
        for n in existing_components.keys():
            if n in graph_node.node_id:
                raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                      msg=f"Node of type: {graph_node.get_type()} not available on site: "
                                          f"{graph_node.get_site()}, already in use by another reservation")

        # For create, we need to allocate the P4
        requested_capacities = requested_sliver.get_capacities()

        # Check if Capacities can be satisfied
        delegation_id = self.__check_capacities(rid=rid,
                                                requested_capacities=requested_capacities,
                                                delegated_capacities=graph_node.get_capacity_delegations(),
                                                existing_reservations=existing_reservations)
        requested_sliver.capacity_allocations = Capacities()
        requested_sliver.capacity_allocations = Capacities.update(lab=requested_capacities)
        requested_sliver.label_allocations = Labels(local_name=graph_node.get_name())

        requested_sliver.set_node_map(node_map=(graph_id, graph_node.node_id))
        requested_sliver.management_ip = graph_node.management_ip

        self.logger.info(f"Reservation# {rid} is being served by delegation# {delegation_id} "
                         f"node# [{graph_id}/{graph_node.node_id}]")

        return delegation_id, requested_sliver

    def allocate(self, *, rid: ID, requested_sliver: BaseSliver, graph_id: str, graph_node: BaseSliver,
                 existing_reservations: List[ABCReservationMixin], existing_components: Dict[str, List[str]],
                 operation: ReservationOperation = ReservationOperation.Create) -> Tuple[str, BaseSliver]:
        """
        Allocate an extending or ticketing reservation
        :param rid: reservation id of the reservation to be allocated
        :param requested_sliver: requested sliver
        :param graph_id: BQM graph id
        :param graph_node: BQM graph node identified to serve the reservation
        :param existing_components: Existing Components
        :param existing_reservations: Existing Reservations served by the same BQM node
        :param operation: Indicates if this is create or modify
        :return: Tuple of Delegation Id and the Requested Sliver annotated with BQM Node Id and other properties
        :raises: BrokerException in case the request cannot be satisfied
        """
        if graph_node.get_capacity_delegations() is None or rid is None:
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT,
                                  msg=f"capacity_delegations is missing or reservation is None")

        if not isinstance(requested_sliver, NodeSliver):
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT,
                                  msg=f"resource type: {requested_sliver.get_type()}")

        if not isinstance(graph_node, NodeSliver):
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT,
                                  msg=f"resource type: {graph_node.get_type()}")

        if requested_sliver.get_type() not in [NodeType.VM, NodeType.Switch]:
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT,
                                  msg=f"Unsupported resource type: {graph_node.get_type()}")

        if requested_sliver.get_type() == NodeType.Switch:
            return self.__allocate_p4_switch(rid=rid, requested_sliver=requested_sliver, graph_id=graph_id,
                                             graph_node=graph_node, existing_reservations=existing_reservations,
                                             existing_components=existing_components, operation=operation)

        delegation_id = None
        requested_capacities = None
        # For create, we need to allocate the VM
        if operation == ReservationOperation.Create:
            # Always use requested capacities to be mapped from flavor i.e. capacity hints
            requested_capacity_hints = requested_sliver.get_capacity_hints()
            catalog = InstanceCatalog()
            requested_capacities = catalog.get_instance_capacities(instance_type=requested_capacity_hints.instance_type)
        else:
            requested_capacities = requested_sliver.get_capacity_allocations()
            # In case of modify, directly get delegation_id
            if len(graph_node.get_capacity_delegations().get_delegation_ids()) > 0:
                delegation_id = next(iter(graph_node.get_capacity_delegations().get_delegation_ids()))

        # Check if Capacities can be satisfied
        delegation_id = self.__check_capacities(rid=rid,
                                                requested_capacities=requested_capacities,
                                                delegated_capacities=graph_node.get_capacity_delegations(),
                                                existing_reservations=existing_reservations)

        # Check if Components can be allocated
        if requested_sliver.attached_components_info is not None:
            requested_sliver.attached_components_info = self.__check_components(
                rid=rid,
                requested_components=requested_sliver.attached_components_info,
                graph_id=graph_id,
                graph_node=graph_node,
                existing_reservations=existing_reservations,
                existing_components=existing_components,
                operation=operation)

        # Do this only for create
        if operation == ReservationOperation.Create:
            requested_sliver.capacity_allocations = Capacities()
            requested_sliver.capacity_allocations = Capacities.update(lab=requested_capacities)
            requested_sliver.label_allocations = Labels(instance_parent=graph_node.get_name())

            requested_sliver.set_node_map(node_map=(graph_id, graph_node.node_id))

        self.logger.info(f"Reservation# {rid} is being served by delegation# {delegation_id} "
                         f"node# [{graph_id}/{graph_node.node_id}]")

        return delegation_id, requested_sliver

    def free(self, *, count: int, request: dict = None, resource: dict = None) -> dict:
        pass
