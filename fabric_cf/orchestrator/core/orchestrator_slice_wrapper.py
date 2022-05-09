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
import threading
from typing import List, Tuple, Dict
from http.client import BAD_REQUEST

from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fim.graph.slices.abc_asm import ABCASMPropertyGraph
from fim.slivers.capacities_labels import CapacityHints
from fim.slivers.instance_catalog import InstanceCatalog
from fim.slivers.network_node import NodeSliver, NodeType
from fim.user import ServiceType

from fabric_cf.actor.fim.fim_helper import FimHelper
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.reservation_converter import ReservationConverter
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.util.id import ID


class OrchestratorSliceWrapper:
    """
    Orchestrator Wrapper Around Slice to hold the computed reservations for processing by Slice Deferred Thread
    """
    def __init__(self, *, controller: ABCMgmtControllerMixin, broker: ID, slice_obj: SliceAvro, logger):
        self.controller = controller
        self.broker = broker
        self.slice_obj = slice_obj
        self.logger = logger
        self.reservation_converter = ReservationConverter(controller=controller, broker=broker)
        self.rid_to_res = {}
        self.computed_reservations = []
        self.computed_l3_reservations = []
        self.thread_lock = threading.Lock()
        self.ignorable_ns = [ServiceType.P4, ServiceType.OVS, ServiceType.MPLS, ServiceType.VLAN]
        self.supported_ns = [ServiceType.L2STS, ServiceType.L2Bridge, ServiceType.L2PTP, ServiceType.FABNetv6,
                             ServiceType.FABNetv4, ServiceType.PortMirror]
        self.l3_ns = [str(ServiceType.FABNetv6), str(ServiceType.FABNetv4)]

    def lock(self):
        """
        Lock slice
        :return:
        """
        self.thread_lock.acquire()

    def unlock(self):
        """
        Unlock slice
        :return:
        """
        if self.thread_lock.locked():
            self.thread_lock.release()

    def get_computed_reservations(self) -> List[TicketReservationAvro]:
        """
        Get computed reservations
        :return: computed reservations
        """
        return self.computed_reservations

    def get_all_reservations(self) -> List[ReservationMng]:
        """
        Get All reservations
        :return: all reservations
        """
        if self.controller is None:
            return None
        return self.controller.get_reservations(slice_id=ID(uid=self.slice_obj.get_slice_id()))

    def get_slice_name(self) -> str:
        """
        Get Slice name
        :return: slice name
        """
        return self.slice_obj.get_slice_name()

    def get_slice_id(self) -> ID:
        """
        Get Slice Id
        :return: slice id
        """
        return ID(uid=self.slice_obj.get_slice_id())

    def create(self, *, slice_graph: ABCASMPropertyGraph) -> List[LeaseReservationAvro]:
        """
        Create a slice
        :param slice_graph: Slice Graph
        :return: List of computed reservations
        """
        try:
            # Build Network Node reservations
            network_node_reservations, node_res_mapping = self.__build_network_node_reservations(slice_graph=slice_graph)

            # Build Network Service reservations
            network_service_reservations = self.__build_network_service_reservations(slice_graph=slice_graph,
                                                                                     node_res_mapping=node_res_mapping)

            # Add Network Node reservations
            for r in network_node_reservations:
                self.controller.add_reservation(reservation=r)
                self.computed_reservations.append(r)

            # Add Network Node reservations
            for r in network_service_reservations:
                self.controller.add_reservation(reservation=r)
                self.computed_reservations.append(r)

            return self.computed_reservations
        except OrchestratorException as e:
            self.logger.error("Exception occurred while generating reservations for slivers: {}".format(e))
            raise e
        except Exception as e:
            self.logger.error("Exception occurred while generating reservations for slivers: {}".format(e))
            raise OrchestratorException(message=f"Failure to build Slivers: {e}")

    @staticmethod
    def __validate_node_sliver(sliver: NodeSliver):
        """
        Validate Network Node Sliver
        @param sliver Node Sliver
        @raises exception for invalid slivers
        """
        if sliver.get_capacities() is None and sliver.get_capacity_hints() is None:
            raise OrchestratorException(message="Either Capacity or Capacity Hints must be specified!",
                                        http_error_code=BAD_REQUEST)

    def __build_network_service_reservations(self, slice_graph: ABCASMPropertyGraph,
                                             node_res_mapping: Dict[str, str]) -> List[LeaseReservationAvro]:
        """
        Build Network Service Reservations
        @param slice_graph Slice graph
        @param node_res_mapping Mapping of Network Node sliver Id to reservation Id;
                                Needed to add dependency information in network service slivers
        @return list of network service reservations
        """
        reservations = []
        for ns_id in slice_graph.get_all_network_service_nodes():

            # Build Network Service Sliver
            sliver = slice_graph.build_deep_ns_sliver(node_id=ns_id)
            sliver_type = sliver.get_type()

            self.logger.trace(f"Network Service Sliver: {sliver}")

            # Ignore Sliver types P4,OVS and MPLS
            if sliver_type in self.ignorable_ns:
                continue

            # Process only the currently supported Network Sliver types L2STS, L2PTP and L2Bridge
            elif sliver_type in self.supported_ns:

                self.logger.trace(f"Network Service Sliver Interfaces: {sliver.interface_info}")
                # Processing Interface Slivers
                if sliver.interface_info is not None:
                    predecessor_reservations = []
                    for ifs in sliver.interface_info.interfaces.values():
                        # Get Mapping information for Interface Sliver from ASM
                        # i.e. [Peer IFS, Peer NS Id, Component Name, Node Id]
                        ifs_mapping = FimHelper.get_interface_sliver_mapping(ifs_node_id=ifs.node_id,
                                                                             slice_graph=slice_graph)

                        if ifs_mapping is None:
                            raise OrchestratorException(message=f"Peer connection point not found for ifs# {ifs}",
                                                        http_error_code=BAD_REQUEST)

                        # capacities (bw in Gbps, burst size is in Mbits) source: (b)
                        # Set Capacities
                        ifs.set_capacities(cap=ifs_mapping.get_peer_ifs().get_capacities())

                        # Set Labels
                        ifs.set_labels(lab=ifs_mapping.get_peer_ifs().get_labels())

                        if not ifs_mapping.is_facility():
                            # Save the parent component name and the parent reservation id in the Node Map
                            parent_res_id = node_res_mapping.get(ifs_mapping.get_node_id(), None)

                            node_map = tuple([ifs_mapping.get_component_name(), parent_res_id])
                            ifs.set_node_map(node_map=node_map)

                            self.logger.trace(f"Interface Sliver: {ifs}")

                            if parent_res_id is not None and parent_res_id not in predecessor_reservations:
                                predecessor_reservations.append(parent_res_id)
                        else:
                            # For Facility Ports, set Node Map [Facility, Facility Name] to help broker lookup
                            node_map = tuple([str(NodeType.Facility), ifs_mapping.get_node_id()])
                            ifs.set_node_map(node_map=node_map)

                    # Generate reservation for the sliver
                    reservation = self.reservation_converter.generate_reservation(sliver=sliver,
                                                                                  slice_id=self.slice_obj.get_slice_id(),
                                                                                  end_time=self.slice_obj.get_lease_end(),
                                                                                  pred_list=predecessor_reservations)
                    if reservation.get_resource_type() in self.l3_ns:
                        self.computed_l3_reservations.append(reservation)
                    reservations.append(reservation)
            else:
                raise OrchestratorException(message="Not implemented",
                                            http_error_code=BAD_REQUEST)
        return reservations

    def __build_network_node_reservations(self, slice_graph: ABCASMPropertyGraph) \
            -> Tuple[List[LeaseReservationAvro], Dict[str, str]]:
        reservations = []
        sliver_to_res_mapping = {}
        for nn_id in slice_graph.get_all_network_nodes():

            # Build Network Node Sliver
            sliver = slice_graph.build_deep_node_sliver(node_id=nn_id)

            if sliver.get_type() not in [NodeType.VM]:
                continue

            # Validate Node Sliver
            self.__validate_node_sliver(sliver=sliver)

            # Compute Requested Capacities from Capacity Hints
            requested_capacities = sliver.get_capacities()
            requested_capacity_hints = sliver.get_capacity_hints()
            catalog = InstanceCatalog()
            if requested_capacities is None and requested_capacity_hints is not None:
                requested_capacities = catalog.get_instance_capacities(
                    instance_type=requested_capacity_hints.instance_type)
                sliver.set_capacities(cap=requested_capacities)

            # Compute Capacity Hints from Requested Capacities
            if requested_capacity_hints is None and requested_capacities is not None:
                instance_type = catalog.map_capacities_to_instance(cap=requested_capacities)
                requested_capacity_hints = CapacityHints(instance_type=instance_type)
                sliver.set_capacity_hints(caphint=requested_capacity_hints)

            # Generate reservation for the sliver
            reservation = self.reservation_converter.generate_reservation(sliver=sliver,
                                                                          slice_id=self.slice_obj.get_slice_id(),
                                                                          end_time=self.slice_obj.get_lease_end())
            reservations.append(reservation)

            self.logger.trace(f"Mapped sliver: {sliver.node_id} to res: {reservation.get_reservation_id()}")

            # Maintain Sliver Id to Reservation Mapping
            sliver_to_res_mapping[sliver.node_id] = reservation.get_reservation_id()
        return reservations, sliver_to_res_mapping
