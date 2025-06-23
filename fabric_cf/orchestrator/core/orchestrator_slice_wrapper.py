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
import ipaddress
import threading
import time
from datetime import datetime
from ipaddress import IPv4Network
from typing import List, Tuple, Dict
from http.client import BAD_REQUEST, NOT_FOUND

from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.reservation_predecessor_avro import ReservationPredecessorAvro
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fim.graph.slices.abc_asm import ABCASMPropertyGraph
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Labels
from fim.slivers.network_node import NodeSliver, NodeType
from fim.slivers.network_service import NetworkServiceSliver
from fim.slivers.topology_diff import WhatsModifiedFlag, TopologyDiff
from fim.user import ServiceType, ExperimentTopology, InterfaceType

from fabric_cf.actor.core.common.constants import ErrorCodes, Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationPendingStates, ReservationStates
from fabric_cf.actor.fim.fim_helper import FimHelper
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.reservation_converter import ReservationConverter
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.util.id import ID


class ModifiedReservation:
    def __init__(self, *, sliver: BaseSliver, dependencies: List[ReservationPredecessorAvro] = None):
        self.sliver = sliver
        self.dependencies = dependencies


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
        # Reservations trigger Add and Demand
        self.computed_add_reservations = []
        self.computed_l3_reservations = []
        # Reservations trigger ExtendTicket (Broker) and ExtendLease (AM)
        self.computed_modify_reservations = {}
        self.computed_remove_reservations = []
        # Reservations trigger ModifyLease (AM)
        self.computed_modify_properties_reservations = []
        self.thread_lock = threading.Lock()
        self.start = None
        self.end = None
        self.lifetime = None

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

    def get_reservation(self, *, rid: str) -> ReservationMng or None:
        if self.controller is None or rid is None:
            return None
        reservation_id = ID(uid=rid) if rid is not None else None
        reservations = self.controller.get_reservations(rid=reservation_id)

        if reservations is None or len(reservations) == 0:
            if self.controller.get_last_error() is not None:
                self.logger.error(self.controller.get_last_error())
                if self.controller.get_last_error().status.code == ErrorCodes.ErrorNoSuchReservation:
                    raise OrchestratorException(f"Reservation# {rid} not found",
                                                http_error_code=NOT_FOUND)

        return reservations[0]

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

    def add_reservations(self):
        start = time.time()
        # Add Network Node reservations
        for r in self.computed_add_reservations:
            self.controller.add_reservation(reservation=r)
        self.logger.info(f"ADD TIME: {time.time() - start:.0f}")

    def create(self, *, slice_graph: ABCASMPropertyGraph, lease_start_time: datetime = None,
               lease_end_time: datetime = None, lifetime: int = None) -> List[LeaseReservationAvro]:
        """
        Create a slice
        :param slice_graph: Slice Graph
        :param lease_start_time: Lease Start Time (UTC)
        :param lease_end_time: Lease End Time (UTC)
        :param lifetime: Lifetime of the slice in hours
        :return: List of computed reservations
        """
        try:
            self.start = lease_start_time
            self.end = lease_end_time
            self.lifetime = lifetime
            # Build Network Node reservations
            start = time.time()
            network_node_reservations, node_res_mapping = self.__build_network_node_reservations(slice_graph=slice_graph)
            self.logger.info(f"Node TIME: {time.time() - start:.0f}")

            # Build Network Service reservations
            start = time.time()
            network_service_reservations = self.__build_network_service_reservations(slice_graph=slice_graph,
                                                                                     node_res_mapping=node_res_mapping)
            self.logger.info(f"NS TIME: {time.time() - start:.0f}")

            # Add Network Node reservations
            for r in network_node_reservations:
                self.computed_add_reservations.append(r)
                self.computed_reservations.append(r)

            # Add Network Node reservations
            for r in network_service_reservations:
                self.computed_add_reservations.append(r)
                self.computed_reservations.append(r)

            return self.computed_reservations
        except OrchestratorException as e:
            self.logger.error("Exception occurred while generating reservations for slivers: {}".format(e))
            raise e
        except Exception as e:
            self.logger.error("Exception occurred while generating reservations for slivers: {}".format(e))
            raise OrchestratorException(message=f"Failure to build Slivers: {e}")

    @staticmethod
    def __validate_node_sliver(*, sliver: NodeSliver):
        """
        Validate Network Node Sliver
        @param sliver Node Sliver
        @raises exception for invalid slivers
        """
        if sliver.get_type() == NodeType.VM and sliver.get_capacities() is None and sliver.get_capacity_hints() is None:
            raise OrchestratorException(message="Either Capacity or Capacity Hints must be specified!",
                                        http_error_code=BAD_REQUEST)
        if sliver.get_type() == NodeType.Switch and sliver.get_capacities() is None:
            raise OrchestratorException(message="Either Capacity must be specified!",
                                        http_error_code=BAD_REQUEST)

    def __build_ns_sliver_reservation(self, *, slice_graph: ABCASMPropertyGraph, node_id: str,
                                      node_res_mapping: Dict[str, str]) -> Tuple[LeaseReservationAvro or None, bool]:
        """
        Build Network Service Reservation
        @param slice_graph Slice graph
        @param node_id Node Id
        @param node_res_mapping Mapping of Network Node sliver Id to reservation Id;
                                Needed to add dependency information in network service slivers
        @return Network service reservation
        """
        dep_update_needed = False

        # Build Network Service Sliver
        sliver = slice_graph.build_deep_ns_sliver(node_id=node_id)
        sliver_type = sliver.get_type()

        self.logger.trace(f"Network Service Sliver: {sliver}")

        # Ignore Sliver types P4,OVS and MPLS
        if sliver_type in Constants.IGNORABLE_NS:
            return None, dep_update_needed

        # Process only the currently supported Network Sliver types L2STS, L2PTP and L2Bridge
        elif sliver_type in Constants.SUPPORTED_SERVICES:
            self.logger.trace(f"Network Service Sliver Interfaces: {sliver.interface_info}")
            # Processing Interface Slivers
            if sliver.interface_info is not None:
                redeem_predecessors = []
                for ifs in sliver.interface_info.interfaces.values():
                    # Get Mapping information for Interface Sliver from ASM
                    # i.e. [Peer IFS, Peer NS Id, Component Name, Node Id]
                    # For Facility IFS [Peer IFS, Peer NS Id, Component Name, Facility Name]
                    ifs_mapping = FimHelper.get_interface_sliver_mapping(ifs_node_id=ifs.node_id,
                                                                         slice_graph=slice_graph)
                    if ifs_mapping is None:
                        raise OrchestratorException(message=f"Peer connection point not found for ifs# {ifs}",
                                                    http_error_code=BAD_REQUEST)

                    # Handle the modify scenario; Only skip the existing slivers;
                    # Retain the parent reservation dependencies for non Facility Port Interface Slivers
                    node_map = ifs.get_node_map()
                    if node_map is not None:
                        if not ifs_mapping.is_facility() and not ifs_mapping.is_peered():
                            parent_res_id = node_res_mapping.get(ifs_mapping.get_node_id(), None)
                            if parent_res_id is not None and parent_res_id not in redeem_predecessors:
                                redeem_predecessors.append(parent_res_id)
                        continue

                    if ifs_mapping.is_facility():
                        # Set Labels
                        ifs.set_labels(lab=ifs_mapping.get_peer_ifs().get_labels())
                        if ifs_mapping.get_peer_ifs().get_peer_labels() is not None:
                            ifs.set_peer_labels(lab=ifs_mapping.get_peer_ifs().get_peer_labels())

                        # For Facility Ports, set Node Map [Facility, Facility Name] to help broker lookup
                        node_map = tuple([str(NodeType.Facility), ifs_mapping.get_node_id()])
                        ifs.set_node_map(node_map=node_map)

                        # capacities (bw in Gbps, burst size is in Mbits) source: (b)
                        # Set Capacities
                        ifs.set_capacities(cap=ifs_mapping.get_peer_ifs().get_capacities())

                    elif ifs_mapping.is_peered():
                        # Peered Interface between L3VPN;
                        # For FABRIC NS IF Sliver, NodeMap [Peered:<peered ns id>:<peer ifs name>,
                        #                                                 Tuple(Site Name (e.g. AWS),
                        #                                                 Node Type (e.g. Facility),
                        #                                                 Node Name (e.g. Cloud_Facility_AWS)]
                        # For AL2S NS IF Sliver, NodeMap [Peered, <site name>]
                        if sliver.get_technology() == Constants.AL2S:
                            node_map = tuple([Constants.PEERED, f"{ifs_mapping.get_peer_site()}"])
                        else:
                            # For FABRIC NS IFS, Need the VLAN to be same as the VLAN allocated by Broker to AL2S NS IFS
                            # Add the AL2S Sliver as a redeem predecessor
                            node_map = tuple([f"{Constants.PEERED},{ifs_mapping.get_peer_ns_id()},{ifs_mapping.get_peer_ifs().get_name()}",
                                              f"{ifs_mapping.get_peer_site()}"])
                            dep_update_needed = True

                        ifs.set_node_map(node_map=node_map)
                        peer_ifs_labels = ifs_mapping.get_peer_ifs().get_labels()
                        if peer_ifs_labels is not None:
                            ifs.set_peer_labels(lab=peer_ifs_labels)
                        # Peer_ifs is AL2S in ASM
                        if peer_ifs_labels is not None and peer_ifs_labels.ipv4_subnet is not None:
                            interface = ipaddress.IPv4Interface(peer_ifs_labels.ipv4_subnet)
                            address_list = list(interface.network.hosts())
                            address_list.remove(interface.ip)
                            if ifs.labels is None:
                                ifs.labels = Labels()
                            ifs.labels = Labels.update(ifs.labels,
                                                       ipv4_subnet=f'{address_list[0]}/{interface.network.prefixlen}')
                            if peer_ifs_labels.bgp_key is not None:
                                if ifs.peer_labels is None:
                                    ifs.peer_labels = Labels()

                                ifs.peer_labels = Labels.update(ifs.peer_labels,
                                                                bgp_key=peer_ifs_labels.bgp_key)
                        # Peer_ifs is FABRIC in ASM
                        else:
                            interface = ipaddress.IPv4Interface(ifs.labels.ipv4_subnet)
                            address_list = list(interface.network.hosts())
                            address_list.remove(interface.ip)
                            if ifs.peer_labels is None:
                                ifs.peer_labels = Labels()
                            ifs.peer_labels = Labels.update(ifs.peer_labels,
                                                            ipv4_subnet=f'{address_list[0]}/{interface.network.prefixlen}',
                                                            bgp_key=ifs.labels.bgp_key)
                    else:
                        # capacities (bw in Gbps, burst size is in Mbits) source: (b)
                        # Set Capacities
                        ifs.set_capacities(cap=ifs_mapping.get_peer_ifs().get_capacities())

                        # Set Labels
                        ifs.set_labels(lab=ifs_mapping.get_peer_ifs().get_labels())

                        # Set Peer Labels
                        if ifs_mapping.get_peer_ifs().get_peer_labels() is not None:
                            ifs.set_peer_labels(lab=ifs_mapping.get_peer_ifs().get_peer_labels())

                        # Save the parent component name and the parent reservation id in the Node Map
                        parent_res_id = node_res_mapping.get(ifs_mapping.get_node_id(), None)

                        node_map = tuple([ifs_mapping.get_component_name(), parent_res_id])
                        ifs.set_node_map(node_map=node_map)

                        self.logger.trace(f"Interface Sliver: {ifs}")

                        if parent_res_id is not None and parent_res_id not in redeem_predecessors:
                            redeem_predecessors.append(parent_res_id)

                    if ifs.peer_labels is not None and ifs.peer_labels.account_id is not None:
                        if sliver.labels is None:
                            sliver.labels = Labels()
                        sliver.labels = Labels.update(sliver.labels,
                                                      local_name=f"{self.slice_obj.get_slice_name()}")

                # Generate reservation for the sliver
                reservation = self.reservation_converter.generate_reservation(sliver=sliver,
                                                                              slice_id=self.slice_obj.get_slice_id(),
                                                                              end_time=self.slice_obj.get_lease_end(),
                                                                              pred_list=redeem_predecessors,
                                                                              start_time=self.slice_obj.get_lease_start())

                if sliver.node_id not in node_res_mapping:
                    node_res_mapping[sliver.node_id] = reservation.get_reservation_id()
                return reservation, dep_update_needed
            else:
                raise OrchestratorException(message="Not implemented",
                                            http_error_code=BAD_REQUEST)

    def __update_peered_ns_dependencies(self, *, ns_peered_reservations: List[LeaseReservationAvro],
                                        ns_mapping: Dict[str, str]):
        for r in ns_peered_reservations:
            for ifs in r.sliver.interface_info.interfaces.values():
                t1, t2 = ifs.get_node_map()
                if Constants.PEERED not in t1:
                    continue
                result = t1.split(",")
                if len(result) > 1:
                    dep_ns_id = result[1]
                    predecessor = ReservationPredecessorAvro()
                    predecessor.reservation_id = ns_mapping.get(dep_ns_id)
                    r.redeem_processors.append(predecessor)
                    if r.get_reservation_id() in self.computed_modify_reservations:
                        self.computed_modify_reservations[r.get_reservation_id()].dependencies.append(predecessor)
                    # Update NODE MAP
                    ifs.set_node_map(node_map=(f"{result[0]},{predecessor.reservation_id},{result[2]}", t2))

    def __build_network_service_reservations(self, *, slice_graph: ABCASMPropertyGraph,
                                             node_res_mapping: Dict[str, str]) -> List[LeaseReservationAvro]:
        """
        Build Network Service Reservations
        @param slice_graph Slice graph
        @param node_res_mapping Mapping of Network Node sliver Id to reservation Id;
                                Needed to add dependency information in network service slivers
        @return list of network service reservations
        """
        reservations = []
        ns_mapping = {}
        ns_peered_reservations = []
        for ns_id in slice_graph.get_all_network_service_nodes():

            # Build Network Service Sliver
            reservation, dep_update_needed = self.__build_ns_sliver_reservation(slice_graph=slice_graph,
                                                                                node_id=ns_id,
                                                                                node_res_mapping=node_res_mapping)
            if reservation is None:
                continue

            if reservation.get_resource_type() in Constants.L3_FABNET_SERVICES_STR:
                self.computed_l3_reservations.append(reservation)

            reservations.append(reservation)
            ns_mapping[reservation.sliver.node_id] = reservation.get_reservation_id()
            if dep_update_needed:
                ns_peered_reservations.append(reservation)
        self.__update_peered_ns_dependencies(ns_peered_reservations=ns_peered_reservations, ns_mapping=ns_mapping)
        return reservations

    def __build_node_sliver_reservation(self, *, slice_graph: ABCASMPropertyGraph,
                                        node_id: str) -> LeaseReservationAvro or None:
        """
        Build Network Node Reservations
        @param slice_graph Slice graph
        @param node_id Node Id
        @return list of node reservations
        """
        # Build Network Node Sliver
        sliver = slice_graph.build_deep_node_sliver(node_id=node_id)

        if sliver.get_type() not in [NodeType.VM, NodeType.Switch]:
            return None

        # Validate Node Sliver
        self.__validate_node_sliver(sliver=sliver)

        if sliver.get_type() == NodeType.VM:
            # Compute Requested Capacities from Capacity Hints
            FimHelper.compute_capacities(sliver=sliver)

        # Generate reservation for the sliver
        reservation = self.reservation_converter.generate_reservation(sliver=sliver,
                                                                      slice_id=self.slice_obj.get_slice_id(),
                                                                      end_time=self.slice_obj.get_lease_end(),
                                                                      start_time=self.slice_obj.get_lease_start())
        return reservation

    def __build_network_node_reservations(self, *, slice_graph: ABCASMPropertyGraph) \
            -> Tuple[List[LeaseReservationAvro], Dict[str, str]]:
        """
        Build Network Node Reservations
        @param slice_graph Slice graph
        @return list of node reservations
        """
        reservations = []
        sliver_to_res_mapping = {}
        for nn_id in slice_graph.get_all_network_nodes():
            reservation = self.__build_node_sliver_reservation(slice_graph=slice_graph, node_id=nn_id)

            if reservation is None:
                continue
            reservations.append(reservation)

            self.logger.trace(f"Mapped sliver: {nn_id} to res: {reservation.get_reservation_id()}")

            # Maintain Sliver Id to Reservation Mapping
            sliver_to_res_mapping[nn_id] = reservation.get_reservation_id()
        return reservations, sliver_to_res_mapping

    def modify(self, *, new_slice_graph: ABCASMPropertyGraph) -> Tuple[TopologyDiff, List[LeaseReservationAvro]]:
        """
        Modify an existing slice
        :param new_slice_graph New Slice Graph
        :param topology Experiment Topology
        :return: List of computed reservations
        """
        existing_topology = FimHelper.get_experiment_topology(graph_id=self.slice_obj.get_graph_id())

        new_topology = ExperimentTopology()

        new_topology.cast(asm_graph=new_slice_graph)
        topology_diff = existing_topology.diff(new_topology)

        reservations = []
        ns_peered_reservations = []
        ns_mapping = {}

        if not topology_diff:
            return topology_diff, reservations

        node_res_mapping = {}

        # Build up the node_res mapping to include nodes before modify
        # This is needed for Network Service slivers when interfaces from VM before modify
        # are added to the new Network Service slivers
        for x in new_topology.nodes.values():
            if x in topology_diff.added.nodes or x in topology_diff.removed.nodes:
                continue
            node_res_mapping[x.node_id] = x.reservation_info.reservation_id

        # Build Network Service mapping to update dependencies for Peered Network Services
        for x in new_topology.nodes.values():
            if x in topology_diff.added.nodes or x in topology_diff.removed.nodes:
                continue
            ns_mapping[x.node_id] = x.reservation_info.reservation_id

        # Add Nodes
        for x in topology_diff.added.nodes:
            reservation = self.__build_node_sliver_reservation(slice_graph=new_slice_graph, node_id=x.node_id)
            if reservation is None:
                continue
            reservations.append(reservation)
            node_res_mapping[x.node_id] = reservation.get_reservation_id()

        # Add Network Services
        for x in topology_diff.added.services:
            if x.get_sliver().get_type() in Constants.IGNORABLE_NS:
                continue
            reservation, dep_update_needed = self.__build_ns_sliver_reservation(slice_graph=new_slice_graph,
                                                                                node_id=x.node_id,
                                                                                node_res_mapping=node_res_mapping)
            reservations.append(reservation)
            ns_mapping[reservation.sliver.node_id] = reservation.get_reservation_id()
            if dep_update_needed:
                ns_peered_reservations.append(reservation)

        # Add components
        for x in topology_diff.added.components:
            sliver, parent_node_id = FimHelper.get_parent_node(graph_model=new_slice_graph, node=x)
            rid = sliver.reservation_info.reservation_id
            # If corresponding sliver also has add operations; it's already in the map
            # No need to rebuild it
            if rid not in self.computed_modify_reservations:
                self.computed_modify_reservations[rid] = ModifiedReservation(sliver=sliver)

        # Remove components
        for x in topology_diff.removed.components:
            # Grab the old sliver
            sliver, parent_node_id = FimHelper.get_parent_node(graph_model=existing_topology.graph_model, node=x)
            rid = sliver.reservation_info.reservation_id
            # If corresponding sliver also has add operations; it's already in the map
            # No need to rebuild it
            if rid not in self.computed_modify_reservations:
                sliver = new_slice_graph.build_deep_node_sliver(node_id=parent_node_id)
                self.computed_modify_reservations[rid] = ModifiedReservation(sliver=sliver)

        # Added Interfaces
        for x in topology_diff.added.interfaces:
            new_sliver, parent_node_id = FimHelper.get_parent_node(graph_model=new_slice_graph, node=x)
            rid = new_sliver.reservation_info.reservation_id
            # If corresponding sliver also has add operations; it's already in the map
            # No need to rebuild it
            if rid not in self.computed_modify_reservations:
                if x.type == InterfaceType.SubInterface:
                    self.computed_modify_reservations[rid] = ModifiedReservation(sliver=new_sliver)
                else:
                    new_reservation, dep_update_needed = self.__build_ns_sliver_reservation(slice_graph=new_slice_graph,
                                                                                            node_id=parent_node_id,
                                                                                            node_res_mapping=node_res_mapping)
                    self.computed_modify_reservations[rid] = ModifiedReservation(sliver=new_reservation.get_sliver(),
                                                                                 dependencies=new_reservation.redeem_processors)

                    if dep_update_needed:
                        ns_peered_reservations.append(new_reservation)
                    ns_mapping[new_reservation.sliver.node_id] = rid

        # Removed Interfaces
        for x in topology_diff.removed.interfaces:
            sliver, parent_node_id = FimHelper.get_parent_node(graph_model=existing_topology.graph_model, node=x)
            rid = sliver.reservation_info.reservation_id
            # If corresponding sliver also has add operations; it's already in the map
            # No need to rebuild it
            if rid not in self.computed_modify_reservations:
                if x.type == InterfaceType.SubInterface:
                    new_sliver = new_slice_graph.build_deep_node_sliver(node_id=parent_node_id)
                    self.computed_modify_reservations[rid] = ModifiedReservation(sliver=new_sliver)
                else:
                    new_reservation, dep_update_needed = self.__build_ns_sliver_reservation(slice_graph=new_slice_graph,
                                                                                            node_id=parent_node_id,
                                                                                            node_res_mapping=node_res_mapping)
                    self.computed_modify_reservations[rid] = ModifiedReservation(sliver=new_reservation.get_sliver(),
                                                                                 dependencies=new_reservation.redeem_processors)

                    if dep_update_needed:
                        ns_peered_reservations.append(new_reservation)
                    ns_mapping[new_reservation.sliver.node_id] = rid

        # Remove nodes
        for x in topology_diff.removed.nodes:
            if x.type != NodeType.Facility:
                self.computed_remove_reservations.append(x.reservation_info.reservation_id)

        # Remove services
        for x in topology_diff.removed.services:
            if x.get_sliver().get_type() in Constants.IGNORABLE_NS:
                continue
            reservation_info = x.get_property('reservation_info')
            self.computed_remove_reservations.append(reservation_info.reservation_id)

        # Modified Interfaces
        for x, flag in topology_diff.modified.interfaces:
            if not (flag & WhatsModifiedFlag.CAPACITIES):
                continue
            sliver, parent_node_id = FimHelper.get_parent_node(graph_model=existing_topology.graph_model, node=x)
            if not sliver.reservation_info or not sliver.reservation_info.reservation_id:
                self.logger.warning(f"Skipping modified interface -- possibly the interface on component; "
                                    f"update on corresponding network service interface would be picked -- {x}")
                continue
            rid = sliver.reservation_info.reservation_id
            # If corresponding sliver also has add operations; it's already in the map
            # No need to rebuild it
            if rid not in self.computed_modify_reservations:
                if x.type == InterfaceType.SubInterface:
                    new_sliver = new_slice_graph.build_deep_node_sliver(node_id=parent_node_id)
                    self.computed_modify_reservations[rid] = ModifiedReservation(sliver=new_sliver)
                else:
                    new_reservation, dep_update_needed = self.__build_ns_sliver_reservation(
                        slice_graph=new_slice_graph,
                        node_id=parent_node_id,
                        node_res_mapping=node_res_mapping)
                    self.computed_modify_reservations[rid] = ModifiedReservation(
                        sliver=new_reservation.get_sliver(),
                        dependencies=new_reservation.redeem_processors)

                    if dep_update_needed:
                        ns_peered_reservations.append(new_reservation)
                    ns_mapping[new_reservation.sliver.node_id] = rid

        # Update Dependencies for Peered NS
        self.__update_peered_ns_dependencies(ns_peered_reservations=ns_peered_reservations, ns_mapping=ns_mapping)

        # Add the new reservations to the controller
        for r in reservations:
            self.computed_add_reservations.append(r)
            self.computed_reservations.append(r)

            if r.get_resource_type() in Constants.L3_FABNET_SERVICES_STR:
                self.computed_l3_reservations.append(r)

        # Processing the reservations which have Label Updates
        modified_reservations = []

        for new_ns, flag in topology_diff.modified.services:
            if flag & WhatsModifiedFlag.LABELS:
                # Only support modify for FabNet Services
                if new_ns.type not in Constants.L3_FABNET_SERVICES:
                    continue
                rid = new_ns.reservation_info.reservation_id
                reservation, dep_update_needed = self.__build_ns_sliver_reservation(slice_graph=new_slice_graph,
                                                                                    node_res_mapping=node_res_mapping,
                                                                                    node_id=new_ns.node_id)
                reservation.set_reservation_id(value=rid)
                modified_reservations.append(reservation)
                #self.computed_modify_properties_reservations.append(reservation)
                if new_ns.type == ServiceType.FABNetv4Ext:
                    self.__check_modify_on_fabnetv4ext(rid=rid, req_sliver=reservation.get_sliver())

                self.computed_modify_reservations[rid] = ModifiedReservation(sliver=reservation.get_sliver(),
                                                                             dependencies=reservation.redeem_processors)

        for x in modified_reservations:
            self.computed_reservations.append(x)

        return topology_diff, self.computed_reservations

    def __check_modify_on_fabnetv4ext(self, *, rid: str, req_sliver: NetworkServiceSliver) -> NetworkServiceSliver:
        if req_sliver.get_type() != ServiceType.FABNetv4Ext:
            return req_sliver

        num_ips_to_be_updated = 0

        states = [ReservationStates.Active.value,
                  ReservationStates.ActiveTicketed.value,
                  ReservationStates.Ticketed.value,
                  ReservationStates.Nascent.value]

        if req_sliver.labels is not None and req_sliver.labels.ipv4 is not None and len(req_sliver.labels.ipv4) > 0:
            bqm_graph_id, owner_mpls_node_id = req_sliver.get_node_map()

            existing_reservations = self.controller.get_reservations(node_id=owner_mpls_node_id, states=states,
                                                                     full=True)
            ip_network = IPv4Network(req_sliver.gateway.lab.ipv4_subnet)
            ipaddress_list = list(ip_network.hosts())
            ipaddress_list.pop(0)

            # Exclude the already allocated Public IPs
            for reservation in existing_reservations:
                if rid == reservation.get_reservation_id():
                    continue

                if not reservation.get_sliver():
                    self.logger.warning(f"No sliver found, Skipping reservation: {reservation.get_reservation_id()}")
                    continue

                if reservation.get_sliver().get_type() != req_sliver.get_type():
                    continue

                allocated_sliver = None
                pending_state = ReservationPendingStates(reservation.get_pending_state())
                state = ReservationStates(reservation.get_state())

                # For Active or Ticketed or Ticketing reservations; remove IPs from available list
                if pending_state == ReservationPendingStates.Ticketing or \
                        state in [ReservationStates.Active, ReservationStates.Ticketed]:
                    allocated_sliver = reservation.get_sliver()

                self.logger.debug(f"Existing res# {reservation.get_reservation_id()} "
                                  f"allocated: {allocated_sliver}")

                if allocated_sliver is None:
                    continue

                if allocated_sliver.labels is not None and allocated_sliver.labels.ipv4 is not None:
                    for x in allocated_sliver.labels.ipv4:
                        ip_to_remove = ipaddress.IPv4Address(x)
                        ipaddress_list.remove(ip_to_remove)
                        self.logger.debug(f"Excluding already allocated IPv4: {x} to "
                                          f"res# {reservation.get_reservation_id()}")

            if len(ipaddress_list) == 0 or len(ipaddress_list) < len(req_sliver.labels.ipv4):
                raise OrchestratorException(message="No available v4 Public IPs")

            ip_to_remove = []
            for x in req_sliver.labels.ipv4:
                ip_add = ipaddress.IPv4Address(x)
                if ip_add not in ipaddress_list:
                    ip_to_remove.append(x)
                else:
                    ipaddress_list.remove(ip_add)

            num_ips_to_be_updated = len(ip_to_remove)
            for x in ip_to_remove:
                req_sliver.labels.ipv4.remove(x)

            for i in range(num_ips_to_be_updated):
                ip_add = str(ipaddress_list.pop(0))
                req_sliver.labels.ipv4.append(ip_add)

        if req_sliver.reservation_info is not None and num_ips_to_be_updated > 0:
            req_sliver.reservation_info.error_message = "IP Addresses were updated due to conflicts"

        return req_sliver

    def update_topology(self, *, topology: ExperimentTopology = None,
                        asm_graph: ABCASMPropertyGraph = None):
        if topology:
            for x in self.computed_reservations:
                sliver = x.get_sliver()
                node_name = sliver.get_name()
                if isinstance(sliver, NodeSliver) and node_name in topology.nodes:
                    node = topology.nodes[node_name]
                    node.set_properties(labels=sliver.labels,
                                        label_allocations=sliver.label_allocations,
                                        capacity_allocations=sliver.capacity_allocations,
                                        reservation_info=sliver.reservation_info,
                                        node_map=sliver.node_map,
                                        management_ip=sliver.management_ip,
                                        capacity_hints=sliver.capacity_hints,
                                        capacities=sliver.capacities)

    def has_sliver_updates_at_authority(self):
        return len(self.computed_reservations) or len(self.computed_remove_reservations) or \
               len(self.computed_modify_reservations) or len(self.computed_modify_properties_reservations)

    def has_topology_diffs(self, *, topology_diff: TopologyDiff) -> bool:
        """
        Check if there any updates in topology
        :param topology_diff: topology difference object
        """
        ret_val = False
        if not topology_diff:
            ret_val = False

        if len(topology_diff.added.nodes) or len(topology_diff.added.components) or \
                len(topology_diff.added.services) or len(topology_diff.added.interfaces):
            ret_val = True

        if len(topology_diff.removed.nodes) or len(topology_diff.removed.components) or \
                len(topology_diff.removed.services) or len(topology_diff.removed.interfaces):
            ret_val = True

        if len(topology_diff.modified.nodes) or len(topology_diff.modified.components) or \
                len(topology_diff.modified.services) or len(topology_diff.modified.interfaces):
            ret_val = True

        self.logger.debug(f"Topology diff found: {ret_val}")
        return ret_val
