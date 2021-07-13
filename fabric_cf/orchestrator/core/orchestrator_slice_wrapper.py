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
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver
from fim.user import ServiceType, ComponentType

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
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
        self.demanded = []
        self.thread_lock = threading.Lock()
        self.ignorable_ns = [ServiceType.P4, ServiceType.OVS, ServiceType.MPLS]
        self.supported_ns = [ServiceType.L2STS, ServiceType.L2Bridge, ServiceType.L2PTP]

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

    def demanded_reservations(self) -> List[str]:
        """
        Return list of reservations demanded from the Actor/Policy
        @return list of demanded reservations
        """
        return self.demanded

    def mark_demanded(self, *, rid: str):
        """
        Mark a reservation demanded
        @param rid: Reservation id
        """
        if rid not in self.demanded:
            self.demanded.append(rid)

    def get_computed_reservations(self) -> List[TicketReservationAvro]:
        """
        Get computed reservations
        :return: computed reservations
        """
        return self.computed_reservations

    def check_predecessors_ticketed_by_rid(self, *, rid: str) -> bool:
        """
        Check if all the parent reservations have been ticketed
        @param rid: Reservation Id
        @return True if parent reservations are ticketed; False otherwise
        """
        res = self.rid_to_res.get(rid, None)

        if res is not None:
            return self.check_predecessors_ticketed(reservation=res)

        return True

    def check_predecessors_ticketed(self, *, reservation: TicketReservationAvro) -> bool:
        """
        Check if all the parent reservations have been ticketed
        @param reservation: Reservation
        @return True if parent reservations are ticketed; False otherwise
        """
        ret_val = True
        if isinstance(reservation, LeaseReservationAvro) and reservation.get_redeem_predecessors() is not None and \
                len(reservation.get_redeem_predecessors()) > 0:

            state_bin = [ReservationStates.Ticketed, ReservationStates.Failed, ReservationStates.Closed,
                         ReservationStates.CloseWait]

            rids = []
            for r in reservation.get_redeem_predecessors():
                rids.append(r.get_reservation_id())

            reservations = self.controller.get_reservations(rid_list=rids)
            rid_res_map = {}

            for r in reservations:
                rid_res_map[r.get_reservation_id()] = r
                state = ReservationStates(r.get_state())
                self.logger.trace(f"Parent Res# {r.get_reservation_id()} is in state# {state}")

                if state not in state_bin:
                    ret_val = False
                    break

            # Parent reservations have been Ticketed; Update BQM Node and Component Id in Node Map
            # Used by Broker to set vlan - source: (c)
            # local_name source: (a)
            # NSO device name source: (a) - need to find the owner switch of the network service in CBM
            # and take its .name or labels.local_name
            if ret_val:
                sliver = reservation.get_sliver()
                for ifs in sliver.interface_info.interfaces.values():
                    component_name, rid = ifs.get_node_map()
                    parent_res = rid_res_map.get(rid)
                    if parent_res is not None and \
                            ReservationStates(parent_res.get_state()) == ReservationStates.Ticketed:
                        node_sliver = parent_res.get_sliver()
                        component = node_sliver.attached_components_info.get_device(name=component_name)
                        graph_id, bqm_component_id = component.get_node_map()
                        graph_id, node_id = node_sliver.get_node_map()
                        ifs.set_node_map(node_map=(node_id, bqm_component_id))

                        # For shared NICs grab the MAC & VLAN from corresponding Interface Sliver
                        # maintained in the Parent Reservation Sliver
                        if component.get_type() == ComponentType.SharedNIC:
                            parent_res_ifs_sliver = FimHelper.get_site_interface_sliver(component=component,
                                                                                        local_name=ifs.get_labels().local_name)
                            parent_labs = parent_res_ifs_sliver.get_label_allocations()

                            ifs.labels.set_fields(mac=parent_labs.mac, vlan=parent_labs.vlan)

                reservation.set_sliver(sliver=sliver)

        self.logger.trace(f"Res# {reservation.get_reservation_id()} {ret_val}")
        return ret_val

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

    def create(self, *, slice_graph: ABCASMPropertyGraph) -> List[TicketReservationAvro]:
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

            # Add Network Node reservations
            for r in network_service_reservations:
                self.controller.add_reservation(reservation=r)

            # Add to computed reservations
            for x in network_node_reservations:
                self.computed_reservations.append(x)
                self.rid_to_res[x.get_reservation_id()] = x

            for x in network_service_reservations:
                self.computed_reservations.append(x)
                self.rid_to_res[x.get_reservation_id()] = x

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

    @staticmethod
    def __validate_network_service_sliver(sliver: NetworkServiceSliver):
        """
        Validate Network Node Sliver
        @param sliver Node Sliver
        @raises exception for invalid slivers
        """
        for ifs in sliver.interface_info.interfaces.values():
            if ifs.labels is not None:
                vlan = int(ifs.get_labels().vlan)
                if vlan <= Constants.VLAN_START or vlan >= Constants.VLAN_END:
                    raise OrchestratorException(message=f"Allowed range for VLAN ({Constants.VLAN_START}-{Constants.VLAN_END})",
                                                http_error_code=BAD_REQUEST)

    def __build_network_service_reservations(self, slice_graph: ABCASMPropertyGraph,
                                             node_res_mapping: Dict[str, str]) -> List[TicketReservationAvro]:
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

            # Ignore Sliver types P4,OVS and MPLS
            if sliver_type in self.ignorable_ns:
                continue

            # Process only the currently supported Network Sliver types L2STS, L2PTP and L2Bridge
            elif sliver_type in self.supported_ns:

                self.logger.trace(f"Network Service Sliver: {sliver}")

                self.__validate_network_service_sliver(sliver=sliver)

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

                        # Save the parent component name and the parent reservation id in the Node Map
                        parent_res_id = node_res_mapping.get(ifs_mapping.get_node_id(), None)

                        node_map = tuple([ifs_mapping.get_component_name(), parent_res_id])
                        ifs.set_node_map(node_map=node_map)

                        self.logger.trace(f"Interface Sliver: {ifs}")

                        if parent_res_id is not None and parent_res_id not in predecessor_reservations:
                            predecessor_reservations.append(parent_res_id)

                    # Generate reservation for the sliver
                    reservation = self.reservation_converter.generate_reservation(sliver=sliver,
                                                                                  slice_id=self.slice_obj.get_slice_id(),
                                                                                  end_time=self.slice_obj.get_lease_end(),
                                                                                  pred_list=predecessor_reservations)
                    reservations.append(reservation)
            else:

                raise OrchestratorException(message="Not implemented",
                                            http_error_code=BAD_REQUEST)
        return reservations

    def __build_network_node_reservations(self, slice_graph: ABCASMPropertyGraph) \
            -> Tuple[List[TicketReservationAvro], Dict[str, str]]:
        reservations = []
        sliver_to_res_mapping = {}
        for nn_id in slice_graph.get_all_network_nodes():

            # Build Network Node Sliver
            sliver = slice_graph.build_deep_node_sliver(node_id=nn_id)

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
                requested_capacity_hints = CapacityHints().set_fields(instance_type=instance_type)
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
