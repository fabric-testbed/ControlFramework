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
import traceback
from datetime import datetime
from typing import Tuple

from fim.graph.resources.neo4j_arm import Neo4jARMGraph
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver

from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.authority_policy import AuthorityPolicy
from fabric_cf.actor.core.common.exceptions import AuthorityException
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
from fabric_cf.actor.core.apis.abc_resource_control import ABCResourceControl
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.time.calendar.authority_calendar import AuthorityCalendar
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.fim.fim_helper import FimHelper


class AuthorityCalendarPolicy(AuthorityPolicy):
    """
    The base for authority policy implementations
    """

    def __init__(self):
        """
        Creates a new instance.
        """
        super().__init__()
        # If true, we will use lazy revocation.
        self.lazy_close = False
        # Resource control objects indexed by guid. <guid, IResourceControl>
        self.controls_by_guid = {}
        # ResourceControl objects indexed by resource type. <type, IResourceControl>
        self.controls_by_resource_type = {}
        # The authority's calendar. A calendar of all requests
        self.calendar = None
        # Says if the actor has been initialized
        self.initialized = False

        self.aggregate_resource_model = None
        self.aggregate_resource_model_graph_id = None

        self.lock = threading.Lock()

    def load_aggregate_resource_model(self):
        if self.aggregate_resource_model_graph_id is not None:
            self.logger.debug(f"Loading an existing Aggregate ResourceModel Graph:"
                              f" {self.aggregate_resource_model_graph_id}")

            self.aggregate_resource_model = FimHelper.get_arm_graph(graph_id=self.aggregate_resource_model_graph_id)
            self.logger.debug(f"Successfully loaded an existing Aggregate Resource Model Graph: "
                              f"{self.aggregate_resource_model_graph_id}")

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']
        del state['controls_by_resource_type']
        del state['calendar']
        del state['aggregate_resource_model']
        del state['lock']
        del state['delegations']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.actor = None
        self.clock = None
        self.initialized = False
        self.controls_by_resource_type = {}
        self.aggregate_resource_model = None

        self.lock = threading.Lock()
        self.restore()

    def restore(self):
        """
        Custom restore function. Invoked during recovering the policy object.
        """
        for c in self.controls_by_guid.values():
            try:
                self.register_control_types(control=c)
            except Exception as e:
                raise AuthorityException(f"Cannot restore resource control e:{e}")

    def set_aggregate_resource_model(self, aggregate_resource_model: Neo4jARMGraph):
        self.aggregate_resource_model = aggregate_resource_model
        if aggregate_resource_model is not None:
            self.set_aggregate_resource_model_graph_id(graph_id=aggregate_resource_model.graph_id)

    def set_aggregate_resource_model_graph_id(self, graph_id: str):
        self.aggregate_resource_model_graph_id = graph_id

    def initialize(self):
        """
        initialize the policy
        """
        if not self.initialized:
            super().initialize()
            self.calendar = AuthorityCalendar(clock=self.clock)
            self.initialize_controls()
            self.load_aggregate_resource_model()
            self.initialized = True

    def initialize_controls(self):
        """
        Initializes all registered controls.
        @raises Exception in case of error
        """
        for control in self.controls_by_guid.values():
            control.set_actor(actor=self.actor)
            control.initialize()

    def eject(self, *, resources: ResourceSet):
        rc = self.get_control_by_type(rtype=resources.get_type())
        if rc is not None:
            return rc.unavailable(resource_set=resources)
        else:
            raise AuthorityException(Constants.UNSUPPORTED_RESOURCE_TYPE.format(resources.get_type()))

    def available(self, *, resources: ResourceSet):
        rc = self.get_control_by_type(rtype=resources.get_type())
        if rc is not None:
            rc.available(resource_set=resources)
        else:
            raise AuthorityException(Constants.UNSUPPORTED_RESOURCE_TYPE.format(resources.get_type()))

    def freed(self, *, resources: ResourceSet):
        super().freed(resources=resources)
        rc = self.get_control_by_type(rtype=resources.get_type())
        if rc is not None:
            rc.freed(resource_set=resources)
        else:
            raise AuthorityException(Constants.UNSUPPORTED_RESOURCE_TYPE.format(resources.get_type()))

    def release(self, *, resources: ResourceSet):
        super().release(resources=resources)
        rc = self.get_control_by_type(rtype=resources.get_type())
        if rc is not None:
            rc.release(resource_set=resources)
        else:
            raise AuthorityException(Constants.UNSUPPORTED_RESOURCE_TYPE.format(resources.get_type()))

    def recovery_starting(self):
        super().recovery_starting()
        for c in self.controls_by_guid.values():
            c.recovery_starting()

    def revisit(self, *, reservation: ABCReservationMixin):
        super().revisit(reservation=reservation)
        if isinstance(reservation, ABCAuthorityReservation):
            term = reservation.get_term()
            if term is None:
                term = reservation.get_requested_term()
            self.calendar.add_closing(reservation=reservation, cycle=self.get_close(term=term))
            approved = reservation.get_approved_resources()
            if approved is None:
                self.logger.debug("Reservation has no approved resources. Nothing is allocated to it.")
                return

            rtype = approved.get_type()
            self.logger.debug(f"Resource type for recovered reservation: {rtype}")
            control = self.get_control_by_type(rtype=rtype)
            if control is None:
                raise AuthorityException("Missing resource control")
            control.revisit(reservation=reservation)

    def recovery_ended(self):
        super().recovery_ended()
        for c in self.controls_by_guid.values():
            c.recovery_ended()

    def bind(self, *, reservation: ABCAuthorityReservation) -> bool:
        # Simple for now: make sure that this is a valid term and do not modify
        # its start/end time and add it to the calendar. If the request came
        # after its start time, but before its end cycle, add it for allocation
        # to lastAllocatedCycle + 1. If it came after its end cycle, throw.
        current_cycle = self.actor.get_current_cycle()
        approved = reservation.get_requested_term()
        start = self.clock.cycle(when=approved.get_new_start_time())

        if start <= current_cycle:
            end = self.clock.cycle(when=approved.get_end_time())
            if end <= current_cycle:
                self.error(message="The request cannot be redeemed: its term has expired")
            start = current_cycle + 1

        self.calendar.add_request(reservation=reservation, cycle=start)
        close = self.get_close(term=reservation.get_requested_term())
        self.calendar.add_closing(reservation=reservation, cycle=close)
        return False

    def extend_authority(self, *, reservation: ABCAuthorityReservation) -> bool:
        # Simple for now: make sure that this is a valid term and do not modify
        # its start/end time and add it to the calendar. If the request came
        # after its start time, but before its end cycle, add it for allocation
        # to lastAllocatedCycle + 1. If it came after its end cycle, throw an
        # exception.
        current_cycle = self.actor.get_current_cycle()
        approved = reservation.get_requested_term()
        start = self.clock.cycle(when=approved.get_new_start_time())

        if start <= current_cycle:
            end = self.clock.cycle(when=approved.get_end_time())
            if end <= current_cycle:
                self.error(message="The request cannot be redeemed: its term has expired")
            start = current_cycle + 1

        self.calendar.remove_closing(reservation=reservation)
        self.calendar.add_request(reservation=reservation, cycle=start)
        close = self.get_close(term=reservation.get_requested_term())
        self.calendar.add_closing(reservation=reservation, cycle=close)
        return False

    def extend(self, *, reservation: ABCReservationMixin, resources: ResourceSet, term: Term):
        raise AuthorityException(Constants.NOT_IMPLEMENTED)

    def correct_deficit(self, *, reservation: ABCAuthorityReservation):
        if reservation.get_resources() is not None:
            rc = self.get_control_by_type(rtype=reservation.get_resources().get_type())
            if rc is not None:
                self.finish_correct_deficit(rset=rc.correct_deficit(reservation=reservation), reservation=reservation)
            else:
                raise AuthorityException(Constants.UNSUPPORTED_RESOURCE_TYPE.format(reservation.get_type()))

    def close(self, *, reservation: ABCReservationMixin):
        self.calendar.remove_schedule_or_in_progress(reservation=reservation)
        if reservation.get_type() is not None:
            rc = self.get_control_by_type(rtype=reservation.get_type())
            if rc is not None:
                rc.close(reservation=reservation)
            else:
                raise AuthorityException(Constants.UNSUPPORTED_RESOURCE_TYPE.format(reservation.get_type()))

    def closed(self, *, reservation: ABCReservationMixin):
        if isinstance(reservation, ABCAuthorityReservation):
            self.calendar.remove_outlay(reservation=reservation)

    def remove(self, *, reservation: ABCReservationMixin):
        raise AuthorityException(Constants.NOT_IMPLEMENTED)

    def finish(self, *, cycle: int):
        super().finish(cycle=cycle)
        self.calendar.tick(cycle=cycle)

    def assign(self, *, cycle: int):
        try:
            requests = self.get_requests(cycle=cycle)
            self.map_for_cycle(requests=requests, cycle=cycle)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"error in assign: {e}")

    def map_for_cycle(self, *, requests: ReservationSet, cycle: int):
        """
        Orders mapper request processing for this cycle.

        @params requests The requests for this cycle
        @params cycle The cycle
        @raises Exception in case of error
        """
        if requests is None or cycle == 0:
            # self.logger.debug("Authority requests for cycle {} = [none]".format(cycle))
            return

        # self.logger.debug("Authority requests for cycle {} = {}".format(cycle, requests))

        self.map_shrinking(bids=requests)
        self.map_growing(bids=requests)

    def map_shrinking(self, *, bids: ReservationSet):
        """
        Maps reservations that are shrinking or staying the same (extending with
        no flex) in this cycle, and removes them from the bid set.

        @param bids set of deferred operations for this cycle (non-null)
        @raises Exception in case of error
        """
        # self.logger.debug("Processing shrinking requests")
        rids_to_remove = []
        node_id_to_reservations = {}
        for reservation in bids.values():
            adjust = reservation.get_deficit()
            if adjust > 0:
                continue
            if not reservation.is_terminal() and reservation.is_extending_lease():
                if adjust < 0:
                    self.logger.debug(f"**Shrinking reservation by {adjust}:{reservation}")
                else:
                    self.logger.debug(f"**Extending reservation (no flex): {reservation}")
                node_id_to_reservations = self.map(reservation=reservation,
                                                   node_id_to_reservations=node_id_to_reservations)
                rids_to_remove.append(reservation.get_reservation_id())

        for rid in rids_to_remove:
            bids.remove_by_rid(rid=rid)

    def map_growing(self, *, bids: ReservationSet):
        """
        Maps reservations that are growing in this cycle (redeems or expanding
        extends), and removes them from the bid set.
        @param bids set of deferred operations for this cycle (non-null)
        @throws Exception in case of error
        """
        # self.logger.debug("Processing growing requests")
        rids_to_remove = []
        node_id_to_reservations = {}
        for reservation in bids.values():
            if reservation.is_terminal():
                continue
            adjust = reservation.get_deficit()

            if adjust > 0:
                if reservation.is_extending_lease():
                    self.logger.debug(f"**Growing reservation by {adjust}:{reservation}")
                else:
                    self.logger.debug(f"**Redeeming reservation by {adjust}:{reservation}")
            node_id_to_reservations = self.map(reservation=reservation, node_id_to_reservations=node_id_to_reservations)
            rids_to_remove.append(reservation.get_reservation_id())

        for rid in rids_to_remove:
            bids.remove_by_rid(rid=rid)

    def map(self, *, reservation: ABCAuthorityReservation, node_id_to_reservations: dict) -> dict:
        """
        Maps a reservation. Indicates we will approve the request: update its
        expire time in the calendar, and issue a map probe. The map probe will
        result in a retry of the mapper request through bind or extend
        above, which will release the request to the associated mapper.

        @param reservation: the reservation
        @param node_id_to_reservations: node_id_to_reservations
        @throws Exception in case of error
        """
        assigned = self.assign_reservation(reservation=reservation, node_id_to_reservations=node_id_to_reservations)
        if assigned is not None:
            approved = reservation.get_requested_term()
            reservation.set_approved(term=approved, approved_resources=assigned)
            reservation.set_bid_pending(value=False)
            node_id = assigned.get_sliver().get_node_map()[1]

            if node_id_to_reservations.get(node_id, None) is None:
                node_id_to_reservations[node_id] = ReservationSet()
            node_id_to_reservations[node_id].add(reservation=reservation)
        else:
            if not reservation.is_terminal():
                self.logger.debug(f"Deferring reservation {reservation} for the next cycle: "
                                  f"{self.actor.get_current_cycle() + 1}")
                self.reschedule(reservation=reservation)

        return node_id_to_reservations

    def assign_reservation(self, *, reservation: ABCAuthorityReservation, node_id_to_reservations: dict):
        """
        Assign resources for the given reservation

        @params reservation the request
        @params node_id_to_reservations node_id_to_reservations
        @returns a set of resources for the request
        @raises Exception in case of error
        """
        requested = reservation.get_requested_resources()
        rtype = requested.get_type()
        rc = self.get_control_by_type(rtype=rtype)
        if rc is not None:
            try:
                ticketed_sliver = requested.get_sliver()
                node_id = ticketed_sliver.get_node_map()[1]
                self.logger.debug(f"node_id {node_id} serving reservation# {reservation}")
                if node_id is None:
                    raise AuthorityException(f"Unable to find node_id {node_id} for reservation# {reservation}")

                graph_node = None
                if isinstance(ticketed_sliver, NodeSliver):
                    graph_node = self.get_network_node_from_graph(node_id=node_id)

                elif isinstance(ticketed_sliver, NetworkServiceSliver):
                    graph_node = self.get_network_service_from_graph(node_id=node_id)

                else:
                    msg = f'Reservation {reservation} sliver type is neither Node, nor NetworkServiceSliver'
                    self.logger.error(msg)
                    raise AuthorityException(msg)

                self.logger.debug(f"Node {graph_node} serving reservation# {reservation}")

                existing_reservations = self.get_existing_reservations(node_id=node_id,
                                                                       node_id_to_reservations=node_id_to_reservations)

                delegation_name, broker_callback = self.get_delegation_name_and_callback(
                    delegation_id=requested.get_resources().get_delegation_id())

                rset = rc.assign(reservation=reservation, delegation_name=delegation_name,
                                 graph_node=graph_node, existing_reservations=existing_reservations)

                if rset is None or rset.get_sliver() is None or rset.get_sliver().get_node_map() is None:
                    raise AuthorityException(f"Could not assign resources to reservation# {reservation}")

                reservation.set_broker_callback(broker_callback=broker_callback)
                return rset
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.error(f"Could not assign {e}")
                return None
        else:
            raise AuthorityException(Constants.UNSUPPORTED_RESOURCE_TYPE.format(reservation.get_type()))

    def configuration_complete(self, *, action: str, token: ConfigToken, out_properties: dict):
        super().configuration_complete(action=action, token=token, out_properties=out_properties)
        rc = self.get_control_by_type(rtype=token.get_resource_type())
        if rc is not None:
            rc.configuration_complete(action=action, token=token, out_properties=out_properties)
        else:
            raise AuthorityException(Constants.UNSUPPORTED_RESOURCE_TYPE.format(token.get_resource_type()))

    @staticmethod
    def is_expired(*, reservation: ABCReservationMixin) -> bool:
        """
        See if a reservation has expired

        @params reservation: reservation
        @return true if the reservation expired; otherwise, return false
        @raises Exception in case of error
        """
        now = datetime.utcnow()
        end = reservation.get_term().get_end_time()

        return now > end

    def reschedule(self, *, reservation: ABCAuthorityReservation):
        """
        Reschedule a reservation into the calendar

        @param reservation the reservation
        """
        self.calendar.remove(reservation=reservation)
        self.calendar.add_request(reservation=reservation, cycle=self.actor.get_current_cycle() + 1)

    def get_close(self, *, term: Term) -> int:
        """
        Return the cycle when a term closes

        @params term: the term
        @returns the cycle of the end of a term
        """
        if self.lazy_close:
            return -1
        else:
            return self.clock.cycle(when=term.get_end_time()) + 1

    def get_closing(self, *, cycle: int) -> ReservationSet:
        return self.calendar.get_closing(cycle=cycle)

    def get_requests(self, *, cycle: int) -> ReservationSet:
        return self.calendar.get_requests(cycle=cycle)

    def get_control_by_id(self, *, guid: ID) -> ABCResourceControl:
        return self.controls_by_guid.get(guid, None)

    def get_control_by_type(self, *, rtype: ResourceType) -> ABCResourceControl:
        return self.controls_by_resource_type.get(rtype, None)

    def get_control_types(self):
        """
        Returns a reverse map of resource control to resource types. The table is
        indexed by the resource control object and each entry is a linked list of
        resource types.

        @returns a table of all of the different control types
        """
        result = {}
        for key, value in self.controls_by_resource_type.items():
            if value not in result:
                result[value] = []
            result[value].append(key)
        return result

    def register_control(self, *, control: ABCResourceControl):
        """
        Registers the given control for the specified resource type. If the
        policy plugin has already been initialized, the control should be
        initialized.

        @param control: the control
        @raises ConfigurationException in case of error
        """
        self.register_control_types(control=control)
        self.controls_by_guid[control.get_guid()] = control

    def register_control_types(self, *, control: ABCResourceControl):
        types = control.get_types()
        if types is None or len(types) == 0:
            raise AuthorityException("Resource control does not specify any types")
        for t in types:
            if t is None:
                raise AuthorityException("Invalid resource type specified")

        index = 0
        try:
            for rtype in types:
                if rtype in self.controls_by_resource_type:
                    raise AuthorityException(f"There is already a control associated with resource type {rtype}")
                self.controls_by_resource_type[rtype] = control
                index += 1
        except Exception as e:
            j = 0
            for t in types:
                if t in self.controls_by_resource_type:
                    self.controls_by_resource_type.pop(t)
                    j += 1
                if j == index:
                    break
            raise e

    def get_network_node_from_graph(self, *, node_id: str) -> NodeSliver or None:
        try:
            self.lock.acquire()
            if self.aggregate_resource_model is None:
                return None
            return self.aggregate_resource_model.build_deep_node_sliver(node_id=node_id)
        finally:
            self.lock.release()

    def get_network_service_from_graph(self, *, node_id: str) -> NetworkServiceSliver or None:
        try:
            self.lock.acquire()
            if self.aggregate_resource_model is None:
                return None
            return self.aggregate_resource_model.build_deep_ns_sliver(node_id=node_id)
        finally:
            self.lock.release()

    def get_existing_reservations(self, node_id: str, node_id_to_reservations: dict):
        existing_reservations = self.actor.get_plugin().get_database().get_reservations_by_graph_node_id(
            graph_node_id=node_id)

        reservations_allocated_in_cycle = node_id_to_reservations.get(node_id, None)

        if reservations_allocated_in_cycle is None:
            return existing_reservations

        if existing_reservations is None:
            return reservations_allocated_in_cycle.values()

        for e in existing_reservations.copy():
            if reservations_allocated_in_cycle.contains(rid=e.get_reservation_id()):
                existing_reservations.remove(e)

        for r in reservations_allocated_in_cycle.values():
            existing_reservations.append(r)

        return existing_reservations

    def get_delegation_name_and_callback(self, *, delegation_id: str) -> Tuple[str, ABCCallbackProxy]:
        try:
            self.lock.acquire()
            delegation = self.delegations.get(delegation_id, None)
            if delegation is not None and delegation.is_delegated():
                return delegation.get_delegation_name(), delegation.get_callback()
        finally:
            self.lock.release()


if __name__ == '__main__':
    policy = AuthorityCalendarPolicy()
