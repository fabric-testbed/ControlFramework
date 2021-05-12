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
from __future__ import annotations

import enum
import threading
import traceback
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Tuple, List

from fim.graph.abc_property_graph import ABCPropertyGraphConstants, GraphFormat
from fim.graph.resources.abc_adm import ABCADMPropertyGraph
from fim.pluggable import PluggableRegistry, PluggableType
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.network_node import NodeSliver, NodeType
from fim.slivers.network_link import NetworkLinkSliver

from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.delegation.resource_ticket import ResourceTicketFactory
from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.policy.broker_calendar_policy import BrokerCalendarPolicy
from fabric_cf.actor.core.policy.fifo_queue import FIFOQueue
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.bids import Bids
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.core.policy.inventory import Inventory
from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
from fabric_cf.actor.fim.fim_helper import FimHelper
from fabric_cf.actor.fim.plugins.broker.aggregate_bqm_plugin import AggregatedBQMPlugin

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_broker_mixin import ABCBrokerMixin
    from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
    from fabric_cf.actor.core.util.resource_type import ResourceType
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet


class BrokerAllocationAlgorithm(Enum):
    FirstFit = enum.auto()
    BestFit = enum.auto()
    WorstFit = enum.auto()

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


class BrokerSimplerUnitsPolicy(BrokerCalendarPolicy):
    """
        BrokerSimplerUnitsPolicy is a simple implementation of the broker policy interface.
        It buffers requests for allocation periods and when it performs allocations of resources it does so
        in FIFO order giving preference to extending requests.
    """
    # The amount of time over specific policy decisions the broker
    # must add when communicating with other brokers as a client (e.g.
    # renew()). Clock skew must be at least one if the broker is ticked after
    # the upstream broker(s). At some point in time we may want this to not
    # be static and learn it from what we see in the system, but for now it
    # is static.
    CLOCK_SKEW = 1

    # Number of cycles between two consecutive allocations.
    CALL_INTERVAL = 1

    #  How far in the future is the broker allocating resources
    ADVANCE_TIME = 3

    def __init__(self, *, actor: ABCBrokerMixin = None):
        super().__init__(actor=actor)
        self.last_allocation = -1
        self.allocation_horizon = 0
        self.ready = False

        self.delegations = {}
        self.combined_broker_model = None
        self.combined_broker_model_graph_id = None

        self.queue = FIFOQueue()
        self.inventory = Inventory()

        self.pluggable_registry = PluggableRegistry()
        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']

        del state['delegations']
        del state['combined_broker_model']
        del state['lock']

        del state['calendar']

        del state['last_allocation']
        del state['allocation_horizon']
        del state['ready']
        del state['queue']
        del state['pluggable_registry']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.actor = None
        self.clock = None
        self.initialized = False

        self.delegations = {}
        self.combined_broker_model = None

        self.lock = threading.Lock()
        self.calendar = None

        self.last_allocation = -1
        self.allocation_horizon = 0
        self.ready = False

        self.queue = None
        self.pluggable_registry = PluggableRegistry()

    def load_combined_broker_model(self):
        if self.combined_broker_model_graph_id is None:
            self.logger.debug("Creating an empty Combined Broker Model Graph")
        else:
            self.logger.debug(f"Loading an existing Combined Broker Model Graph: {self.combined_broker_model_graph_id}")

        self.combined_broker_model = FimHelper.get_neo4j_cbm_graph(graph_id=self.combined_broker_model_graph_id)
        self.combined_broker_model_graph_id = self.combined_broker_model.get_graph_id()
        self.logger.debug(f"Successfully loaded an Combined Broker Model Graph: {self.combined_broker_model_graph_id}")
        self.pluggable_registry.register_pluggable(t=PluggableType.Broker, p=AggregatedBQMPlugin, actor=self.actor,
                                                   logger=self.logger)
        self.logger.debug(f"Registered AggregateBQMPlugin")

    def initialize(self):
        if not self.initialized:
            super().initialize()
            self.load_combined_broker_model()
            self.initialized = True

    def register_inventory(self, *, resource_type: ResourceType, inventory: InventoryForType):
        """
        Registers the given inventory for the specified resource type. If the
        policy plugin has already been initialized, the inventory should be
        initialized.

        @param inventory: the inventory
        @param resource_type: resource_type
        """
        self.inventory.add_inventory_by_type(rtype=resource_type, inventory=inventory)

    def bind(self, *, reservation: ABCBrokerReservation) -> bool:
        term = reservation.get_requested_term()
        self.logger.info("SlottedAgent bind arrived at cycle {} requested term {}".format(
            self.actor.get_current_cycle(), term))

        bid_cycle = self.get_allocation(reservation=reservation)

        self.calendar.add_request(reservation=reservation, cycle=bid_cycle)

        return False

    def bind_delegation(self, *, delegation: ABCDelegation) -> bool:
        try:
            self.lock.acquire()
            self.delegations[delegation.get_delegation_id()] = delegation
        finally:
            self.lock.release()

        return False

    def extend_broker(self, *, reservation: ABCBrokerReservation) -> bool:
        requested_term = reservation.get_requested_term()
        self.logger.info("SlottedAgent extend arrived at cycle {} requested term {}".format(
            self.actor.get_current_cycle(), requested_term))

        source = reservation.get_source()

        if source is None:
            self.error(message="cannot find parent ticket for extend")

        if source.is_failed():
            self.error(message="parent ticket could not be renewed")

        bid_cycle = self.get_allocation(reservation=reservation)

        self.calendar.add_request(reservation=reservation, cycle=bid_cycle, source=source)
        self.calendar.add_request(reservation=reservation, cycle=bid_cycle)

        return False

    def formulate_bids(self, *, cycle: int) -> Bids:
        pending = self.calendar.get_pending()
        renewing = self.calendar.get_renewing(cycle=cycle)
        extending = self.process_renewing(renewing=renewing, pending=pending)
        return Bids(ticketing=ReservationSet(), extending=extending)

    def get_allocation(self, *, reservation: ABCBrokerReservation) -> int:
        if not self.ready:
            self.error(message="Agent not ready to accept bids")

        start = self.clock.cycle(when=reservation.get_requested_term().get_new_start_time())

        start -= self.ADVANCE_TIME

        intervals = int((start - self.last_allocation)/self.CALL_INTERVAL)

        if intervals <= 0:
            intervals = 1

        start = self.last_allocation + (intervals * self.CALL_INTERVAL) + self.ADVANCE_TIME

        return start

    def get_approved_term(self, *, reservation: ABCBrokerReservation) -> Term:
        return Term(start=reservation.get_requested_term().get_start_time(),
                    end=reservation.get_requested_term().get_end_time(),
                    new_start=reservation.get_requested_term().get_new_start_time())

    def get_next_allocation(self, *, cycle: int) -> int:
        return self.last_allocation + self.CALL_INTERVAL

    def get_renew(self, *, reservation: ABCClientReservation) -> int:
        new_start_cycle = self.actor.get_actor_clock().cycle(when=reservation.get_term().get_end_time()) + 1
        return new_start_cycle - self.ADVANCE_TIME - self.CLOCK_SKEW

    def get_start_for_allocation(self, *, allocation_cycle: int) -> int:
        return allocation_cycle + self.ADVANCE_TIME

    def get_end_for_allocation(self, *, allocation_cycle: int) -> int:
        return allocation_cycle + self.ADVANCE_TIME + self.allocation_horizon

    def prepare(self, *, cycle: int):
        if not self.ready:
            self.last_allocation = cycle - self.CALL_INTERVAL
            self.ready = True

        try:
            self.check_pending()
        except Exception as e:
            self.logger.error("Exception in prepare {}".format(e))

    def process_renewing(self, *, renewing: ReservationSet, pending: ReservationSet) -> ReservationSet:
        """
        Performs checks on renewing reservations. Updates the terms to
        suggest new terms, stores the extend on the pending list. Returns a
        fresh ReservationSet of expiring reservations to try to renew in this
        bidding cycle.

        @param renewing collection of the renewing reservations
        @param pending collection of reservations that are pending

        @return non-null set of renewals
        """
        result = ReservationSet()
        if renewing is None:
            return None

        #self.logger.debug("Expiring = {}".format(renewing.size()))

        for reservation in renewing.values():
            self.logger.debug("Expiring res: {}".format(reservation))

            if reservation.is_renewable():
                self.logger.debug("This is a renewable expiring reservation")

                term = reservation.get_term()

                term = term.extend()

                reservation.set_approved(term=term, approved_resources=reservation.get_resources().abstract_clone())

                result.add(reservation=reservation)
                self.calendar.add_pending(reservation=reservation)
            else:
                self.logger.debug("This is not a renewable expiring reservation")

        return result

    def allocate(self, *, cycle: int):
        if self.get_next_allocation(cycle=cycle) != cycle:
            return

        self.last_allocation = cycle

        start_cycle = self.get_start_for_allocation(allocation_cycle=cycle)
        advance_cycle = self.get_end_for_allocation(allocation_cycle=cycle)
        requests = self.calendar.get_all_requests(cycle=advance_cycle)

        if (requests is None or requests.size() == 0) and (self.queue is None or self.queue.size() == 0):
            self.logger.debug(f"requests: {requests} queue: {self.queue}")
            self.logger.debug(f"no requests for auction start cycle {start_cycle}")
            return

        self.logger.debug(f"allocating resources for cycle {start_cycle}")

        self.allocate_extending_reservation_set(requests=requests, start_cycle=start_cycle)
        self.allocate_queue(start_cycle=start_cycle)
        self.allocate_ticketing(requests=requests, start_cycle=start_cycle)

    def get_default_resource_type(self) -> ResourceType:
        result = None

        if len(self.inventory.get_inventory().keys()) > 0:
            result = self.inventory.get_inventory().keys().__iter__().__next__()

        return result

    def allocate_extending_reservation_set(self, *, requests: ReservationSet, start_cycle: int):
        if requests is not None:
            for reservation in requests.values():
                if reservation.is_extending_ticket() and not reservation.is_closed():
                    start = reservation.get_requested_term().get_new_start_time()
                    end = self.align_end(when=reservation.get_requested_term().get_end_time())

                    resource_type = reservation.get_resources().get_type()

                    inv = self.inventory.get(resource_type=resource_type)

                    if inv is not None:
                        ext_term = Term(start=reservation.get_term().get_start_time(), end=end, new_start=start)
                        self.extend_private(reservation=reservation, inv=inv, term=ext_term)
                    else:
                        reservation.fail(message=Constants.NO_POOL)

    def allocate_ticketing(self, *, requests: ReservationSet, start_cycle: int):
        if requests is not None:
            # Holds the Node Id to List of Reservation Ids allocated
            # This is used to check on the reservations allocated during this cycle to compute available resources
            # as the reservations are not updated in the database yet
            node_id_to_reservations = {}
            for reservation in requests.values():
                if not reservation.is_ticketing():
                    continue

                status, node_id_to_reservations = self.ticket(reservation=reservation,
                                                              node_id_to_reservations=node_id_to_reservations)
                if status:
                    continue

                if self.queue is None and not reservation.is_failed():
                    reservation.fail(message="Insufficient resources")
                    continue

                if not reservation.is_failed():
                    reservation.fail(message=f"Insufficient resources for specified start time, Failing reservation: "
                                             f"{reservation.get_reservation_id()}")

    def allocate_queue(self, *, start_cycle: int):
        if self.queue is None:
            return

        # Holds the Node Id to List of Reservation Ids allocated
        # This is used to check on the reservations allocated during this cycle to compute available resources
        # as the reservations are not updated in the database yet
        node_id_to_reservations = {}
        for reservation in self.queue.values():
            status, node_id_to_reservations = self.ticket(reservation=reservation,
                                                          node_id_to_reservations=node_id_to_reservations)
            if not status:
                # TODO
                threshold = 100
                start = self.clock.cycle(when=reservation.get_requested_term().get_new_start_time())

                if threshold != 0 and ((start_cycle - start) > threshold):
                    reservation.fail_warn(message=f"Request has exceeded its threshold on the queue {reservation}")
                    self.queue.remove(reservation=reservation)
            else:
                self.queue.remove(reservation=reservation)

    def ticket(self, *, reservation: ABCBrokerReservation, node_id_to_reservations: dict) -> Tuple[bool, dict]:
        self.logger.debug(f"cycle: {self.actor.get_current_cycle()} new ticket request: "
                          f"{reservation}/{type(reservation).__name__}")

        start = self.align_start(when=reservation.get_requested_term().get_new_start_time())
        end = self.align_end(when=reservation.get_requested_term().get_end_time())

        term = None
        resource_type = reservation.get_requested_resources().get_type()
        if resource_type is None or not self.inventory.contains_type(resource_type=resource_type):
            resource_type = self.get_default_resource_type()

        if resource_type is not None:
            inv = self.inventory.get(resource_type=resource_type)

            if inv is not None:
                if term is None:
                    term = Term(start=start, end=end)

                return self.ticket_inventory(reservation=reservation, inv=inv, term=term,
                                             node_id_to_reservations=node_id_to_reservations)
            else:
                reservation.fail(message=Constants.NO_POOL)
        else:
            reservation.fail(message=Constants.NO_POOL)

        return False, node_id_to_reservations

    def __candidate_nodes(self, sliver: NodeSliver) -> List[str]:
        """
        Identify candidate worker nodes in this site that have at least
        as many needed components as in the sliver.
        """
        node_props = {ABCPropertyGraphConstants.PROP_SITE: sliver.site,
                      ABCPropertyGraphConstants.PROP_TYPE: str(NodeType.Server)}
        return self.combined_broker_model.get_matching_nodes_with_components(
            label=ABCPropertyGraphConstants.CLASS_NetworkNode,
            props=node_props,
            comps=sliver.attached_components_info)

    def __candidate_links(self, sliver: NetworkLinkSliver) -> List[str]:
        return list()

    def ticket_inventory(self, *, reservation: ABCBrokerReservation, inv: InventoryForType, term: Term,
                         node_id_to_reservations: dict) -> Tuple[bool, dict]:
        try:
            rset = reservation.get_requested_resources()
            needed = rset.get_units()

            # for network node slivers
            # find a list of candidate worker nodes that satisfy the requirements based on delegated
            # capacities within the site
            # for network link slivers
            # orchestrator needs to provide a map to CBM guid of the node representing the
            # intended link (and possibly interfaces connected to it)

            res_sliver = rset.get_sliver()

            if isinstance(res_sliver, NodeSliver):
                node_id_list = self.__candidate_nodes(res_sliver)
            elif isinstance(res_sliver, NetworkLinkSliver):
                node_id_list = self.__candidate_links(res_sliver)
            else:
                self.logger.error(f'Reservation {reservation} sliver type is neither Node, nor NetworkLink')
                raise BrokerException(msg=f"Reservation sliver type is neither Node "
                                      f"nor NetworkLink for reservation# {reservation}")

            # no candidate nodes found
            if len(node_id_list) == 0:
                self.logger.error(f'No candidates nodes found to serve {reservation}')
                return False, node_id_to_reservations

            delegation_id = None
            sliver = None
            if self.get_algorithm_type().lower() == BrokerAllocationAlgorithm.FirstFit.name.lower():
                delegation_id, sliver = self.__find_first_fit(node_id_list=node_id_list,
                                                              node_id_to_reservations=node_id_to_reservations,
                                                              inv=inv, reservation=reservation)
            else:
                raise BrokerException(error_code=ExceptionErrorCode.NOT_SUPPORTED,
                                      msg=f"Broker currently only supports First Fit")

            if delegation_id is not None:
                delegation = self.actor.get_delegation(did=delegation_id)
                reservation = self.issue_ticket(reservation=reservation, units=needed, rtype=rset.get_type(), term=term,
                                                source=delegation, sliver=sliver)

                node_map = sliver.get_node_map()
                node_id = node_map[1]
                if node_id_to_reservations.get(node_id, None) is None:
                    node_id_to_reservations[node_id] = ReservationSet()
                node_id_to_reservations[node_id].add(reservation=reservation)
                return True, node_id_to_reservations
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(e)
            reservation.fail(message=str(e))
        return False, node_id_to_reservations

    def __find_first_fit(self, node_id_list: List[str], node_id_to_reservations: dict, inv: InventoryForType,
                         reservation: ABCBrokerReservation) -> Tuple[str, BaseSliver]:
        delegation_id = None
        sliver = None
        self.logger.debug(f"Possible candidates to serve res# {reservation} candidates# {node_id_list}")
        for node_id in node_id_list:
            try:
                self.logger.debug(f"Attempting to allocate res# {reservation} via graph_node# {node_id}")
                graph_node = self.get_node_from_graph(node_id=node_id)

                existing_reservations = self.get_existing_reservations(node_id=node_id,
                                                                       node_id_to_reservations=node_id_to_reservations)

                delegation_id, sliver = inv.allocate(reservation=reservation,
                                                     graph_id=self.combined_broker_model_graph_id,
                                                     graph_node=graph_node, existing_reservations=existing_reservations)

                if delegation_id is not None and sliver is not None:
                    break
            except BrokerException as e:
                if e.error_code == ExceptionErrorCode.INSUFFICIENT_RESOURCES:
                    self.logger.error(f"Exception occurred: {e}")
                else:
                    raise e

        return delegation_id, sliver

    def extend_private(self, *, reservation: ABCBrokerReservation, inv: InventoryForType, term: Term):
        try:
            self.logger.debug(f"Extend private initiated for {reservation}")
            requested_resources = reservation.get_requested_resources()
            needed = requested_resources.get_units()
            current = reservation.get_resources().get_units()
            if needed == current:
                self.issue_ticket(reservation=reservation, units=needed, rtype=requested_resources.get_type(),
                                  term=term, source=reservation.get_source(), sliver=reservation.get_resources().sliver)
            else:
                self.logger.error(f"Failed to satisfy the extend for reservation# {reservation}")
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            reservation.fail(exception=e)

    def issue_ticket(self, *, reservation: ABCBrokerReservation, units: int, rtype: ResourceType,
                     term: Term, source: ABCDelegation, sliver: BaseSliver) -> ABCBrokerReservation:

        # make the new delegation
        resource_delegation = ResourceTicketFactory.create(issuer=self.actor.get_identity().get_guid(), units=units,
                                                           term=term, rtype=rtype)

        # extract a new resource set
        mine = self.extract(source=source, delegation=resource_delegation)

        # attach the current request properties so that we can look at them in the future
        mine.set_sliver(sliver=sliver)

        if mine is not None and not reservation.is_failed():
            reservation.set_approved(term=term, approved_resources=mine)
            reservation.set_source(source=source)
            self.logger.debug(f"allocated: {mine.get_units()} for term: {term}")
            self.logger.debug(f"resourceshare= {units} mine= {mine.get_units()}")

            self.add_to_calendar(reservation=reservation)
            reservation.set_bid_pending(value=False)
        else:
            if mine is None:
                raise BrokerException(msg="There was an error extracting a ticket from the source delegation")
        return reservation

    def release(self, *, reservation):
        if isinstance(reservation, ABCBrokerReservation):
            self.logger.debug("Broker reservation")
            super().release(reservation=reservation)
            if reservation.is_closed_in_priming():
                self.logger.debug("Releasing resources (closed in priming)")
                self.release_resources(rid=str(reservation.get_reservation_id()),
                                       slice_id=str(reservation.get_slice_id()),
                                       rset=reservation.get_approved_resources(),
                                       term=reservation.get_approved_term())
            else:
                self.logger.debug("Releasing resources")
                self.release_resources(rid=str(reservation.get_reservation_id()),
                                       slice_id=str(reservation.get_slice_id()),
                                       rset=reservation.get_resources(),
                                       term=reservation.get_term())
        elif isinstance(reservation, ABCClientReservation):
            self.logger.debug("Client reservation")
            super().release(reservation=reservation)
            status = self.inventory.remove(source=reservation)
            self.logger.debug(f"Removing reservation: {reservation.get_reservation_id()} "
                              f"from inventory status: {status}")

    def release_resources(self, *, rid: str, slice_id: str, rset: ResourceSet, term: Term):
        try:
            if rset is None or term is None or rset.get_resources() is None:
                self.logger.warning("Reservation does not have resources to release")
                return
            inv = self.inventory.get(resource_type=rset.get_type())
            if inv is None:
                raise BrokerException(msg="Cannot release resources: missing inventory")
        except Exception as e:
            self.logger.error(f"release resources {e}")

    def align_end(self, *, when: datetime) -> datetime:
        """
        Aligns the specified date with the end of the closest cycle.

        @param when when to align

        @return date aligned with the end of the closes cycle
        """
        cycle = self.clock.cycle(when=when)
        time = self.clock.cycle_end_in_millis(cycle=cycle)
        return ActorClock.from_milliseconds(milli_seconds=time)

    def align_start(self, *, when: datetime) -> datetime:
        """
        Aligns the specified date with the start of the closest cycle.

        @param when when to align

        @return date aligned with the start of the closes cycle
        """
        cycle = self.clock.cycle(when=when)
        time = self.clock.cycle_start_in_millis(cycle=cycle)
        return ActorClock.from_milliseconds(milli_seconds=time)

    def query(self, *, p: dict) -> dict:
        """
        Returns the Broker Query Model
        @params p : dictionary containing filters (not used currently)
        """
        result = {}
        self.logger.debug("Processing Query with properties: {}".format(p))

        query_action = self.get_query_action(properties=p)
        query_level = p.get(Constants.QUERY_DETAIL_LEVEL, None)
        if query_level is not None:
            query_level = int(query_level)

        if query_action is None:
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT,
                                  msg=f"query_action {query_action}")

        if query_action != Constants.QUERY_ACTION_DISCOVER_BQM:
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT,
                                  msg=f"query_action {query_action}")

        bqm_format = p.get(Constants.BROKER_QUERY_MODEL_FORMAT, None)
        if bqm_format is not None:
            bqm_format = GraphFormat(int(bqm_format))
        else:
            bqm_format = GraphFormat.GRAPHML

        try:
            self.lock.acquire()
            if self.combined_broker_model is not None:
                graph = self.combined_broker_model.get_bqm(query_level=query_level)
                graph_string = None
                if graph is not None:
                    graph_string = graph.serialize_graph(format=bqm_format)
                if graph_string is not None:
                    result[Constants.BROKER_QUERY_MODEL] = graph_string
                    result[Constants.QUERY_RESPONSE_STATUS] = "True"
                else:
                    result[Constants.BROKER_QUERY_MODEL] = ""
                    result[Constants.QUERY_RESPONSE_STATUS] = "False"
                    result[Constants.QUERY_RESPONSE_MESSAGE] = "Resource(s) not found"
                graph.delete_graph()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(e)
            result[Constants.BROKER_QUERY_MODEL] = ""
            result[Constants.QUERY_RESPONSE_STATUS] = "False"
            result[Constants.QUERY_RESPONSE_MESSAGE] = str(e)
        finally:
            self.lock.release()

        self.logger.debug("Returning Query Result: {}".format(result))
        return result

    def donate_delegation(self, *, delegation: ABCDelegation):
        """
        Donate an incoming delegation by merging to CBM;
        We take snapshot of CBM before merge, and rollback to snapshot in case merge fails
        :param delegation:
        :return:
        :raises: Exception in case of failure
        """
        self.logger.debug("Donate Delegation")
        self.bind_delegation(delegation=delegation)
        try:
            self.lock.acquire()
            if delegation.get_delegation_id() in self.delegations:
                self.merge_adm(adm_graph=delegation.get_graph())
                self.logger.debug(f"Donated Delegation: {delegation.get_delegation_id()}")
            else:
                self.logger.warning("Delegation ignored")
        except Exception as e:
            self.logger.error(f"Failed to merge ADM: {delegation}")
            self.logger.error(traceback.format_exc())
            raise e
        finally:
            self.lock.release()

    def closed_delegation(self, *, delegation: ABCDelegation):
        """
        Close a delegation by un-merging from CBM
        We take snapshot of CBM before un-merge, and rollback to snapshot in case un-merge fails
        :param delegation:
        :return:
        """
        self.logger.debug("Close Delegation")
        try:
            self.lock.acquire()
            if delegation.get_delegation_id() in self.delegations:
                self.unmerge_adm(graph_id=delegation.get_delegation_id())
                self.delegations.pop(delegation.get_delegation_id())
                self.logger.debug(f"Closed Delegation: {delegation.get_delegation_id()}")
            else:
                self.logger.warning("Delegation ignored")
        except Exception as e:
            self.logger.error(f"Failed to un-merge ADM: {delegation}")
            self.logger.error(traceback.format_exc())
            raise e
        finally:
            self.lock.release()

    def get_node_from_graph(self, *, node_id: str) -> NodeSliver:
        """
        Get Node from CBM
        :param node_id:
        :return:
        """
        try:
            self.lock.acquire()
            if self.combined_broker_model is None:
                return None
            return self.combined_broker_model.build_deep_node_sliver(node_id=node_id)
        finally:
            self.lock.release()

    def get_existing_reservations(self, node_id: str, node_id_to_reservations: dict) -> List[ABCReservationMixin]:
        """
        Get existing reservations which are served by CBM node identified by node_id
        :param node_id:
        :param node_id_to_reservations:
        :return: list of reservations
        """
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

    def set_logger(self, logger):
        """
        Set logger
        :param logger:
        :return:
        """
        super().set_logger(logger=logger)
        if self.inventory is not None:
            for inv in self.inventory.map.values():
                inv.set_logger(logger=logger)

    def merge_adm(self, *, adm_graph: ABCADMPropertyGraph):
        """
        Merge delegation model in CBM
        :param adm_graph: ADM
        :return:
        """
        snapshot_graph_id = None
        try:
            if self.combined_broker_model.graph_exists():
                snapshot_graph_id = self.combined_broker_model.snapshot()
            self.combined_broker_model.merge_adm(adm=adm_graph)
            self.combined_broker_model.validate_graph()
            # delete the snapshot
            self.combined_broker_model.importer.delete_graph(graph_id=snapshot_graph_id)
        except Exception as e:
            self.logger.error(f"Exception occurred: {e}")
            self.logger.error(traceback.format_exc())
            if snapshot_graph_id is not None:
                self.logger.info(f"CBM rollback due to merge failure")
                self.combined_broker_model.rollback(graph_id=snapshot_graph_id)
            raise e

    def unmerge_adm(self, *, graph_id: str):
        """
        Unmerge delegation model from CBM
        :param graph_id:
        :return:
        """
        snapshot_graph_id = None
        try:
            if self.combined_broker_model.graph_exists():
                snapshot_graph_id = self.combined_broker_model.snapshot()
            self.combined_broker_model.unmerge_adm(graph_id=graph_id)
            self.combined_broker_model.validate_graph()
        except Exception as e:
            self.logger.error(f"Exception occurred: {e}")
            self.logger.error(traceback.format_exc())
            if snapshot_graph_id is not None:
                self.logger.info(f"CBM rollback due to un-merge failure")
                self.combined_broker_model.rollback(graph_id=snapshot_graph_id)
            raise e

    def get_algorithm_type(self) -> str:
        if self.properties is not None:
            algo_str = self.properties.get(Constants.ALGORITHM, None)
            if algo_str is not None:
                return algo_str
        return BrokerAllocationAlgorithm.FirstFit.name


if __name__ == '__main__':
    policy = BrokerSimplerUnitsPolicy()
