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

import threading
from typing import TYPE_CHECKING

from fabric.actor.boot.inventory.neo4j_resource_pool_factory import Neo4jResourcePoolFactory
from fabric.actor.core.apis.i_delegation import IDelegation
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.policy.broker_calendar_policy import BrokerCalendarPolicy
from fabric.actor.core.time.actor_clock import ActorClock
from fabric.actor.core.util.bids import Bids
from fabric.actor.core.util.prop_list import PropList
from fabric.actor.core.util.reservation_set import ReservationSet

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
    from fabric.actor.core.apis.i_client_reservation import IClientReservation
    from fabric.actor.core.apis.i_broker import IBroker
    from fabric.actor.core.time.term import Term


class BrokerSimplePolicy(BrokerCalendarPolicy):
    """
    BrokerSimplePolicy is a simple implementation of the broker policy interface. It buffers requests for allocation
    periods and when it performs allocations of resources it does so in FIFO order giving preference to extending
    requests.
    """
    REQUEST_TYPE = "requestType"
    PropertyAllocationHorizon = "allocation.horizon"

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

    def __init__(self, *, actor: IBroker):
        super().__init__(actor=actor)
        self.last_allocation = -1
        self.allocation_horizon = 0
        self.ready = False

        self.delegations = {}
        self.combined_broker_model = None
        self.combined_broker_model_graph_id = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']

        del state['delegations']
        del state['combined_broker_model']
        del state['for_approval']
        del state['lock']

        del state['calendar']

        del state['last_allocation']
        del state['allocation_horizon']
        del state['ready']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.actor = None
        self.clock = None
        self.initialized = False

        self.delegations = {}
        self.combined_broker_model = None
        self.load_combined_broker_model()
        self.lock = threading.Lock()
        self.calendar = None

        if self.required_approval:
            self.for_approval = ReservationSet()

        self.last_allocation = -1
        self.allocation_horizon = 0
        self.ready = False

    def load_combined_broker_model(self):
        if self.combined_broker_model_graph_id is None:
            self.logger.debug("Creating an empty Combined Broker Model Graph")

            self.combined_broker_model = Neo4jResourcePoolFactory.get_neo4j_cbm_empty_graph()
            self.combined_broker_model_graph_id = self.combined_broker_model.get_graph_id()

            self.logger.debug("Empty Combined Broker Model Graph created: {}".format(
                self.combined_broker_model_graph_id))
        else:
            self.logger.debug("Loading an existing Combined Broker Model Graph: {}".format(
                self.combined_broker_model_graph_id))

            self.combined_broker_model = Neo4jResourcePoolFactory.get_neo4j_cbm_graph_from_database(
                combined_broker_model_graph_id=self.combined_broker_model_graph_id)
            self.logger.debug(
                "Successfully loaded an existing Combined Broker Model Graph: {}".format(
                    self.combined_broker_model_graph_id))

    def initialize(self):
        if not self.initialized:
            super().initialize()
            self.load_combined_broker_model()
            self.initialized = True

    def configure(self, *, properties: dict):
        if self.PropertyAllocationHorizon in properties:
            self.allocation_horizon = int(properties[self.PropertyAllocationHorizon])
        else:
            self.logger.warning("Invalid property value key: {}".format(self.PropertyAllocationHorizon))

    def create_source_dict(self, *, sources: list) -> dict:
        result = {}
        for s in sources:
            rtype = s.get_type()
            if rtype in sources:
                self.logger.debug("sourcehash already contains key - only allow one source per type")
            else:
                result[rtype] = s
        return result

    def switch_source(self, *, sources: list, start_time: int, source_hash: dict):
        for s in sources:
            extending_for_source = self.calendar.get_request(source=s, cycle=start_time)
            for r in extending_for_source.values():
                if r.get_requested_type() != s.get_type():
                    # this reservation has changed its type
                    if r.get_requested_type() in source_hash:
                        other_source = source_hash[r.get_requested_type()]
                        self.calendar.remove_request(reservation=r, source=s)
                        self.calendar.add_request(reservation=r, cycle=start_time, source=other_source)
                    else:
                        r.fail_warn("This agent has no resources to satisfy a request for type: {}".format(
                            r.get_requested_type()))

    def allocate_extending(self, *, sources: list, start_time: int):
        """
        Iterates through all of the sources and allocates resources to
        their extending bids based on FIFO. Only allocates requests with types
        specified in requestTypes or all requests if requestTypes is None

        @param sources sources for this allocation
        @param start_time when the allocation begins

        @throws Exception in case of error
        """
        for s in sources:
            extending_for_source = self.calendar.get_request(source=s, cycle=start_time)
            if extending_for_source is None or extending_for_source.size() == 0:
                self.logger.debug("There are no extends for source: {}".format(s))
            else:
                self.logger.debug("There are {} extend reservations".format(extending_for_source.size()))

                for extend in extending_for_source.values():
                    self.logger.info("Extend res {}".format(extend))

                    wanted = extend.get_requested_units()
                    self.logger.debug("ASPPlugin:allocateBids: allocating source rid({}) to extendDynamicRes({}) "
                                      "that has props:{}".format(s.get_reservation_id(), extend.get_reservation_id(),
                                                                 extend.get_requested_resources().
                                                                 get_request_properties()))
                    satisfy = self.verify_wanted_resources(reservation=extend, source=s, start_time=start_time,
                                                           wanted=wanted)

                    if satisfy:
                        self.satisfy_allocation(reservation=extend, source=s, resource_share=wanted,
                                                start_res_time=start_time)
                    extending_for_source.remove(extend)

    def allocate_new_bids(self, *, new_bids: list, source_hash: dict, start_time: int):
        """
        Iterates through all of the new bids and allocates resources
        based on FIFO. Only allocates requests with types specified in
        requestTypes or all requests if requestTypes
        is null. Requests are associated with their appropriate source.

        @param new_bids new bids to be allocated
        @param source_hash the sources for this allocation
        @param start_time when the allocation begins

        @throws Exception in case of error
        """
        for reservation in new_bids:
            if not reservation.is_extending_ticket():
                if reservation.get_requested_type() in source_hash:
                    source = source_hash[reservation.get_requested_type()]
                    reservation.set_source(source)
                    wanted = reservation.get_requested_units()
                    satisfy = self.verify_wanted_resources(reservation=reservation, source=source,
                                                           start_time=start_time,
                                                           wanted=wanted)
                    if satisfy:
                        self.satisfy_allocation(reservation=reservation, source=source, resource_share=wanted,
                                                start_res_time=start_time)
                else:
                    reservation.fail("There are no sources of type {}".format(reservation.get_requested_type()))

            new_bids.remove(reservation)

    def allocate(self, *, cycle: int):
        """
        Runs an allocation of requests against all sources, given a set
        of pending bids by using FCFS giving extends priority. This simple
        policy runs through the bids in iterator order, and gives every request
        min(requested,available) until we run out. It gives
        priority to extending requests compared to new requests.<p>The
        implementation assumes that there is only 1 source reservation per
        resource type. It also makes the assumption that we allocate resources
        sequentially in time, so there are never less resources available in
        the future.
        @param cycle cycle
        """
        # This method should first decide whether to run an allocation on the
        # current cycle. If the answer is yes, then it has to decide on the
        # start time of reservations it is going to allocate.
        if self.get_next_allocation(cycle=cycle) != cycle:
            self.logger.debug("Next allocation cycle {} != {}".format(self.get_next_allocation(cycle=cycle), cycle))
            return

        self.last_allocation = cycle
        # Determine the cycle for which the agent is allocating resources
        start_time = self.get_start_for_allocation(allocation_cycle=cycle)

        all_bids = self.calendar.get_requests(cycle=start_time)

        # If there are no extending and no new requests - return
        if all_bids is None or all_bids.size() == 0:
            self.logger.debug("No requests for allocation start cycle {}".format(start_time))
            return

        self.logger.debug("There are {} bids for cycle {}".format(all_bids.size(), start_time))

        # Put source on a Hashtable for the new request allocation
        holdings = self.calendar.get_holdings(self.clock.cycle(cycle=start_time))
        sources = holdings.values()
        source_hash = self.create_source_dict(sources=sources)

        self.switch_source(sources=sources, start_time=start_time, source_hash=source_hash)

        self.allocate_extending(sources=sources, start_time=start_time)

        self.allocate_new_bids(new_bids=all_bids.values(), source_hash=source_hash, start_time=start_time)

    def bind(self, *, reservation: IBrokerReservation) -> bool:
        term = reservation.get_requested_term()
        self.logger.info("SlottedAgent bind arrived at cycle {} requested term {}".format(
            self.actor.get_current_cycle(), term))

        bid_cycle = self.get_allocation(reservation=reservation)

        self.calendar.add_request(reservation=reservation, cycle=bid_cycle)

        return False

    def bind_delegation(self, *, delegation: IDelegation) -> bool:
        try:
            self.lock.acquire()
            self.delegations[delegation.get_delegation_id()] = delegation
        finally:
            self.lock.release()

        return False

    def calculate_available(self, *, start_time: int, source: IClientReservation) -> int:
        """
        Calculates the available resources from the specified source for
        the specified start time.

        @param start_time start of proposed ticket
        @param source source whose resources are being counted

        @return number of available units
        """
        start_date = self.clock.cycle_start_date(cycle=start_time)

        outlays = self.calendar.get_outlays(source=source, time=start_date)
        c = self.count(rvset=outlays, when=start_date)
        active = c.count_active(resource_type=source.get_type())

        available = source.get_units(when=start_date) - active

        self.logger.debug("There are {} resources available - extend".format(available))
        self.logger.debug("There are {} resources active".format(active))
        return available

    def extend_broker(self, *, reservation: IBrokerReservation) -> bool:
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

    def extract_ticket(self, *, reservation: IBrokerReservation, source: IClientReservation,
                       approved: Term, resource_share: int):
        mine = None
        ticket = source.get_resources()
        try:
            if not source.get_term().contains(term=approved):
                reservation.fail(message="Source term does not contain requested term or reservation is not elastic in "
                                         "time: sourceterm={} resterm={}".format(source.get_term(), approved),
                                 exception=None)
            else:
                delegation = self.actor.get_plugin().get_ticket_factory().make_delegation(
                    units=resource_share, term=approved, rtype=ticket.get_type(),
                    holder=self.get_client_id(reservation=reservation))
                mine = self.extract(source=ticket, delegation=delegation)

                req_properties = reservation.get_requested_resources().get_request_properties()
                mine.set_request_properties(p=req_properties)
        except Exception as e:
            self.logger.error("Term not satisfied: Mapper extract failed: has: {} {}".format(
                ticket.get_concrete_units(), e))
            raise Exception("Term not satisfied: Mapper extract failed: has: {} {}".format(
                ticket.get_concrete_units(), e))

        if mine is not None and not reservation.is_failed():
            reservation.set_approved(term=approved, approved_resources=mine)
            reservation.set_source(source=source)

            self.logger.debug("allocated: {} for term {}".format(mine.get_units(), approved))
            self.logger.debug("resource_share {} mine {}".format(resource_share, mine.get_units()))

        return mine

    def formulate_bids(self, *, cycle: int) -> Bids:
        pending = self.calendar.get_pending()
        renewing = self.calendar.get_renewing(cycle=cycle)
        extending = self.process_renewing(renewing=renewing, pending=pending)
        return Bids(ticketing=ReservationSet(), extending=extending)

    def get_allocation(self, *, reservation: IBrokerReservation) -> int:
        if not self.ready:
            self.error(message="Agent not ready to accept bids")

        start = self.clock.cycle(when=reservation.get_requested_term().get_new_start_time())

        start -= self.ADVANCE_TIME

        intervals = int((start - self.last_allocation)/self.CALL_INTERVAL)

        if intervals <= 0:
            intervals = 1

        start = self.last_allocation + (intervals * self.CALL_INTERVAL) + self.ADVANCE_TIME

        return start

    def get_approved_term(self, *, reservation: IBrokerReservation) -> Term:
        return Term(start=reservation.get_requested_term().get_start_time(),
                    end=reservation.get_requested_term().get_end_time(),
                    new_start=reservation.get_requested_term().get_new_start_time())

    def get_next_allocation(self, *, cycle: int) -> int:
        return self.last_allocation + self.CALL_INTERVAL

    def get_renew(self, *, reservation: IClientReservation) -> int:
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
                self.logger.debug("This is a renewable expiring reservtion")

                term = reservation.get_term()

                term = term.extend()

                reservation.set_approved(term=term, approved_resources=reservation.get_resources().abstract_clone())

                result.add(reservation=reservation)
                self.calendar.add_pending(reservation=reservation)
            else:
                self.logger.debug("This is not a renewable expiring res")

        return result

    def query(self, *, p: dict) -> dict:
        """
        Returns the Broker Query Model
        @params p : dictionary containing filters (not used currently)
        """
        result = {}
        self.logger.debug("Processing Query with properties: {}".format(p))

        try:
            self.lock.acquire()
            if self.combined_broker_model is not None:
                graph = self.combined_broker_model.get_bqm(some=5)
                result[Constants.BrokerQueryModel] = graph.serialize_graph()
                graph.delete_graph()
        finally:
            self.lock.release()

        self.logger.debug("Returning Query Result: {}".format(result))
        return result

    def satisfy_allocation(self, *, reservation: IBrokerReservation, source: IClientReservation, resource_share: int,
                           start_res_time: int):
        """
        These are request upcalls from the mapper with the manager lock held.
        Each request is a "bid". Here we simply validate the bids and save them
        in various calendar structures, pending action by an allocation policy
        that runs periodically ("auction").

        Performs all of the checks and necessary functions to allocate a
        ticket to a reservation from a source. Performs time shifting of a
        request's term if allowed, extracts units from the source, and assigns
        reservation.

        @param reservation request being allocated resources
        @param source source request is being allocated from
        @param resource_share number of resources being allocated to the request
        @param start_res_time when the ticket for the request starts

        @throws Exception in case of error
        """
        mine = None
        approved = self.get_approved_term(reservation=reservation)

        # Shift the requested term to start at the start time
        if self.clock.cycle(when=approved.get_new_start_time()) != start_res_time:
            if PropList.is_elastic_time(rset=reservation.get_requested_resources()):
                self.logger.debug("Reservation {} was schedules to start at {} is being shifted to {}"
                                  .format(reservation, self.clock.cycle(when=approved.get_new_start_time()),
                                          start_res_time))
                approved = approved.shift(date=self.clock.cycle_start_date(cycle=start_res_time))
            else:
                reservation.fail(message="Reservation has a different start time and time shifting is not allowed",
                                 exception=None)

        # Make sure the term end is aligned on a cycle boundary. NOTE: this
        # should also be aligned on call interval boundary!

        # calculate the cycle
        cycle = self.clock.cycle(when=approved.get_end_time())

        end_millis = self.clock.cycle_end_in_millis(cycle)

        # get the date that represents the end of the cycle
        aligned_end = ActorClock.from_milliseconds(milli_seconds=end_millis)
        # update the term end
        approved.set_end_time(date=aligned_end)

        # Get the ticket with the correct resources
        mine = self.extract_ticket(reservation=reservation, source=source, approved=approved,
                                   resource_share=resource_share)

        # Add to the calendar
        self.add_to_calendar(reservation=reservation)

        # Whatever happened up there, this bid is no longer pending. It either
        # succeeded or is now marked failed. (Could/should assert.)
        reservation.set_bid_pending(value=False)

    def verify_wanted_resources(self, *, reservation: IBrokerReservation, source: IClientReservation, start_time: int,
                                wanted: int):
        """
        Ensures that there are sufficient resources to allocate the
        number of requested resources.

        @param reservation reservation being allocated
        @param source source proposed for the requesting reservation
        @param start_time start of the proposed ticket
        @param wanted number of resources being requested

        @return true if there are sufficient resources,
                false otherwise

        @throws Exception in case of error
        """
        # calculate the number of available units
        available = self.calculate_available(start_time=start_time, source=source)
        mywanted = wanted

        self.logger.debug("ASPPlugin:allocateBids: allocating source rid({}) to newDynamicRes({}) that has props:{}"
                          .format(source.get_reservation_id(),
                                  reservation.get_reservation_id(),
                                  reservation.get_requested_resources().get_request_properties()))

        # Check if we have sufficient resources. If the reservation is flexible
        # adjust its resource demand to what is available.
        if wanted > available:
            if PropList.is_elastic_size(rset=reservation.get_requested_resources()) and available > 0:
                self.logger.debug("broker is shrinking an elastic request")
                mywanted = available
        else:
            reservation.fail_warn(message="Selected source holding has insufficient resources: source {}"
                                          " available {}; request {}".format(source, available, reservation))

        # If the (possibly adjusted) demand fits in what we've got, then try to
        # allocate.
        if mywanted <= available:
            return True
        else:
            return False

    def donate_delegation(self, *, delegation: IDelegation):
        self.logger.debug("Donate Delegation")
        self.bind_delegation(delegation=delegation)
        try:
            self.lock.acquire()
            if delegation.get_delegation_id() in self.delegations:
                self.combined_broker_model.merge_adm(adm=delegation.get_graph())
                self.combined_broker_model.validate_graph()
                self.logger.debug("Donated Delegation: self.combined_broker_model: {}".format(
                    self.combined_broker_model.serialize_graph()))
            else:
                self.logger.debug("Delegation ignored")
        finally:
            self.lock.release()

    def closed_delegation(self, *, delegation: IDelegation):
        self.logger.debug("Close Delegation")
        # TODO remove the delegation from the combined broker model
