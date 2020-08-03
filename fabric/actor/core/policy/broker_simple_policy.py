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
from datetime import datetime
from typing import TYPE_CHECKING

from fabric.actor.core.policy.broker_calendar_policy import BrokerCalendarPolicy
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

    def __init__(self, actor: IBroker):
        super().__init__(actor)
        self.last_allocation = -1
        self.allocation_horizon = 0
        self.ready = False

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']

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

        self.lock = threading.Lock()
        self.calendar = None

        if self.required_approval:
            self.for_approval = ReservationSet()

        self.last_allocation = -1
        self.allocation_horizon = 0
        self.ready = False

        # TODO Fetch Actor object and setup logger, actor and clock member variables

    def configure(self, properties: dict):
        if self.PropertyAllocationHorizon in properties:
            self.allocation_horizon = int(properties[self.PropertyAllocationHorizon])
        else:
            self.logger.warning("Invalid property value key: {}".format(self.PropertyAllocationHorizon))

    def create_source_dict(self, sources: list) -> dict:
        result = {}
        for s in sources:
            rtype = s.get_type()
            if rtype in sources:
                self.logger.debug("sourcehash already contains key - only allow one source per type")
            else:
                result[rtype] = s
        return result

    def switch_source(self, sources: list, start_time: int, source_hash: dict):
        for s in sources:
            extending_for_source =  self.calendar.get_request(s, start_time)
            for r in extending_for_source.values():
                if r.get_requested_type() != s.get_type():
                    # this reservation has changed its type
                    if r.get_requested_type() in source_hash:
                        other_source = source_hash[r.get_requested_type()]
                        self.calendar.remove_request(r, s)
                        self.calendar.add_request(r, start_time, other_source)
                    else:
                        r.fail_warn("This agent has no resources to satisfy a request for type: {}".format(r.get_requested_type()))

    def allocate_extending(self, sources: list, start_time: int):
        """
        Iterates through all of the sources and allocates resources to
        their extending bids based on FIFO. Only allocates requests with types
        specified in requestTypes or all requests if requestTypes is None
       
        @param sources sources for this allocation
        @param start_time when the allocation begins
       
        @throws Exception in case of error
        """
        for s in sources:
            extending_for_source = self.calendar.get_request(s, start_time)
            if extending_for_source is None or extending_for_source.size() == 0:
                self.logger.debug("There are no extends for source: {}".format(s))
            else:
                self.logger.debug("There are {} extend reservations".format(extending_for_source.size()))

                for extend in extending_for_source.values():
                    self.logger.info("Extend res {}".format(extend))

                    wanted = extend.get_requested_units()
                    self.logger.debug("ASPPlugin:allocateBids: allocating source rid({}) to extendDynamicRes({}) that has props:{}".format(
                            s.get_reservation_id(), extend.get_reservation_id(),
                            extend.get_requested_resources().get_request_properties()))
                    satisfy = self.verify_wanted_resources(extend, s, start_time, wanted)

                    if satisfy:
                        self.satisfy_allocation(extend, s, wanted, start_time)
                    extending_for_source.remove(extend)

    def allocate_new_bids(self, new_bids: list, source_hash: dict, start_time: int):
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
                    satisfy = self.verify_wanted_resources(reservation, source, start_time, wanted)
                    if satisfy:
                        self.satisfy_allocation(reservation, source, wanted, start_time)
                else:
                    reservation.fail("There are no sources of type {}".format(reservation.get_requested_type()))

            new_bids.remove(reservation)

    def allocate(self, cycle: int):
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
        if self.get_next_allocation(cycle) != cycle:
            self.logger.debug("Next allocation cycle {} != {}".format(self.get_next_allocation(cycle), cycle))
            return

        self.last_allocation = cycle
        # Determine the cycle for which the agent is allocating resources
        start_time = self.get_start_for_allocation(cycle)

        all_bids = self.calendar.get_requests(start_time)

        # If there are no extending and no new requests - return
        if all_bids is None or all_bids.size() == 0:
            self.logger.debug("No requests for allocation start cycle {}".format(start_time))
            return

        self.logger.debug("There are {} bids for cycle {}".format(all_bids.size(), start_time))

        # Put source on a Hashtable for the new request allocation
        holdings = self.calendar.get_holdings(self.clock.cycle(cycle=start_time))
        sources = holdings.values()
        source_hash = self.create_source_dict(sources)

        self.switch_source(sources, start_time, source_hash)

        self.allocate_extending(sources, start_time)

        self.allocate_new_bids(all_bids.values(), source_hash, start_time)

    def bind(self, reservation: IBrokerReservation) -> bool:
        term = reservation.get_requested_term()
        self.logger.info("SlottedAgent bind arrived at cycle {} requested term {}".format(self.actor.get_current_cycle(), term))

        bid_cycle = self.get_allocation(reservation)

        self.calendar.add_request(reservation, bid_cycle)

        return False

    def calculate_available(self, start_time: int, source: IClientReservation) -> int:
        """
        Calculates the available resources from the specified source for
        the specified start time.
       
        @param start_time start of proposed ticket
        @param source source whose resources are being counted
       
        @return number of available units
        """
        start_date = self.clock.cycle_start_date(start_time)

        outlays = self.calendar.get_outlays(source, start_date)
        c = self.count(outlays, start_date)
        active = c.count_active(source.get_type())

        available = source.get_units(start_date) - active

        self.logger.debug("There are {} resources available - extend".format(available))
        self.logger.debug("There are {} resources active".format(active))
        return available

    def extend_broker(self, reservation:IBrokerReservation) -> bool:
        requested_term = reservation.get_requested_term()
        self.logger.info("SlottedAgent extend arrived at cycle {} requested term {}".format(self.actor.get_current_cycle(), requested_term))

        source = reservation.get_source()

        if source is None:
            self.error("cannot find parent ticket for extend")

        if source.is_failed():
            self.error("parent ticket could not be renewed")

        bid_cycle = self.get_allocation(reservation)

        self.calendar.add_request(reservation, bid_cycle, source)
        self.calendar.add_request(reservation, bid_cycle)

        return False

    def extract_ticket(self, reservation: IBrokerReservation, source: IClientReservation, approved: Term, resource_share: int):
        mine = None
        ticket = source.get_resources()
        try:
            if not source.get_term().contains(term=approved):
                reservation.fail("Source term does not contain requested term or reservation is not elastic in time: sourceterm={} resterm={}".format(source.get_term(), approved), None)
            else:
                delegation = self.actor.get_plugin().get_ticket_factory().make_delegation(units=resource_share, term=approved, rtype=ticket.get_type(), holder=self.get_client_id(reservation))
                mine = self.extract(ticket, delegation)

                req_properties = reservation.get_requested_resources().get_request_properties()
                mine.set_request_properties(req_properties)
        except Exception as e:
            self.logger.error("Term not satisfied: Mapper extract failed: has: {} {}".format(ticket.get_concrete_units(), e))
            raise Exception("Term not satisfied: Mapper extract failed: has: {} {}".format(ticket.get_concrete_units(), e))

        if mine is not None and not reservation.is_failed():
            reservation.set_approved(approved, mine)
            reservation.set_source(source)

            self.logger.debug("allocated: {} for term {}".format(mine.get_units(), approved))
            self.logger.debug("resource_share {} mine {}".format(resource_share, mine.get_units()))

        return mine

    def formulate_bids(self, cycle: int) -> Bids:
        pending = self.calendar.get_pending()
        renewing = self.calendar.get_renewing(cycle)
        extending = self.process_renewing(renewing, pending)
        return Bids(ReservationSet(), extending)

    def get_allocation(self, reservation: IBrokerReservation) -> int:
        if not self.ready:
            self.error("Agent not ready to accept bids")

        start = self.clock.cycle(when=reservation.get_requested_term().get_new_start_time())

        start -= self.ADVANCE_TIME

        intervals = int((start - self.last_allocation)/self.CALL_INTERVAL)

        if intervals <= 0:
            intervals = 1

        start = self.last_allocation + (intervals * self.CALL_INTERVAL) + self.ADVANCE_TIME

        return start

    def get_approved_term(self, reservation: IBrokerReservation) -> Term:
        return Term(start=reservation.get_requested_term().get_start_time(),
                    end=reservation.get_requested_term().get_end_time(),
                    new_start=reservation.get_requested_term().get_new_start_time())

    def get_next_allocation(self, cycle: int) -> int:
        return self.last_allocation + self.CALL_INTERVAL

    def get_renew(self, reservation: IClientReservation) -> int:
        new_start_cycle = self.actor.get_actor_clock().cycle(when=reservation.get_term().get_end_time()) + 1
        return new_start_cycle - self.ADVANCE_TIME - self.CLOCK_SKEW

    def get_start_for_allocation(self, allocation_cycle: int) -> int:
        return allocation_cycle + self.ADVANCE_TIME

    def get_end_for_allocation(self, allocation_cycle: int) -> int:
        return allocation_cycle + self.ADVANCE_TIME + self.allocation_horizon

    def prepare(self, cycle: int):
        if not self.ready:
            self.last_allocation = cycle - self.CALL_INTERVAL
            self.ready = True

        try:
            self.check_pending()
        except Exception as e:
            self.logger.error("Exception in prepare {}".format(e))

    def process_renewing(self, renewing: ReservationSet, pending: ReservationSet) -> ReservationSet:
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

        self.logger.debug("Expiring = {}".format(renewing.size()))

        for reservation in renewing.values():
            self.logger.debug("Expiring res: {}".format(reservation))

            if reservation.is_renewable():
                self.logger.debug("This is a renewable expiring reservtion")

                term = reservation.get_term()

                term = term.extend()

                reservation.set_approved(term, reservation.get_resources().abstract_clone())

                result.add(reservation)
                self.calendar.add_pending(reservation)
            else:
                self.logger.debug("This is not a renewable expiring res")

        return result

    def query(self, p: dict) -> dict:
        """
        Returns the ADVANCE_TIME of an agent's allocation
        in the properties. This is used so that controller and downstream
        brokers know how early to bid. If the requested properties is null, the
        agent returns all of the properties that it has defined.
        """
        result =  super().query(p)

        result["advanceTime"] = self.ADVANCE_TIME
        return result

    def satisfy_allocation(self, reservation: IBrokerReservation, source: IClientReservation, resource_share: int,
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
        approved = self.get_approved_term(reservation)

        # Shift the requested term to start at the start time
        if self.clock.cycle(when=approved.get_new_start_time()) != start_res_time:
            if PropList.is_elastic_time(reservation.get_requested_resources()):
                self.logger.debug("Reservation {} was schedules to start at {} is being shifted to {}"
                                  .format(reservation, self.clock.cycle(when=approved.get_new_start_time()),
                                          start_res_time))
                approved = approved.shift(self.clock.cycle_start_date(start_res_time))
            else:
                reservation.fail("Reservation has a different start time and time shifting is not allowed", None)

        # Make sure the term end is aligned on a cycle boundary. NOTE: this
        # should also be aligned on call interval boundary!

        # calculate the cycle
        cycle = self.clock.cycle(when=approved.get_end_time())

        end_millis = self.clock.cycle_end_in_millis(cycle)

        # get the date that represents the end of the cycle
        aligned_end = datetime.fromtimestamp(end_millis / 1000)
        # update the term end
        approved.set_end_time(aligned_end)

        # Get the ticket with the correct resources
        mine = self.extract_ticket(reservation, source, approved, resource_share)

        # Add to the calendar
        self.add_to_calendar(reservation)

        # Whatever happened up there, this bid is no longer pending. It either
        # succeeded or is now marked failed. (Could/should assert.)
        reservation.set_bid_pending(False)

    def verify_wanted_resources(self, reservation: IBrokerReservation, source: IClientReservation, start_time: int,
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
        available = self.calculate_available(start_time, source)
        mywanted = wanted

        self.logger.debug("ASPPlugin:allocateBids: allocating source rid({}) to newDynamicRes({}) that has props:{}"
                          .format(source.get_reservation_id(),
                                  reservation.get_reservation_id(),
                                  reservation.get_requested_resources().get_request_properties()))

        # Check if we have sufficient resources. If the reservation is flexible
        # adjust its resource demand to what is available.
        if wanted > available:
            if PropList.is_elastic_size(reservation.get_requested_resources()) and available > 0:
                self.logger.debug("broker is shrinking an elastic request")
                mywanted = available
        else:
            reservation.fail_warn("Selected source holding has insufficient resources: source {} available {}; request {}"
                                  .format(source, available, reservation))

        # If the (possibly adjusted) demand fits in what we've got, then try to
        # allocate.
        if mywanted <= available:
            return True
        else:
            return False