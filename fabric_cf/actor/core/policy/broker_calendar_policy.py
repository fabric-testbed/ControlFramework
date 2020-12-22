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

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import BrokerException
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.actor.core.time.calendar.broker_calendar import BrokerCalendar
from fabric_cf.actor.core.util.resource_count import ResourceCount
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric_cf.actor.core.core.broker_policy import BrokerPolicy
from fabric_cf.actor.core.apis.i_client_reservation import IClientReservation

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_broker import IBroker
    from fabric_cf.actor.core.apis.i_reservation import IReservation


class BrokerCalendarPolicy(BrokerPolicy):
    """
    BrokerCalendarPolicy specifies and implements some of the
    broker's base resource allocation and upstream bidding policy.
    """
    PropertyTypeNamePrefix = "type.name."
    PropertyTypeDescriptionPrefix = "type.description."
    PropertyTypeUnitsPrefix = "type.units."
    PropertyTypeCount = "type.count"
    PropertyDiscoverTypes = "query.discovertypes"

    def __init__(self, *, actor: IBroker):
        super().__init__(actor=actor)
        # The broker calendar: list of client requests, source reservations, and allocated reservations.
        self.calendar = None
        # Indicates if this actor is initialized
        self.initialized = False

    def add_for_approval_calendar(self, *, reservation: IBrokerReservation):
        """
        Adds the reservation to the approval list and removes the
        reservation from the closing calendar (if it belongs to it).

        @param reservation reservation to add
        @throws Exception in case of error
        """
        if reservation.get_term() is not None:
            self.add_for_approval(reservation=reservation)

    def add_to_calendar(self, *, reservation: IBrokerReservation):
        """
        Records the reservation in the calendar.

        @param reservation reservation
        """
        if reservation.get_approved_resources() is not None and not reservation.is_failed():
            self.calendar.add_outlay(source=reservation.get_source(), client=reservation,
                                     start=reservation.get_approved_term().get_new_start_time(),
                                     end=reservation.get_approved_term().get_end_time())

            if reservation.get_term() is not None:
                self.calendar.remove_closing(reservation=reservation)

            self.calendar.add_closing(reservation=reservation,
                                      cycle=self.clock.cycle(when=reservation.get_approved_term().get_end_time()))

            self.logger.debug("AgentAllocated: units= {} res= {} term= {} "
                              .format(reservation.get_approved_resources().get_units(),
                                      reservation, reservation.get_approved_term()))
        else:
            reservation.fail(message="Either there are no resources on the source or the reservation failed",
                             exception=None)

    def approve(self, *, reservation: IBrokerReservation):
        # Our current policy is to allow the allocation code to issue a ticket
        # but to prevent the ticket from being sent to the client until the
        # administrator approves the allocation.

        self.remove_for_approval(reservation=reservation)

        self.add_to_calendar(reservation=reservation)

        reservation.set_bid_pending(value=False)

    def check_pending(self):
        """
        Checks pending bids, and installs successfully completed
        requests in the holdings calendar. Note that the policy module must add
        bids to the pending set, or they may not install in the calendar.

        @throws Exception in case of error
        """
        rvset = self.calendar.get_pending()

        if rvset is None:
            return

        for reservation in rvset.values():
            if not reservation.is_nascent() and reservation.is_no_pending():
                self.logger.debug("Pending request completed {}".format(reservation))

            if not reservation.is_terminal() and reservation.is_renewable():
                cycle = self.get_renew(reservation=reservation)
                reservation.set_renew_time(time=cycle)
                reservation.set_dirty()

            self.calendar.remove_pending(reservation)

    def close(self, *, reservation: IReservation):
        if isinstance(reservation, IClientReservation):
            rset = self.calendar.get_outlays(source=reservation)
            self.logger.debug("Client reservation; get outlays: {}".format(rset))
            self.actor.close_reservations(reservations=rset)
        else:
            self.logger.debug("Removing reservation from scheduled or in progress list")
            self.calendar.remove_scheduled_or_in_progress(reservation=reservation)

    def closed(self, *, reservation: IReservation):
        self.release(reservation=reservation)

    def count(self, *, rvset: ReservationSet, when: datetime):
        """
        Returns a counter for the passed set and the specified data.

        @param rvset the set of reservations being counted
        @param when the date when to count the resources

        @return counter
        """
        rc = ResourceCount()
        rvset.count(rc=rc, when=when)
        return rc

    def donate_reservation(self, *, reservation: IClientReservation):
        term = reservation.get_term()
        term.validate()

        self.logger.debug("Donated ticket {} term {}".format(reservation, term))

        term = reservation.get_previous_term()

        if term is not None:
            self.calendar.remove_closing(reservation=reservation)

        self.calendar.add_source(source=reservation)
        term = reservation.get_term()
        self.calendar.add_closing(reservation=reservation,
                                  cycle=self.clock.cycle(when=term.get_end_time()))

        if reservation.is_renewable():
            self.calendar.add_renewing(reservation=reservation, cycle=reservation.get_renew_time())

    def finish(self, *, cycle: int):
        self.calendar.tick(cycle=cycle)

    def get_closing(self, *, cycle: int) -> ReservationSet:
        return self.calendar.get_closing(cycle=cycle)

    def get_renew(self, *, reservation: IClientReservation) -> int:
        """
        Returns the cycle when the reservation must be renewed.

        @param reservation reservation for which to calculate renew time

        @return renew cycle

        @throws Exception in case of error
        """
        raise BrokerException("not implemented")

    def initialize(self):
        if not self.initialized:
            super().initialize()
            self.calendar = BrokerCalendar(clock=self.clock)

            if self.required_approval:
                self.for_approval = ReservationSet()

            self.initialized = True

    def release(self, *, reservation):
        if isinstance(reservation, IBrokerReservation):
            self.logger.debug("Broker reservation")
            source = reservation.get_source()
            if source is not None:
                self.logger.debug("Broker reservation; removing outlay")
                self.calendar.remove_outlay(source=source, client=reservation)

        elif isinstance(reservation, IClientReservation):
            self.logger.debug("Client reservation; removing source calendar")
            self.calendar.remove_source_calendar(source=reservation)

    def release_not_approved(self, *, reservation: IBrokerReservation):
        self.release(reservation=reservation)

    def remove(self, *, reservation: IReservation):
        self.calendar.remove(reservation=reservation)

    def revisit(self, *, reservation: IReservation):
        super().revisit(reservation=reservation)

        if isinstance(reservation, IClientReservation):
            self.revisit_client(reservation=reservation)
        elif isinstance(reservation, IBrokerReservation):
            self.revisit_server(reservation=reservation)

    def revisit_client(self, *, reservation: IClientReservation):
        """
        Recovers a source reservation.

        @param reservation reservation to recover

        @throws Exception in case of error
        """
        if (reservation.get_state() == ReservationStates.Nascent and
            reservation.get_pending_state() == ReservationPendingStates.None_) or \
            (reservation.get_state() == ReservationStates.Ticketed and
             reservation.get_pending_state() == ReservationPendingStates.ExtendingTicket):
            self.calendar.add_pending(reservation=reservation)

    def revisit_server(self, *, reservation: IBrokerReservation):
        """
        Recovers a client reservation.

        @param reservation reservation to recover

        @throws Exception in case of error
        """
        if reservation.get_state() == ReservationStates.Ticketed and \
                (reservation.get_pending_state() == ReservationPendingStates.None_ or
                    reservation.get_pending_state() == ReservationPendingStates.Priming):
            source = reservation.get_source()
            if source is None:
                raise BrokerException(Constants.not_specified_prefix.format("source reservation"))

            self.calendar.add_outlay(source=source,
                                     client=reservation,
                                     start=reservation.get_term().get_new_start_time(),
                                     end=reservation.get_term().get_end_time())

            self.calendar.add_closing(reservation=reservation,
                                      cycle=self.clock.cycle(when=reservation.get_term().get_end_time()))

    def query(self, *, p):
        self.logger.debug("Processing Query with properties: {}".format(p))
        result = {}
        if p is not None and self.PropertyDiscoverTypes in p:
            holdings = self.calendar.get_holdings()
            count = 0
            for reservation in holdings.values():
                try:
                    ticket = reservation.get_resources()
                    if ticket is not None:
                        rdata = ticket.get_resource_data()
                        if rdata is not None:
                            resource_properties = rdata.get_resource_properties()
                            if resource_properties is not None:
                                result[self.PropertyTypeNamePrefix + str(count)] = ticket.get_type().get_type()
                                result[self.PropertyTypeDescriptionPrefix + str(count)] = p
                                count += 1
                except Exception as e:
                    self.logger.error("query", e)

                result[self.PropertyTypeCount] = count
        self.logger.debug("Returning Query Result: {}".format(result))
        return result

