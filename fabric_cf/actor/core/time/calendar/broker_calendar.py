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
from datetime import datetime

from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.time.calendar.client_calendar import ClientCalendar
from fabric_cf.actor.core.time.calendar.source_calendar import SourceCalendar
from fabric_cf.actor.core.util.reservation_list import ReservationList
from fabric_cf.actor.core.util.reservation_set import ReservationSet


class BrokerCalendar(ClientCalendar):
    """
    BrokerCalendar used to organize reservation information for a broker. It builds on the functionality provided by
    ClientCalendar and extends it with the following lists:
    - closing: list of reservations organized by closing time
    - requests: list of incoming requests
    - source calendars for each source reservation
    """
    def __init__(self, *, clock: ActorClock):
        """
        Constructor
        @params clock: clock factory
        """
        super().__init__(clock=clock)
        # List of reservations grouped by closing time.
        self.closing = ReservationList()
        # Reservation requests grouped by start cycle.
        self.requests = ReservationList()
        # Source reservation calendars indexed by the source reservation identifier
        # <ReservationID, SourceCalendar>
        self.sources = {}

    def remove(self, *, reservation: ABCReservationMixin):
        super().remove(reservation=reservation)
        self.remove_closing(reservation=reservation)
        if isinstance(reservation, ABCBrokerReservation):
            self.remove_request(reservation=reservation)

            source = reservation.get_source()
            if source is not None:
                self.remove_request(reservation=reservation, source=source)
                self.remove_outlay(source=source, client=reservation)

    def remove_scheduled_or_in_progress(self, *, reservation: ABCReservationMixin):
        super().remove_scheduled_or_in_progress(reservation=reservation)
        self.remove_closing(reservation=reservation)

        if isinstance(reservation, ABCBrokerReservation):
            self.remove_request(reservation=reservation)

            source = reservation.get_source()
            if source is not None:
                self.remove_request(reservation=reservation, source=source)

    def get_requests(self, *, cycle: int) -> ReservationSet:
        """
        Returns all client requests for the given cycle.
        @params cycle: cycle
        @returns set of reservations representing requests starting at the specified cycle
        """
        try:
            self.lock.acquire()
            return self.requests.get_reservations(cycle=cycle)
        finally:
            self.lock.release()

    def get_all_requests(self, *, cycle: int) -> ReservationSet:
        """
        Returns all client requests up the the given cycle.
        @params cycle: cycle
        @returns set of reservations representing requests with start time no later than cycle
        """
        try:
            self.lock.acquire()
            return self.requests.get_all_reservations(cycle=cycle)
        finally:
            self.lock.release()

    def add_request(self, *, reservation: ABCReservationMixin, cycle: int, source: ABCDelegation = None):
        """
        Adds a client request.

        @params reservation: client request
        @params cycle: start cycle
        @params source: source reservation
        """
        try:
            self.lock.acquire()
            if source is None:
                self.requests.add_reservation(reservation=reservation, cycle=cycle)
            else:
                calendar = self.get_source_calendar(source=source)
                calendar.extending.add_reservation(reservation=reservation, cycle=cycle)
        finally:
            self.lock.release()

    def get_request(self, *, source: ABCDelegation, cycle: int) -> ReservationSet:
        """
        Returns the extending requests for the given source reservation.

        @params source: source reservation
        @params cycle: cycle number

        @returns set of extending reservation requests for the given source at
                the specified cycle
        """
        try:
            self.lock.acquire()
            calendar = self.get_source_calendar(source=source)
            return calendar.extending.get_reservations(cycle=cycle)
        finally:
            self.lock.release()

    def remove_request(self, *, reservation: ABCReservationMixin, source: ABCDelegation = None):
        """
        Removes the specified reservation from the requests list.
        @params reservation:  reservation to remove
        @params source: source reservation
        """
        try:
            self.lock.acquire()
            if source is not None:
                calendar = self.get_source_calendar(source=source)
                calendar.extending.remove_reservation(reservation=reservation)
            else:
                self.requests.remove_reservation(reservation=reservation)
        finally:
            self.lock.release()

    def add_outlay(self, *, source: ABCDelegation, client: ABCReservationMixin, start: datetime, end: datetime):
        """
         Adds an outlay reservation.

        @params source: source reservation
        @params client: reservation to add
        @params start: start time
        @params end: start time
        """
        try:
            self.lock.acquire()
            calendar = self.get_source_calendar(source=source)
            calendar.outlays.add_reservation(reservation=client, start=ActorClock.to_milliseconds(when=start),
                                             end=ActorClock.to_milliseconds(when=end))
        finally:
            self.lock.release()

    def remove_outlay(self, *, source: ABCDelegation, client: ABCReservationMixin):
        """
        Removes an outlay reservation.

        @params source : source reservation
        @params client : client reservation to be removed
        """
        try:
            self.lock.acquire()
            calendar = self.get_source_calendar(source=source)
            calendar.outlays.remove_reservation(reservation=client)
        finally:
            self.lock.release()

    def add_source(self, *, source: ABCDelegation):
        """
        Adds a source reservation. Creates a placeholder if necessary
        and adds the reservation to the holdings list.

        @params source:  source reservation
        """
        term = None
        try:
            self.lock.acquire()
            self.get_source_calendar(source=source)
            term = source.get_term()
        finally:
            self.lock.release()
        self.add_holdings(reservation=source, start=term.get_new_start_time(), end=term.get_end_time())

    def get_source_calendar(self, *, source: ABCDelegation) -> SourceCalendar:
        """
        Returns the outlay calendar for the given source reservation.

        @params source : source reservation

        @returns source calendar
        """
        calendar = self.sources.get(source.get_delegation_id())
        if calendar is None:
            calendar = SourceCalendar(clock=self.clock, source=source)
            self.sources[source.get_delegation_id()] = calendar
        return calendar

    def remove_source_calendar(self, *, source: ABCDelegation):
        """
        Removes any data structures associated with a source
        reservation.

        @params source : source reservation
        """
        try:
            self.lock.acquire()
            if source.get_delegation_id() in self.sources:
                self.sources.pop(source.get_delegation_id())
        finally:
            self.lock.release()

    def get_outlays(self, *, source: ABCDelegation, time: datetime = None) -> ReservationSet:
        """
        Returns the client reservations satisfied from the given source
        reservation at the specified time.

        @params source : source reservation
        @params time:  time instance

        @returns set of client reservations satisfied from the given source at the specified time.
        """
        try:
            self.lock.acquire()
            calendar = self.get_source_calendar(source=source)
            if time is None:
                return calendar.outlays.get_reservations()
            else:
                return calendar.outlays.get_reservations(time=ActorClock.to_milliseconds(when=time))
        finally:
            self.lock.release()

    def get_closing(self, *, cycle: int) -> ReservationSet:
        """
        Returns all reservations that need to be closed on the specified
        cycle.

        @params cycle : cycle

        @returns a set of reservations to be closed on the specified cycle
        """
        try:
            self.lock.acquire()
            return self.closing.get_all_reservations(cycle=cycle)
        finally:
            self.lock.release()

    def add_closing(self, *, reservation: ABCReservationMixin, cycle: int):
        """
        Adds a reservation to be closed on the specified cycle

        @params reservation : reservation to close
        @params cycle : cycle
        """
        try:
            self.lock.acquire()
            self.closing.add_reservation(reservation=reservation, cycle=cycle)
        finally:
            self.lock.release()

    def remove_closing(self, *, reservation: ABCReservationMixin):
        """
        Removes the specified reservation from the list of closing
        reservations.

        @params reservation : reservation to remove
        """
        try:
            self.lock.acquire()
            self.closing.remove_reservation(reservation=reservation)
        finally:
            self.lock.release()

    def tick(self, *, cycle: int):
        super().tick(cycle=cycle)
        try:
            self.lock.acquire()
            self.requests.tick(cycle=cycle)
            self.closing.tick(cycle=cycle)

            for calendar in self.sources.values():
                calendar.tick(cycle=cycle)
        finally:
            self.lock.release()
