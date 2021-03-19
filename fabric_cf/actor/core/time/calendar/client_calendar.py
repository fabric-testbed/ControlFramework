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
from datetime import datetime

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.time.calendar.base_calendar import BaseCalendar
from fabric_cf.actor.core.util.reservation_holdings import ReservationHoldings
from fabric_cf.actor.core.util.reservation_list import ReservationList
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.core.util.resource_type import ResourceType


class ClientCalendar(BaseCalendar):
    """
    This a client-side calendar to be used by brokers  or service managers. A client
    calendar maintains the following lists:
    - demand: a list of reservations representing demand for resources
    - pending: a list of reservations with pending operations
    - renewing: a list of reservations organized by the time they must be renewed (cycle)
    - holdings: a list of active/granted reservations associated with their lease term.
      This list should be maintained using real time, not cycles.

    The renewing and holding lists are automatically purged by the implementation as
    time advances. The demand and pending list, however, must be purged manually by the
    user of this class.
    """
    def __init__(self, *, clock: ActorClock):
        """
        Constructor
        @params clock: clock factory
        """
        super().__init__(clock=clock)
        # Set of reservations representing the current demand. Callers are
        # responsible for removing serviced reservations
        self.demand = ReservationSet()
        # Set of reservations for which a request has been issued but no
        # confirmation has been received. Callers are responsible for removing
        # acknowledged reservations.
        self.pending = ReservationSet()
        # Set of reservations grouped by renewing time.
        self.renewing = ReservationList()
        # Set of active reservations.
        self.holdings = ReservationHoldings()
        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.lock = threading.Lock()

    def remove(self, *, reservation: ABCReservationMixin):
        """
        Removes the specified reservation from all internal calendar data structures
        @params reservation: reservation to remove
        """
        self.remove_demand(reservation=reservation)
        self.remove_pending(reservation=reservation)
        self.remove_renewing(reservation=reservation)
        self.remove_holdings(reservation=reservation)

    def remove_scheduled_or_in_progress(self, *, reservation: ABCReservationMixin):
        """
        Removes the specified reservations from all internal calendar data
        structures that represent operations to be scheduled in the future or
        operations that are currently in progress. Does not remove the
        reservation from the holdings list.
        @params reservation : reservation to remove
        """
        self.remove_demand(reservation=reservation)
        self.remove_pending(reservation=reservation)
        self.remove_renewing(reservation=reservation)

    def get_demand(self) -> ReservationSet:
        """
        Returns the known demand. Can be for resources starting at different times.
        @returns the set of demanded reservations.
        """
        try:
            self.lock.acquire()
            return self.demand.clone()
        finally:
            self.lock.release()

    def add_demand(self, *, reservation: ABCReservationMixin):
        """
        Adds a reservation to the demand list.
        @params reservation: reservation to add
        """
        try:
            self.lock.acquire()
            self.demand.add(reservation=reservation)
        finally:
            self.lock.release()

    def remove_demand(self, *, reservation: ABCReservationMixin):
        """
        Removes a reservation to the demand list.
        @params reservation: reservation to remove
        """
        try:
            self.lock.acquire()
            self.demand.remove(reservation=reservation)
        finally:
            self.lock.release()

    def get_pending(self) -> ReservationSet:
        """
        Returns the known pending. Can be for resources starting at different times.
        @returns the set of pending reservations.
        """
        try:
            self.lock.acquire()
            return self.pending.clone()
        finally:
            self.lock.release()

    def add_pending(self, *, reservation: ABCReservationMixin):
        """
        Adds a reservation to the pending list.
        @params reservation: reservation to add
        """
        try:
            self.lock.acquire()
            self.pending.add(reservation=reservation)
        finally:
            self.lock.release()

    def remove_pending(self, *, reservation: ABCReservationMixin):
        """
        Removes a reservation to the pending list.
        @params reservation: reservation to remove
        """
        try:
            self.lock.acquire()
            self.pending.remove(reservation=reservation)
        finally:
            self.lock.release()

    def get_renewing(self, *, cycle: int) -> ReservationSet:
        """
        Returns the reservations that need to be renewed on the specified cycle.
        @params cycle : cycle number
        @returns reservation set with reservations to be renewed on the specified cycle
        """
        try:
            self.lock.acquire()
            return self.renewing.get_all_reservations(cycle=cycle)
        finally:
            self.lock.release()

    def add_renewing(self, *, reservation: ABCReservationMixin, cycle: int):
        """
        Adds a reservation to the renewing list at the given cycle.
        @params reservation : reservation to add
        @params cycle : cycle number
        """
        try:
            self.lock.acquire()
            self.renewing.add_reservation(reservation=reservation, cycle=cycle)
        finally:
            self.lock.release()

    def remove_renewing(self, *, reservation: ABCReservationMixin):
        """
        Removes the reservation from the renewing list.
        @params reservation : reservation to remove
        """
        try:
            self.lock.acquire()
            self.renewing.remove_reservation(reservation=reservation)
        finally:
            self.lock.release()

    def get_holdings(self, *, d: datetime = None, type: ResourceType = None) -> ReservationSet:
        """
        Returns the resources of the specified type held by the client that are active at the specified time instance.
        @params d : datetime instance.
        @params type : resource type
        @returns st of reservations of the specified type that are active at the specified time
        """
        try:
            self.lock.acquire()
            when = None
            if d is not None:
                when = ActorClock.to_milliseconds(when=d)
            return self.holdings.get_reservations(time=when, rtype=type)
        finally:
            self.lock.release()

    def add_holdings(self, *, reservation: ABCReservationMixin, start: datetime, end: datetime):
        """
        Adds a reservation to the holdings list.
        @params reservation : reservation to add
        @params start : start time
        @params end : end time
        """
        try:
            self.lock.acquire()
            self.holdings.add_reservation(reservation=reservation, start=ActorClock.to_milliseconds(when=start),
                                          end=ActorClock.to_milliseconds(when=end))
        finally:
            self.lock.release()

    def remove_holdings(self, *, reservation: ABCReservationMixin):
        """
        Removes the reservation from the renewing list.
        @params reservation : reservation to remove
        """
        try:
            self.lock.acquire()
            self.holdings.remove_reservation(reservation=reservation)
        finally:
            self.lock.release()

    def tick(self, *, cycle: int):
        try:
            self.lock.acquire()
            super().tick(cycle=cycle)
            self.renewing.tick(cycle=cycle)
            ms = self.clock.cycle_end_in_millis(cycle=cycle)
            self.holdings.tick(time=ms)
        finally:
            self.lock.release()
