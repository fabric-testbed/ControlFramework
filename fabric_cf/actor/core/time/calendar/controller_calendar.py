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
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.time.calendar.client_calendar import ClientCalendar
from fabric_cf.actor.core.util.reservation_list import ReservationList
from fabric_cf.actor.core.util.reservation_set import ReservationSet


class ControllerCalendar(ClientCalendar):
    """
    Controller calendar. In addition to the lists maintained by ClientCalendar,
    this class maintains the following lists:
     - closing: a list of reservations organized by the cycle they must be closed
     - redeeming: a list of reservations organized by the cycle they must be redeemed
    """
    def __init__(self, *, clock: ActorClock):
        """
        Constructor
        @params clock: clock
        """
        super().__init__(clock=clock)
        self.closing = ReservationList()
        self.redeeming = ReservationList()

    def remove(self, *, reservation: ABCReservationMixin):
        """
        Removes the reservation from the calendar.

        @params reservation: reservation to remove
        """
        super().remove(reservation=reservation)
        self.remove_closing(reservation=reservation)
        self.remove_redeeming(reservation=reservation)

    def get_closing(self, *, cycle: int) -> ReservationSet:
        """
        Returns all reservations that need to be closed up to and including the specified cycle.

        @params cycle : cycle

        @returns a set of reservations that must be closed on the specified cycle
        """
        try:
            self.lock.acquire()
            return self.closing.get_all_reservations(cycle=cycle)
        finally:
            self.lock.release()

    def add_closing(self, *, reservation: ABCReservationMixin, cycle: int):
        """
        Adds a reservation to be closed on the specified cycle.

        @params reservation : reservation to add
        @params cycle : cycle
        """
        try:
            self.lock.acquire()
            self.closing.add_reservation(reservation=reservation, cycle=cycle)
        finally:
            self.lock.release()

    def remove_closing(self, *, reservation: ABCReservationMixin):
        """
         Removes the given reservation from the closing list.

        @params reservation : reservation to remove
        """
        try:
            self.lock.acquire()
            self.closing.remove_reservation(reservation=reservation)
        finally:
            self.lock.release()

    def get_redeeming(self, *, cycle: int) -> ReservationSet:
        """
        Returns all reservations that need to be redeemed up to and including the specified cycle.

        @params cycle : cycle

        @returns a set of reservations that must be redeemed on the specified cycle
        """
        try:
            self.lock.acquire()
            return self.redeeming.get_all_reservations(cycle=cycle)
        finally:
            self.lock.release()

    def add_redeeming(self, *, reservation: ABCReservationMixin, cycle: int):
        """
        Adds a reservation to be redeeming on the specified cycle.

        @params reservation : reservation to add
        @params cycle : cycle
        """
        try:
            self.lock.acquire()
            self.redeeming.add_reservation(reservation=reservation, cycle=cycle)
        finally:
            self.lock.release()

    def remove_redeeming(self, *, reservation: ABCReservationMixin):
        """
         Removes the given reservation from the redeeming list.

        @params reservation : reservation to remove
        """
        try:
            self.lock.acquire()
            self.redeeming.remove_reservation(reservation=reservation)
        finally:
            self.lock.release()

    def tick(self, *, cycle: int):
        super().tick(cycle=cycle)
        try:
            self.lock.acquire()
            self.closing.tick(cycle=cycle)
            self.redeeming.tick(cycle=cycle)
        finally:
            self.lock.release()
