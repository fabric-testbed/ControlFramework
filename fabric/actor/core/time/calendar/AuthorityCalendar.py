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

from fabric.actor.core.apis.IAuthorityReservation import IAuthorityReservation
from fabric.actor.core.apis.IReservation import IReservation
from fabric.actor.core.apis.IServerReservation import IServerReservation
from fabric.actor.core.time.ActorClock import ActorClock
from fabric.actor.core.time.calendar.BaseCalendar import BaseCalendar
from fabric.actor.core.util.ReservationHoldings import ReservationHoldings
from fabric.actor.core.util.ReservationList import ReservationList
from fabric.actor.core.util.ReservationSet import ReservationSet


class AuthorityCalendar(BaseCalendar):
    """
    An AuthorityCalendar is used to organized reservation information for an authority. It extends the functionality of
    BaseCalendar with a number of collections:
    - requests: a collection of client requests organized by the time to be serviced
    - closing: a collection of client reservations organized by closing time
    - outlays: a collection of active reservations (outlays)
    """

    def __init__(self, clock: ActorClock):
        """
        Creates a new instance.
        @params clock : clock factory
        """
        # List of incoming requests grouped by start cycle.
        super().__init__(clock)
        self.requests = ReservationList()
        # List of reservations to be closed grouped by cycle.
        self.closing = ReservationList()
        # All currently active reservations.
        self.outlays = ReservationHoldings()
        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.lock = threading.Lock()

    def remove(self, reservation: IReservation):
        """
        Removes the specified reservation from the calendar.
        @params reservation : reservation to remove
        """
        try:
            self.lock.acquire()
            if isinstance(reservation, IServerReservation):
                self.remove_request(reservation)
                self.remove_closing(reservation)

            if isinstance(reservation, IAuthorityReservation):
                self.remove_outlay(reservation)
        finally:
            self.lock.release()

    def remove_schedule_or_in_progress(self, reservation: IReservation):
        """
        Removes the specified reservations from all internal calendar data
        structures that represent operations to be scheduled in the future or
        operations that are currently in progress. Does not remove the
        reservation from the outlays list
        @params reservation: reservation to remove
        """
        try:
            self.lock.acquire()
            if isinstance(reservation, IServerReservation):
                self.remove_request(reservation)
                self.remove_closing(reservation)
        finally:
            self.lock.release()

    def get_requests(self, cycle : int) -> ReservationSet:
        """
        Returns all client requests for the specified cycle.
        @params cycle:  cycle
        @returns set of requests for the cycle
        """
        try:
            self.lock.acquire()
            return self.requests.get_reservations(cycle)
        finally:
            self.lock.release()

    def add_request(self, reservation: IReservation, cycle: int):
        """
        Adds a new client request.
        @params reservation: reservation to add
        @params cycle: cycle
        """
        try:
            self.lock.acquire()
            self.requests.add_reservation(reservation, cycle)
        finally:
            self.lock.release()

    def remove_request(self, reservation: IReservation):
        """
        Removes the specified reservation from the request list.
        @params reservation: reservation to remove
        """
        self.requests.remove_reservation(reservation)

    def get_closing(self, cycle: int) -> ReservationSet:
        """
        Returns all reservations scheduled for closing at the specified cycle.
        @params cycle: cycle
        @returns set of reservations scheduled for closing at the cycle
        """
        try:
            self.lock.acquire()
            result = self.closing.get_all_reservations(cycle)
            return result
        finally:
            self.lock.release()

    def add_closing(self, reservation: IReservation, cycle: int):
        """
        Adds a reservation to the closing list.
        @params reservation: reservation to add
        @params cycle: cycle
        """
        try:
            self.lock.acquire()
            self.closing.add_reservation(reservation, cycle)
        finally:
            self.lock.release()

    def remove_closing(self, reservation: IReservation):
        """
        Removes the specified reservation from the closing list.
        @params reservation: reservation to remove
        """
        self.closing.remove_reservation(reservation)

    def add_outlay(self, reservation: IReservation, start: datetime, end: datetime):
        """
        Adds an allocated client reservation.
        @params reservation: reservation to add
        @params start: start time
        @params end: end time
        """
        try:
            self.lock.acquire()
            self.outlays.add_reservation(reservation, int(start.timestamp() * 1000), int(end.timestamp() * 1000))
        finally:
            self.lock.release()

    def remove_outlay(self, reservation: IReservation):
        """
        Removes a reservation from the outlays list.
        @params reservation: reservation to remove
        """
        try:
            self.lock.acquire()
            self.outlays.remove_reservation(reservation)
        finally:
            self.lock.release()

    def get_outlays(self, d: datetime = None) -> ReservationSet:
        """
        Returns the active client reservations.
        @returns set of all active client reservations
        """
        try:
            self.lock.acquire()
            if d is None:
                return self.outlays.get_reservations()
            else:
                return self.outlays.get_reservations(int(d.timestamp() * 1000))
        finally:
            self.lock.release()

    def tick(self, cycle: int):
        try:
            self.lock.acquire()
            super().tick(cycle)
            self.requests.tick(cycle)
            self.closing.tick(cycle)

            ms = self.clock.cycle_end_in_millis(cycle)
            self.outlays.tick(ms)
        finally:
            self.lock.release()