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
import datetime

from actor.plugins.apis.IReservation import IReservation
from actor.plugins.util.ResourceCount import ResourceCount


class ReservationSet:
    """
    ReservationSet is a collection of reservations indexed by ReservationID
    """
    def __init__(self):
        self.reservations = {}

    def __int__(self, reservations: dict):
        self.reservations = reservations

    def add(self, reservation: IReservation):
        """
        Adds the reservation to the set

        Args:
            reservation: reservation to be added
        """
        self.reservations[reservation.get_reservation_id()] = reservation

    def clear(self):
        """
        Remove all the reservations from the set
        """
        self.reservations.clear()

    def contains(self, reservation: IReservation):
        """
        Checks if the reservation is part of the set

        Args:
            reservation: reservation to check
        Returns:
            true if the set contains the specified reservation; false otherwise
        """
        if reservation.get_reservation_id() in self.reservations :
            return True
        return False

    def contains(self, rid: str):
        """
        Checks if the reservation is part of the set

        Args:
            rid: reservation id
        Returns:
            true if the set contains the specified reservation; false otherwise
        """
        if rid in self.reservations:
            return True
        return False

    def count(self, rc: ResourceCount, when: datetime):
        """
        Tallies up resources in the ReservationSet. Note: "just a hint" unless kernel lock is held.

        Args:
            rc: holder for counts
            when: date relative to which to do the counting
        """
        for reservation in self.reservations.values():
            reservation.count(rc, when)

    def get(self, rid: str):
        """
        Retrieves a reservation from the set.

        Args:
            rid: reservation id
        Returns:
            Reservation identified by rid
        """
        return self.reservations.get(rid)

    def is_empty(self):
        """
        Checks if the set is empty.

        Returns:
            true if the set is empty
        """
        if len(self.reservations.keys()) == 0:
            return True
        return False

    def remove(self, reservation: IReservation):
        """
        Removes the specified reservation.

        Args:
            reservation: reservation to remove
        """
        self.reservations.pop(reservation.get_reservation_id())

    def size(self):
        """
        Returns the number of reservations in the set.

        Returns:
            the size of the reservation set
        """
        return len(self.reservations.keys())