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

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.exceptions import FrameworkException
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_count import ResourceCount


class ReservationSet:
    """
    ReservationSet is a collection of reservations indexed by ReservationID
    """
    def __init__(self, *, reservations: dict = None):
        self.reservations = {}
        if reservations is not None:
            self.reservations = reservations

    def __str__(self):
        result = ""
        for r in self.reservations.values():
            result += "rid={} r={}".format(r.get_reservation_id(), r)
        return result

    def add(self, *, reservation: ABCReservationMixin):
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

    def contains(self, *, reservation: ABCReservationMixin = None, rid: ID = None):
        """
        Checks if the reservation is part of the set

        Args:
            reservation: reservation to check
            rid: reservation id
        Returns:
            true if the set contains the specified reservation; false otherwise
        """
        if reservation is not None and reservation.get_reservation_id() in self.reservations:
            return True
        if rid is not None and rid in self.reservations:
            return True
        return False

    def count(self, *, rc: ResourceCount, when: datetime):
        """
        Tallies up resources in the ReservationSet. Note: "just a hint" unless kernel lock is held.

        Args:
            rc: holder for counts
            when: date relative to which to do the counting
        """
        for reservation in self.reservations.values():
            reservation.count(rc, when)

    def get(self, *, rid: ID) -> ABCReservationMixin:
        """
        Retrieves a reservation from the set.

        Args:
            rid: reservation id
        Returns:
            Reservation identified by rid
        """
        return self.reservations.get(rid, None)

    def get_exception(self, *, rid: ID) -> ABCReservationMixin:
        """
        Returns the specified reservation. If the reservation is not
        present in the set, throws an exception.

        @param rid the reservation identifier

        @return Reservation identified by rid

        @throws Exception if the requested reservation is not present in the set
        """
        if rid in self.reservations:
            return self.reservations.get(rid)

        raise FrameworkException("No reservation with ID {}".format(rid))

    def is_empty(self) -> bool:
        """
        Checks if the set is empty.

        Returns:
            true if the set is empty
        """
        if len(self.reservations.keys()) == 0:
            return True
        return False

    def remove(self, *, reservation: ABCReservationMixin):
        """
        Removes the specified reservation.

        Args:
            reservation: reservation to remove
        """
        if reservation.get_reservation_id() in self.reservations:
            self.reservations.pop(reservation.get_reservation_id())

    def remove_by_rid(self, *, rid: ID):
        """
        Removes the specified reservation.

        Args:
            rid: reservation id of reservation to remove
        """
        if rid in self.reservations:
            self.reservations.pop(rid)

    def size(self) -> int:
        """
        Returns the number of reservations in the set.

        Returns:
            the size of the reservation set
        """
        return len(self.reservations.keys())

    def __eq__(self, other):
        if not isinstance(other, ReservationSet):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.reservations == other.reservations

    def clone(self):
        result = ReservationSet()
        result.reservations = self.reservations.copy()
        return result

    def values(self) -> list:
        result = []
        for r in self.reservations.values():
            result.append(r)
        return result
