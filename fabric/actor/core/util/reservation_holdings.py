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
import  bisect
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.util.reservation_set import ReservationSet
from fabric.actor.core.util.resource_type import ResourceType
from fabric.actor.core.util.utils import binary_search


class ReservationWrapper:
    """
    Internal class to represent a reservation.
    """
    def __init__(self, reservation: IReservation, start: int, end: int):
        self.start = start
        self.end = end
        self.reservation = reservation

    def __eq__(self, other):
        if not isinstance(other, ReservationWrapper):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return (self.start == other.start and
                self.end == other.end and
                self.reservation.get_reservation_id() == other.reservation.get_reservation_id())

    def __lt__(self, other):
        if not isinstance(other, ReservationWrapper):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.end < other.end and self.reservation.get_reservation_id() < self.reservation.get_reservation_id()

    def __gt__(self, other):
        if not isinstance(other, ReservationWrapper):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.end > other.end and self.reservation.get_reservation_id() > self.reservation.get_reservation_id()


class ReservationHoldings:
    """
    This class maintains a collection of reservations. Each reservation is
    associated with a validity interval. The class allows to answer intersection
    queries: what reservations are valid at a given time instance.

    As time goes by, the class can be purged from irrelevant reservation records
    by invoking tick(). Purging is strongly recommended as it reduces the cost of intersection queries.

    An attempt has been made to optimize the cost of using this data structure.
    Inserts are O(log(n)). Queries, however, may take between O(log(n)) and O(n).
    """
    def __init__(self):
        # List of reservation wrappers sorted by increasing end time.
        self.list = []
        # All reservations stored in this collection.
        self.reservation_set = ReservationSet()
        # Map of reservations to ReservationWrappers. Needed when removing a reservation.
        self.map = {}

    def add_reservation(self, reservation: IReservation, start: int, end: int):
        """
        Adds a reservation to the collection for the specified period of time.
        The interval is closed on both sides.
        @params reservation : reservation to add
        @params start : start time
        @params end : end time
        """
        # If this is an extended reservation, we may already have it in the
        # list (with potentially different start and end times). Remove the
        # previous entry if this is the case.
        my_start = start
        entry = None
        if reservation.get_reservation_id() in self.map :
            entry = self.map[reservation.get_reservation_id()]
            if entry is not None:
                if start - entry.end <= 1:
                    raise Exception("Invalid")
                my_start = entry.start
                self.remove_reservation(reservation)

        entry = ReservationWrapper(reservation, my_start, end)
        self.add_to_list(entry)
        self.reservation_set.add(reservation)
        self.map[reservation.get_reservation_id()] = entry

    def add_to_list(self, entry: ReservationWrapper):
        """
        Adds the entry to the linked list. Maintains the list in sorted order.
        Cost: O(log(n)).
        @params entry : entry to add
        """
        bisect.insort(self.list, entry)

    def clear(self):
        """
        Clears the collection.
        """
        self.map.clear()
        self.list.clear()
        self.reservation_set.clear()

    def get_reservations(self, time: int = None, rtype: ResourceType = None) -> ReservationSet:
        """
        Performs an intersection query: returns all reservations from the
        specified resource type present in the collection that are active at the
        specified time instance.
        @params time : time instance
        @params rtype : resource type
        @returns reservations set containing active reservations
        """
        if time is None and rtype is None:
            return self.reservation_set

        result = ReservationSet()
        key = ReservationWrapper(None, time, time)

        # Find the location of key in the list.
        index = binary_search(self.list, key)
        if index < 0:
            index = -index -1

        # Scan the upper part of the list. We need to scan the whole list.
        i = index
        count = self.size()
        while i < count:
            entry = self.list[i]

            if rtype is None or rtype == entry.reservation.getType():
                if entry.start <= time <= entry.end:
                    result.add(entry.reservation)
            i += 1

        # Scan the lower part of the list until no further intersections are possible
        i = index - 1
        while i >= 0:
            entry = self.list[i]
            if entry.end < time:
                break

            if entry.start <= time:
                if rtype is None or entry.reservation.getType() == rtype:
                    result.add(entry.reservation)
            i -= 1

        return result

    def remove_from_list(self, entry: ReservationWrapper):
        """
        Removes a reservation from the collection.
        @params reservation : reservation to remove
        """
        index = binary_search(self.list, entry)

        if index >= 0:
            self.list.pop(index)

    def remove_reservation(self, reservation: IReservation):
        """
        Removes a reservation from the collection.
        @params reservation : reservation to remove
        """
        if reservation.get_reservation_id() in self.map:
            entry = self.map[reservation.get_reservation_id()]
            self.map.pop(reservation.get_reservation_id())
            self.reservation_set.remove(reservation)
            self.remove_from_list(entry)

    def size(self) -> int:
        """
        Returns the size of the collection.
        @returns size of the collection
        """
        return self.reservation_set.size()

    def tick(self, time: int):
        """
        Removes all reservations that have end time not after the given cycle.
        @params time : time
        """
        while True:
            if len(self.list) > 0 :
                entry = self.list[0]
                if entry.end <= time:
                    self.list.remove(entry)
                    self.reservation_set.remove(entry.reservation)
                    self.map.pop(entry.reservation.get_reservation_id())
                else:
                    break
            else:
                break
