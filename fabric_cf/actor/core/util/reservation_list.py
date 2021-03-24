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
import bisect

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import FrameworkException
from fabric_cf.actor.core.util.reservation_set import ReservationSet


class ReservationSetWrapper:
    """
    Internal representation for all reservations associated with a cycle.
    """
    def __init__(self, *, reservation_set: ReservationSet, cycle: int):
        """
        Constructor
        @params reservation_set: Reservation set
        @params cycle: cycle
        """
        self.reservation_set = reservation_set
        self.cycle = cycle

    def __lt__(self, other):
        if not isinstance(other, ReservationSetWrapper):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.cycle < other.cycle

    def __gt__(self, other):
        if not isinstance(other, ReservationSetWrapper):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.cycle > other.cycle

    def __str__(self):
        return "{}, cycle: {}".format(self.reservation_set, self.cycle)


class ReservationList:
    """
    Maintains a list of reservations associated with a cycle number. Allows for
    efficient retrieval and removal. Groups reservations by the cycle with which
    they are associated and maintains the reservation sets using a sorted list
    and a hash table. The hash table is used for quick lookup of a given
    reservation set. The sorted list allows for efficient reclaiming of sets,
    which are no longer needed.
    Cost of operations:
    - Insert: first insert to a cycle O(log(cycles)), subsequent inserts to an
    existing cycle: 0(1)
    - Remove: 0(1)
    - Reclaim: 0(k), where k is the number of cycles to reclaim.
    """

    def __init__(self):
        """
        Constructor
        """
        self.count = 0
        # list of ReservationSetWrapper
        self.rset_wrapper_list = []
        # map of <int(cycle), ReservationSet>
        self.cycle_to_rset = {}
        # map of <reservation_id, int(cycle)>
        self.reservation_id_to_cycle = {}

    def add_reservation(self, *, reservation: ABCReservationMixin, cycle: int):
        """
        Adds a reservation associated with a given cycle.
        @params reservation:  the reservation
        @params cycle: the cycle with which to associate the reservation
        """
        if reservation is None or cycle < 0 or reservation.get_reservation_id() is None:
            raise FrameworkException(Constants.INVALID_ARGUMENT)

        if reservation.get_reservation_id() in self.reservation_id_to_cycle:
            existing_cycle = self.reservation_id_to_cycle[reservation.get_reservation_id()]
            if existing_cycle == cycle:
                return
            else:
                raise RuntimeError(
                    "Reservation: #{} is already in the list at a different cycle. Please remove it first, "
                    "before adding to a different cycle".format(reservation.get_reservation_id()))

        reservation_set = None
        if cycle in self.cycle_to_rset:
            reservation_set = self.cycle_to_rset[cycle]
        else:
            reservation_set = ReservationSet()

        if reservation_set.contains(reservation=reservation) is False:
            reservation_set.add(reservation=reservation)
            self.count += 1

        if cycle not in self.cycle_to_rset:
            self.add_to_list(reservation_set=reservation_set, cycle=cycle)
            self.cycle_to_rset[cycle] = reservation_set

        self.reservation_id_to_cycle[reservation.get_reservation_id()] = cycle

    def add_to_list(self, *, reservation_set: ReservationSet, cycle: int):
        """
        Adds an entry to the sorted list
        @params reservation_set: reservation set
        @params cycle: cycle
        """
        entry = ReservationSetWrapper(reservation_set=reservation_set, cycle=cycle)
        bisect.insort(self.rset_wrapper_list, entry)

    def get_all_reservations(self, *, cycle: int) -> ReservationSet:
        """
        Returns all reservations associated with cycles up to and including the specified cycle
        @params cycle : cycle
        @returns a set of reservations associated with the cycles up to and including specified cycle.
        Note that removing from the set will not affect the ReservationList
        """
        result = ReservationSet()
        for entry in self.rset_wrapper_list:
            if entry.cycle <= cycle:
                for reservation in entry.reservation_set.values():
                    result.add(reservation=reservation)
            else:
                break
        return result

    def get_reservations(self, *, cycle: int) -> ReservationSet:
        """
        Returns all reservations associated with the specified cycle.
        @params cycle : cycle
        @return a set of reservations associated with the specified cycle. Note
        that removing from the set will not affect the ReservationList
        """
        result = ReservationSet()

        if cycle in self.cycle_to_rset:
            result = self.cycle_to_rset[cycle].clone()

        return result

    def remove_reservation(self, *, reservation: ABCReservationMixin):
        """
        Removes a reservation from the list.
        @params reservation: reservation to remove
        """
        rid = reservation.get_reservation_id()
        if rid in self.reservation_id_to_cycle:
            cycle = self.reservation_id_to_cycle[rid]
            self.reservation_id_to_cycle.pop(rid)

            if cycle in self.cycle_to_rset:
                reservation_set = self.cycle_to_rset[cycle]
                reservation_set.remove(reservation=reservation)

                if reservation_set.size() == 0:
                    self.cycle_to_rset.pop(cycle)
                self.count -= 1

    def size(self) -> int:
        """
        Returns the number of reservations in this collection
        @returns number of reservations in the collection
        """
        return self.count

    def tick(self, *, cycle: int):
        """
        Removes reservations associated with cycles less than or equal to the given cycle
        @params cycle: cycle
        """
        while True:
            if len(self.rset_wrapper_list) > 0:
                entry = self.rset_wrapper_list[0]
                if entry.cycle <= cycle:
                    self.rset_wrapper_list.remove(entry)

                    if entry.cycle in self.cycle_to_rset:
                        self.cycle_to_rset.pop(entry.cycle)

                    self.count -= entry.reservation_set.size()

                    for reservation in entry.reservation_set.values():
                        self.reservation_id_to_cycle.pop(reservation.get_reservation_id())
                    entry.reservation_set.clear()
                else:
                    break
            else:
                break

    def print(self):
        """
        Print the list (used for tests)
        :return:
        """
        print("rw_list: {} c_to_r: {} r_to_c: {}".format(len(self.rset_wrapper_list), len(self.cycle_to_rset),
                                                         len(self.reservation_id_to_cycle)))
        for rw in self.rset_wrapper_list:
            print("{}".format(rw))

        for k, v in self.cycle_to_rset.items():
            print("{}, cycle: {}".format(v, k))

        for k, v in self.reservation_id_to_cycle.items():
            print("rid: {}, cycle: {}".format(k, v))
