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
from collections import defaultdict
from datetime import datetime, timedelta

from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.attached_components import ComponentSliver, ComponentType
from fim.slivers.capacities_labels import Capacities, FreeCapacity
from fim.slivers.network_node import NodeSliver


class TimeSlot:
    """Represents a time slot for resources, tracking capacities and components."""

    def __init__(self, end: datetime):
        """
        Initialize a TimeSlot instance with end time and empty resource capacities and components.

        :param end: The end datetime for this time slot.
        :type end: datetime
        """
        self.end = end
        self.capacities = Capacities()
        self.components = {}

    def __update_capacities(self, capacities: Capacities):
        """
        Update the capacities in this time slot.

        :param capacities: The capacities to add to this time slot.
        :type capacities: Capacities
        """
        self.capacities += capacities

    def __update_components(self, by_type: dict[ComponentType, list[ComponentSliver]]):
        """
        Update the components by type in this time slot.

        :param by_type: Dictionary with component types as keys and lists of ComponentSliver as values.
        :type by_type: dict[ComponentType, list[ComponentSliver]]
        """
        for comp_type, comps in by_type.items():
            if comp_type not in self.components:
                self.components[comp_type] = 0
            for c in comps:
                if c.get_capacities():
                    units = c.get_capacities().unit
                else:
                    units = c.get_capacity_allocations().unit
                self.components[comp_type] += units

    def add_sliver(self, sliver: BaseSliver):
        """
        Add sliver capacities and components to the current time slot.

        :param sliver: The sliver containing resource capacities and components to add.
        :type sliver: BaseSliver
        """
        if isinstance(sliver, NodeSliver):
            if sliver.capacity_allocations:
                self.__update_capacities(capacities=sliver.capacity_allocations)
            else:
                self.__update_capacities(capacities=sliver.capacities)

            if sliver.attached_components_info:
                self.__update_components(by_type=sliver.attached_components_info.by_type)

    def __str__(self):
        """
        Return a string representation of the capacities and components in this time slot.

        :return: String representation of capacities and components.
        :rtype: str
        """
        return f"Capacities: {self.capacities}, Components: {self.components}"


class ResourceTracker:
    """Tracks resource over time slots and checks availability of resources."""

    def __init__(self, cbm_node: NodeSliver):
        """
        Initialize ResourceTracker with total capacities and components from a CBM node.

        :param cbm_node: The CBM node from which to initialize capacities and components.
        :type cbm_node: NodeSliver
        """
        self.total_capacities = cbm_node.get_capacities()
        self.total_components = {}

        if cbm_node.attached_components_info:
            for comp_type, comps in cbm_node.attached_components_info.by_type.items():
                if comp_type not in self.total_components:
                    self.total_components[comp_type] = 0
                    for c in comps:
                        self.total_components[comp_type] += c.get_capacities().unit

        self.time_slots = defaultdict(TimeSlot)
        self.reservation_ids = set()

    def update(self, reservation: ReservationMng, start: datetime, end: datetime):
        """
        Update allocated resource information.

        :param reservation: Existing Reservation.
        :type reservation: ReservationMng
        :param start: Requested start datetime.
        :type start: datetime
        :param end: Requested end datetime.
        :type end: datetime
        """
        # Check if reservation has already been captured, if so skip it
        if reservation.get_reservation_id() in self.reservation_ids:
            return
        self.reservation_ids.add(reservation.get_reservation_id())

        start = start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        end = end.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        sliver_end = ActorClock.from_milliseconds(milli_seconds=reservation.get_end())
        sliver_end = sliver_end.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

        current_time = start
        while current_time < sliver_end and current_time < end:
            if current_time not in self.time_slots:
                self.time_slots[current_time] = TimeSlot(current_time)

            # Update the specific time slot with the sliver's resource usage
            self.time_slots[current_time].add_sliver(sliver=reservation.get_sliver())

            current_time += timedelta(hours=1)

    def __check_components(self, requested_sliver: NodeSliver,
                           allocated: dict[ComponentType, int]) -> bool:
        """
        Check if requested components can be fulfilled by available components.

        :param requested_sliver: The sliver with requested components.
        :type requested_sliver: NodeSliver
        :param allocated: Dictionary of available components by type.
        :type allocated: dict[ComponentType, int]
        :return: True if components can be fulfilled, False otherwise.
        :rtype: bool
        """
        if not requested_sliver.attached_components_info:
            return True
        for comp_type, comps in requested_sliver.attached_components_info.by_type.items():
            if comp_type not in self.total_components:
                return False
            elif len(comps) > (self.total_components[comp_type] - allocated.get(comp_type, 0)):
                return False
        return True

    def find_next_available(self, requested_sliver: NodeSliver, start: datetime, end: datetime,
                            duration: int) -> list[datetime]:
        """
        Find the next available time slot that can fulfill the requested sliver's capacities and components.

        :param requested_sliver: The sliver with requested capacities and components.
        :type requested_sliver: NodeSliver
        :param start: The start datetime to begin searching for availability.
        :type start: datetime
        :param end: The end datetime to stop searching.
        :type end: datetime
        :param duration: The duration in hours for which the resources are needed.
        :type duration: int
        :return: List of all possible next available time slot, or empty list if not found.
        :rtype: datetime
        """
        current_time = start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        duration_timedelta = timedelta(hours=duration)
        ret_val = []

        while current_time + duration_timedelta <= end:
            available = True
            for i in range(duration):
                time_slot_time = current_time + timedelta(hours=i)
                if time_slot_time not in self.time_slots:
                    # If there's no entry for this time slot, assume full capacity available
                    continue

                time_slot = self.time_slots[time_slot_time]
                free_capacity = FreeCapacity(total=self.total_capacities, allocated=time_slot.capacities)

                # Check if accumulated capacities are negative (means not enough capacity)
                if free_capacity.free.negative_fields():
                    available = False
                    break

                diff = free_capacity.free - requested_sliver.capacities
                if diff.negative_fields():
                    available = False
                    break

                if not self.__check_components(requested_sliver=requested_sliver, allocated=time_slot.components):
                    available = False
                    break

            if available:
                ret_val.append(current_time)

            current_time += timedelta(hours=1)

        return ret_val
