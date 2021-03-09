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
from fabric_cf.actor.core.util.resource_type import ResourceType


class CountsPerType:
    """
    Inner class representing counts for a given resource type.
    """
    def __init__(self, *, resource_type: ResourceType):
        self.resource_type = resource_type
        self.active = 0
        self.pending = 0
        self.expired = 0
        self.failed = 0
        self.closed = 0


class ResourceCount:
    """
    ResourceCount is a utility class used to count reservations in a ReservationSet based on the reservation
    state and resource type. An instance of this class is used as an argument to the "tally" methods in ReservationSet.
    Note: no internal locking.
    """
    def __init__(self):
        # Counts per resource type. <String, CountsPerType>
        self.map = {}

    def get_counts(self, *, resource_type: ResourceType) -> CountsPerType:
        """
        Returns the count entry for the given resource type.

        Args:
            resource_type: resource type
        Returns:
            count entry for the given resource type
        """
        return self.map.get(resource_type.get_type(), None)

    def get_or_create_counts(self, *, resource_type: ResourceType) -> CountsPerType:
        """
        Returns or creates a new count entry for the given resource type.

        Args:
            resource_type: resource type
        Returns:
            count entry for the given resource type
        """
        result = self.map.get(resource_type.get_type(), None)

        if result is None:
            result = CountsPerType(resource_type=resource_type)
            self.map[resource_type.get_type()] = result

        return result

    def count_active(self, *, resource_type: ResourceType) -> int:
        """
        Returns the number of active units from the given resource type.

        Args:
            resource_type: resource type
        Returns:
            number of active units
        """
        counts = self.get_counts(resource_type=resource_type)
        if counts is not None:
            return counts.active
        else:
            return 0

    def count_close(self, *, resource_type: ResourceType) -> int:
        """
        Returns the number of closed units from the given resource type.

        Args:
            resource_type: resource type
        Returns:
            number of closed units
        """
        counts = self.get_counts(resource_type=resource_type)
        if counts is not None:
            return counts.closed
        else:
            return 0

    def count_expired(self, *, resource_type: ResourceType) -> int:
        """
        Returns the number of expired units from the given resource type.

        Args:
            resource_type: resource type
        Returns:
            number of expired units
        """
        counts = self.get_counts(resource_type=resource_type)
        if counts is not None:
            return counts.expired
        else:
            return 0

    def count_failed(self, *, resource_type: ResourceType) -> int:
        """
        Returns the number of failed units from the given resource type.

        Args:
            resource_type: resource type
        Returns:
            number of failed units
        """
        counts = self.get_counts(resource_type=resource_type)
        if counts is not None:
            return counts.failed
        else:
            return 0

    def count_pending(self, *, resource_type: ResourceType) -> int:
        """
        Returns the number of pending units from the given resource type.

        Args:
            resource_type: resource type
        Returns:
            number of pending units
        """
        counts = self.get_counts(resource_type=resource_type)
        if counts is not None:
            return counts.pending
        else:
            return 0

    def tally_active(self, *, resource_type: ResourceType, count: int):
        """
        Increments with count the internal counter for active units of the specified type.

        Args:
            resource_type: resource type
            count: count
        """
        counts = self.get_or_create_counts(resource_type=resource_type)
        counts.active += count

    def tally_close(self, *, resource_type: ResourceType, count: int):
        """
        Increments with count the internal counter for closed units of the specified type.

        Args:
            resource_type: resource type
            count: count
        """
        counts = self.get_or_create_counts(resource_type=resource_type)
        counts.closed += count

    def tally_expired(self, *, resource_type: ResourceType, count: int):
        """
        Increments with count the internal counter for expired units of the specified type.

        Args:
            resource_type: resource type
            count: count
        """
        counts = self.get_or_create_counts(resource_type=resource_type)
        counts.expired += count

    def tally_failed(self, *, resource_type: ResourceType, count: int):
        """
        Increments with count the internal counter for failed units of the specified type.

        Args:
            resource_type: resource type
            count: count
        """
        counts = self.get_or_create_counts(resource_type=resource_type)
        counts.failed += count

    def tally_pending(self, *, resource_type: ResourceType, count: int):
        """
        Increments with count the internal counter for pending units of the specified type.

        Args:
            resource_type: resource type
            count: count
        """
        counts = self.get_or_create_counts(resource_type=resource_type)
        counts.pending += count
