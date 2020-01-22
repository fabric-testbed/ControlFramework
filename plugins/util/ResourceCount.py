#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################
from plugins.util.ResourceType import ResourceType


class CountsPerType:
    """
    Inner class representing counts for a given resource type.
    """
    def __init__(self, resource_type: ResourceType):
        self.resource_type = resource_type
        self.active = 0
        self.pending = 0
        self.expired = 0
        self.failed = 0
        self.closed = 0


class ResourceCount:
    """
    ResourceCount</code> is a utility class used to count reservations in a ReservationSet based on the reservation
    state and resource type. An instance of this class is used as an argument to the "tally" methods in ReservationSet.
    Note: no internal locking.
    """
    def __init__(self):
        self.map = {}

    def get_counts(self, resource_type: ResourceType):
        """
        Returns the count entry for the given resource type.

        Args:
            resource_type: resource type
        Returns:
            count entry for the given resource type
        """
        return self.map.get(resource_type.getType())

    def get_or_create_counts(self, resource_type: ResourceType):
        """
        Returns or creates a new count entry for the given resource type.

        Args:
            resource_type: resource type
        Returns:
            count entry for the given resource type
        """
        result = self.map.get(resource_type.getType())

        if result is None:
            result = CountsPerType(resource_type.getType())
            map.put(resource_type.getType(), result)

        return result

    def count_active(self, resource_type: ResourceType):
        """
        Returns the number of active units from the given resource type.

        Args:
            resource_type: resource type
        Returns:
            number of active units
        """
        counts = self.getCounts(resource_type)
        if counts is not None:
            return counts.active
        else:
            return 0

    def count_close(self, resource_type: ResourceType):
        """
        Returns the number of closed units from the given resource type.

        Args:
            resource_type: resource type
        Returns:
            number of closed units
        """
        counts = self.getCounts(resource_type)
        if counts is not None:
            return counts.closed
        else:
            return 0

    def count_expired(self, resource_type: ResourceType):
        """
        Returns the number of expired units from the given resource type.

        Args:
            resource_type: resource type
        Returns:
            number of expired units
        """
        counts = self.getCounts(resource_type)
        if counts is not None:
            return counts.expired
        else:
            return 0

    def count_failed(self, resource_type: ResourceType):
        """
        Returns the number of failed units from the given resource type.

        Args:
            resource_type: resource type
        Returns:
            number of failed units
        """
        counts = self.getCounts(resource_type)
        if counts is not None:
            return counts.failed
        else:
            return 0

    def count_pending(self, resource_type: ResourceType):
        """
        Returns the number of pending units from the given resource type.

        Args:
            resource_type: resource type
        Returns:
            number of pending units
        """
        counts = self.getCounts(resource_type)
        if counts is not None:
            return counts.pending
        else:
            return 0

    def tally_active(self, resource_type: ResourceType, count: int):
        """
        Increments with count the internal counter for active units of the specified type.

        Args:
            resource_type: resource type
            count: count
        """
        counts = self.get_or_create_counts(resource_type)
        counts.active += count

    def tally_close(self, resource_type: ResourceType, count: int):
        """
        Increments with count the internal counter for closed units of the specified type.

        Args:
            resource_type: resource type
            count: count
        """
        counts = self.get_or_create_counts(resource_type)
        counts.closed += count

    def tally_expired(self, resource_type: ResourceType, count: int):
        """
        Increments with count the internal counter for expired units of the specified type.

        Args:
            resource_type: resource type
            count: count
        """
        counts = self.get_or_create_counts(resource_type)
        counts.expired += count

    def tally_failed(self, resource_type: ResourceType, count: int):
        """
        Increments with count the internal counter for failed units of the specified type.

        Args:
            resource_type: resource type
            count: count
        """
        counts = self.get_or_create_counts(resource_type)
        counts.failed += count

    def tally_pending(self, resource_type: ResourceType, count: int):
        """
        Increments with count the internal counter for pending units of the specified type.

        Args:
            resource_type: resource type
            count: count
        """
        counts = self.get_or_create_counts(resource_type)
        counts.pending += count