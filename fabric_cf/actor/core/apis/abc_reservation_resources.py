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
from __future__ import annotations

import datetime

from abc import abstractmethod, ABC
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fabric_cf.actor.core.time.term import Term
    from fabric_cf.actor.core.util.resource_type import ResourceType
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet


class ABCReservationResources(ABC):
    """
    IReservationResources defines the API for resources associated with a reservation.
    Each reservation has a number of resource sets associated with it:
        requested: resources that have been requested either to an upstream actor or by a downstream actor.
        approved: resources that have been approved either for making a request to an upstream actor,
                  or to be sent back to a downstream actor to satisfy its request.
        resources: the resources currently bound to the reservation
        previousResources: the previous resource set bound to the reservation
        leasedResources: the concrete resources bound to the reservation.
    """
    @abstractmethod
    def get_approved_resources(self) -> ResourceSet:
        """
        Returns the resources approved for this reservation by the last policy decision. If the policy has never
        made a decision about the reservation, this method will return None.

        Returns:
            resources last approved for the reservation. None if no resources have ever been approved.
        """

    @abstractmethod
    def get_approved_term(self) -> Term:
        """
        Returns the term approved for this reservation by the last policy decision. If the policy has never
        made a decision about the reservation, this method will return None.

        Returns:
            term last approved for the reservation. None if no resources have ever been approved.
        """

    @abstractmethod
    def get_approved_type(self) -> ResourceType:
        """
        Returns the resource type approved for this reservation by the last policy decision. If the policy has never
        made a decision about the reservation, this method will return None.

        Returns:
            resource type last approved for the reservation. None if no resources have ever been approved.
        """

    @abstractmethod
    def get_approved_units(self) -> int:
        """
        Returns the number of units approved for this reservation by the last policy decision. If the policy has never
        made a decision about the reservation, this method will return None.

        Returns:
            number of units last approved for the reservation. None if no resources have ever been approved.
        """

    @abstractmethod
    def get_leased_abstract_units(self) -> int:
        """
        Returns number of abstract units leased by the reservation. If the reservation does not represent leased
        resources or has not yet leased any resources, e.g., holds only a ticket, the method will return 0

        Returns:
            number of abstract units leased
        """

    @abstractmethod
    def get_leased_units(self) -> int:
        """
        Returns the number of concrete units leased by the reservation.
        If the reservation does not represent leased resources or has not yet leased any resources,
        e.g., holds only a ticket, the method will return 0.
        Note: This call will always return 0 for reservations that have not recreated their concrete sets, e.g.,
        reservations fetched from the database as a result of a query.
        For such reservations use #get_leased_abstract_units() or obtain the actual reservation object.

        Returns:
            number of leased units
        """

    @abstractmethod
    def get_previous_resources(self) -> ResourceSet:
        """
        Returns the resources represented by/allocated to the reservation at the time before the last update.
        Can be None

        Returns:
            resource represented by the reservation at the time before the last update. Can be None.
        """

    @abstractmethod
    def get_previous_term(self) -> Term:
        """
        Returns the previously allocated term for the reservation.
        Can be None

        Returns:
            previously allocated term. None if reservation has not yet been extended
        """

    @abstractmethod
    def get_requested_resources(self) -> ResourceSet:
        """
        Returns the resources requested for the reservation. If the kernel has not yet issued the resource request
        this method will return None

        Returns:
            resources requested for the reservation. null if no request has been made yet.
        """

    @abstractmethod
    def get_requested_term(self) -> Term:
        """
        Returns the last requested term. If the kernel has not yet issued the resource request
        this method will return None

        Returns:
            last requested term. null if no request has been made yet.
        """

    @abstractmethod
    def get_requested_type(self) -> ResourceType:
        """
        Returns the requested resource type.

        Returns:
            requested resource type
        """

    @abstractmethod
    def get_requested_units(self) -> int:
        """
        Returns the number of requested units. If no units have yet been requested, the method will return 0.

        Returns:
            number of requested units
        """

    @abstractmethod
    def get_resources(self) -> ResourceSet:
        """
        Returns the resources represented by/allocated to the reservation. If no resources have yet been allocated to
        the reservation, this method will return None

        Returns:
            resources represented by the reservation. None if no resources have been allocated to the reservation.
        """

    @abstractmethod
    def get_term(self) -> Term:
        """
        Returns the currently allocated term for the reservation. If no resources have yet been allocated to
        the reservation, this method will return None

        Returns:
            currently allocated term. None if no resources have been allocated to the reservation.
        """

    @abstractmethod
    def get_units(self, *, when: datetime = None) -> int:
        """
        Returns the number of units assigned to the reservation at the specific time instance.
        If the time instance falls outside of the reservation term, this method will return 0.

        Args:
            when: time
        Returns:
            number of units
        """

    @abstractmethod
    def is_approved(self) -> bool:
        """
        Checks if the policy has made a decision for the reservation.

        Returns:
            true if the policy has made a decision for the reservation
        """

    @abstractmethod
    def set_approved(self, *, term: Term = None, approved_resources: ResourceSet = None):
        """
        Sets the term and resources approved for the reservation. This method should be called by the actor policy
        after it determines the resources and term for the reservation. The method also sets the approved flag.

        Args:
            term: term the policy approved
            approved_resources: resources the policy approved
        """

    @abstractmethod
    def set_approved_resources(self, *, approved_resources: ResourceSet):
        """
        Sets the resources approved for the reservation. This method should be called by the actor policy after
        it determines the resources for the reservation. This method will not set the approved flag.
        Once all approval decisions are complete, setApproved()} must be invoked.

        Args:
            approved_resources: resources the policy approved
        """

    @abstractmethod
    def set_approved_term(self, *, term: Term):
        """
        Sets the term approved for the reservation. This method should be called by the actor policy after it
        determines the term for the reservation. This method will not set the approved flag.
        Once all approval decisions are complete, setApproved() must be invoked.

        Args:
            term: approved term
        """
