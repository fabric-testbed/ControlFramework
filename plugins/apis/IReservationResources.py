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


from plugins.util.ResourceCount import ResourceCount
import datetime


class IReservationResources:
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
    def __init__(self):
        return

    def count(self, rc: ResourceCount, time: datetime):
        """
        Counts the number of resources in the reservation relative to the specified time.
        The ResourceCount object is updated with the count of active, pending, expired, failed, etc. units.
        Note: "just a hint" unless the kernel lock is held.

        Args:
            rc: holder for counts
            time: time instance
        """
        return

    def get_approved_resources(self):
        """
        Returns the resources approved for this reservation by the last policy decision. If the policy has never
        made a decision about the reservation, this method will return None.

        Returns:
            resources last approved for the reservation. None if no resources have ever been approved.
        """
        return None

    def get_approved_term(self):
        """
        Returns the term approved for this reservation by the last policy decision. If the policy has never
        made a decision about the reservation, this method will return None.

        Returns:
            term last approved for the reservation. None if no resources have ever been approved.
        """
        return None

    def get_approved_type(self):
        """
        Returns the resource type approved for this reservation by the last policy decision. If the policy has never
        made a decision about the reservation, this method will return None.

        Returns:
            resource type last approved for the reservation. None if no resources have ever been approved.
        """
        return None

    def get_approved_units(self):
        """
        Returns the number of units approved for this reservation by the last policy decision. If the policy has never
        made a decision about the reservation, this method will return None.

        Returns:
            number of units last approved for the reservation. None if no resources have ever been approved.
        """
        return 0

    def get_leased_abstract_units(self):
        """
        Returns number of abstract units leased by the reservation. If the reservation does not represent leased
        resources or has not yet leased any resources, e.g., holds only a ticket, the method will return 0

        Returns:
            number of abstract units leased
        """
        return 0

    def get_leased_units(self):
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
        return 0

    def get_previous_resources(self):
        """
        Returns the resources represented by/allocated to the reservation at the time before the last update.
        Can be None

        Returns:
            resource represented by the reservation at the time before the last update. Can be None.
        """
        return None

    def get_previous_term(self):
        """
        Returns the previously allocated term for the reservation.
        Can be None

        Returns:
            previously allocated term. None if reservation has not yet been extended
        """
        return None

    def get_requested_resources(self):
        """
        Returns the resources requested for the reservation. If the kernel has not yet issued the resource request
        this method will return None

        Returns:
            resources requested for the reservation. null if no request has been made yet.
        """
        return None

    def get_requested_term(self):
        """
        Returns the last requested term. If the kernel has not yet issued the resource request
        this method will return None

        Returns:
            last requested term. null if no request has been made yet.
        """
        return None

    def get_request_type(self):
        """
        Returns the requested resource type.

        Returns:
            requested resource type
        """
        return None

    def get_requested_units(self):
        """
        Returns the number of requested units. If no units have yet been requested, the method will return 0.

        Returns:
            number of requested units
        """
        return None

    def get_resources(self):
        """
        Returns the resources represented by/allocated to the reservation. If no resources have yet been allocated to
        the reservation, this method will return None

        Returns:
            resources represented by the reservation. None if no resources have been allocated to the reservation.
        """
        return None

    def get_term(self):
        """
        Returns the currently allocated term for the reservation. If no resources have yet been allocated to
        the reservation, this method will return None

        Returns:
            currently allocated term. None if no resources have been allocated to the reservation.
        """
        return None

    def get_units(self):
        """
        Returns the currently assigned resource units. If the reservation has not yet been assigned units,
        the method will return 0. For extended reservations this method will return the number of units from the
        latest extension. In case of tickets, this number may represent resources in the future and may be different
        from the number of units from before the extension.

        Returns:
            number of assigned/allocated units
        """
        return None

    def get_units(self, when: datetime):
        """
        Returns the number of units assigned to the reservation at the specific time instance.
        If the time instance falls outside of the reservation term, this method will return 0.

        Args:
            when: time
        Returns:
            number of units
        """
        return None

    def is_approved(self):
        """
        Checks if the policy has made a decision for the reservation.

        Returns:
            true if the policy has made a decision for the reservation
        """
        return False

    def set_approved(self):
        """
        Indicates that the policy completed making its decisions about the reservation. Sets the approved flag.
        This flag is used when performing unit counts. If the flag is set, the number of units in the
        approved resource set will be counted as pending. Failure to set this flag will only affect resource counts.
        """
        return

    def set_approved(self, term, approved_resources):
        """
        Sets the term and resources approved for the reservation. This method should be called by the actor policy
        after it determines the resources and term for the reservation. The method also sets the approved flag.

        Args:
            term: term the policy approved
            approved_resources: resources the policy approved
        """
        return

    def set_approved_resources(self, approved_resources):
        """
        Sets the resources approved for the reservation. This method should be called by the actor policy after
        it determines the resources for the reservation. This method will not set the approved flag.
        Once all approval decisions are complete, setApproved()} must be invoked.

        Args:
            approved_resources: resources the policy approved
        """
        return

    def set_approved_term(self, term):
        """
        Sets the term approved for the reservation. This method should be called by the actor policy after it
        determines the term for the reservation. This method will not set the approved flag.
        Once all approval decisions are complete, setApproved() must be invoked.

        Args:
            term: approved term
        """
        return
