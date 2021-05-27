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

from abc import abstractmethod
from typing import TYPE_CHECKING

from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_server_policy import ABCServerPolicy

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet


class ABCAuthorityPolicy(ABCServerPolicy):
    """
    IAuthorityPolicy defines the policy interface for an actor acting in the authority role.
    """
    @abstractmethod
    def eject(self, *, resources: ResourceSet):
        """
        Ejects resources from the inventory. Resource ejection is unconditional:
        the policy must remove the specified concrete nodes from its inventory.
        Any nodes that reside on ejected hosts should be marked as failed. The
        policy should take no action to destroy those nodes.

        @params resources : resources to be ejected

        @raises Exception in case of error
        """

    @abstractmethod
    def bind(self, *, reservation: ABCAuthorityReservation) -> bool:
        """
        Handles a requests to allocate resources for a ticketed reservation. The
        requested resources can be obtained by calling
        reservation.get_requested_resources(). The requested lease
        term can be obtained by calling
        reservation.get_requested_term(). Properties specific to the
        lease protocol can be obtained by calling
        reservation.get_requested_resources().get_configuration_properties()

        If the policy completed processing this request, the functions should
        return true. If no further intervention is required, e.g., approval by an
        administrator, the policy should also clear the bid_pending
        flag.

        The policy may decide to defer the request for a later time. In this case
        the function should return false and the bid_pending flag
        should remain unchanged.

        This method may be invoked multiple times for a given reservation, i.e.,
        if the policy delays the allocation, the system will continue invoking
        this method at later times until the policy completes processing this
        request.

        @params reservation: reservation to allocate resources for.

        @returns true, if the request has been fulfilled, false, if the allocation
                of resources will be delayed until a later time.
        @raises Exception in case of error
        """

    @abstractmethod
    def assign(self, *, cycle: int):
        """
        Assigns leases to incoming tickets. This method is called by the policy
        once per cycle. The method should determine whether to perform resource
        allocation on the given cycle and what requests to consider in that
        process.

        @params cycle: the cycle the authority is making assignment for

        @raises Exception in case of error
        """

    @abstractmethod
    def correct_deficit(self, *, reservation: ABCAuthorityReservation):
        """
        Informs the policy that a reservation has a deficit and allows the policy
        to correct the deficit. The policy can attempt to correct the deficit,
        fail the reservation, or indicate that the reservation should be sent
        back to the client with the deficit.
        See {@link IAuthorityReservation#setSendWithDeficit(boolean)}

        @params reservation: reservation with deficit

        @raises Exception in case of error
        """

    @abstractmethod
    def extend_authority(self, *, reservation: ABCAuthorityReservation) -> bool:
        """
        Handles a requests to extend the allocation of previously allocated
        resources. The requested resources can be obtained by calling
        reservation.get_requested_resources(). The requested lease
        term can be obtained by calling
        reservation.get_requested_term(). Properties specific to the
        lease protocol can be obtained by calling
        reservation.get_requested_resources().get_configuration_properties()

        If the policy completed processing this request, the functions should
        return true. If no further intervention is required, e.g., approval by an
        administrator, the policy should also clear the bid_pending
        flag.

        The policy may decide to defer the request for a later time. In this case
        the function should return false and the bid_pending flag
        should remain unchanged.

        This method may be invoked multiple times for a given reservation, i.e.,
        if the policy delays the allocation, the system will continue invoking
        this method at later times until the policy completes processing this
        request.


        @params reservation: reservation to allocate resources for.

        @returns true, if the request has been fulfilled, false, if the allocation
                of resources will be delayed until a later time.
        @raises Exception in case of error
        """

    @abstractmethod
    def release(self, *, resources: ResourceSet):
        """
        Releases allocated resources that are no longer in use. The set may
        represent active as well as failed resources. The policy must decide what
        to do with the released resources. Resources that have been properly
        closed/terminated are safe to be considered free for future use. Failed
        resources, however, are problematic. If the policy has no information
        about the cause of the failure and does not posses the means to recover
        the failure it should not consider the resources as free. In such cases,
        and administrator may need to correct the failure manually. When/if the
        failure is corrected and the resources are safe to be reused, the
        administrator will issue a call to freed(ResourceSet), which is
        used to free resources unconditionally.


        @params resources: the resource set to be released

        @raises Exception in case of error
        """

    @abstractmethod
    def freed(self, *, resources: ResourceSet):
        """
        Informs the policy that a set of allocated resources can be considered as
        free. Most probably these resources represent previously failed
        resources, which have been repaired by an administrator. The policy must
        update its data structures to reflect the fact that the incoming
        resources are no longer in use. The policy should disregard any state
        information that individual resource units may contain.

        @params resources: resources

        @raises Exception in case of error
        """

    @abstractmethod
    def failed(self, *, resources: ResourceSet):
        """
        Informs the policy that inventory resources have failed. This is a new
        method, which may change in the future.

        @params resources: set of failed inventory resources
        """

    @abstractmethod
    def recovered(self, *, resources: ResourceSet):
        """
        Informs the policy that previously failed inventory nodes have been
        recovered and now are ready to use. This is a new method and may change
        in the future.

        @params resources: set of recovered inventory resources
        """

    @abstractmethod
    def reclaim(self, *, delegation: ABCDelegation):
        """
        Reclaim a delegation
        @param delegation delegation to be reclaimed
        """