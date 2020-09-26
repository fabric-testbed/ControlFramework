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

from fabric.actor.core.apis.i_policy import IPolicy

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
    from fabric.actor.core.apis.i_client_reservation import IClientReservation
    from fabric.actor.core.apis.i_reservation import IReservation


class IServerPolicy(IPolicy):
    """
    IServerPolicy defines the policy interface for an actor acting
    as a server for another actor (broker or a site authority).
    """

    @abstractmethod
    def allocate(self, *, cycle: int):
        """
        Allocates resources to all clients who have requested them. This method
        is called by the policy once per cycle. The method should determine
        whether to perform resource allocation on the given cycle and what
        requests to consider in that process.
        
        @params cycle : the cycle for this allocation
        
        @raises Exception in case of error
        """

    @abstractmethod
    def bind(self, *, reservation: IReservation) -> bool:
        """
        Handles an incoming request to allocate resources and issue a ticket for
        the reservation. The requested resources can be obtained by calling
        reservation.get_requested_resources(). The requested lease
        term can be obtained by calling
        reservation.get_requested_term(). Properties specific to the
        allocation protocol can be obtained by calling
        reservation.get_requested_resources().get_request_properties()
        If the policy completed processing this request, the functions should
        return true. If no further intervention is required, e.g., approval by an
        administrator, the policy should also clear the bid_pending flag.

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
    def donate_reservation(self, *, reservation: IClientReservation):
        """
        Accepts ticketed resources to be used for allocation of client requests.
        The policy should add the resources represented by this reservation to
        its inventory.
        Note: This method will be invoked only for resources not directly
        requested by the policy. For example, exported resources claimed manually
        by an administrator. The policy is itself responsible to "donate"
        resources to its inventory when resources it requests become available.

        @param reservation : reservation representing resources to be used for allocation
        
        @raises Exception in case of error
        """

    @abstractmethod
    def extend_broker(self, *, reservation:IBrokerReservation) -> bool:
        """
        Handles an incoming request to extend previously allocated resources and
        issue a ticket for the reservation. The requested resources can be
        obtained by calling reservation.getRequestedResources()
        Properties specific to the allocation protocol can be obtained by calling
        reservation.get_requested_resources().get_request_properties().
        The requested lease term can be obtained by calling
        reservation.get_requested_term(). The new term must extend the
        currently allocated term.

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

        While the policy is free to modify the term as it wishes, care must be
        taken that the client reservation is not closed before the extension is
        actually granted.

        @param reservation : reservation to allocate resources for.
        
        @returns true, if the request has been fulfilled, false, if the allocation
                of resources will be delayed until a later time.
        @raises Exception in case of error
        """
