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

from fabric_cf.actor.core.apis.abc_policy import ABCPolicy


if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
    from fabric_cf.actor.core.util.bids import Bids
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.core.time.term import Term
    from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation


class ABCClientPolicy(ABCPolicy):
    """
    IClientPolicy defines the policy interface for an actor acting
    as a client of another actor (broker or a orchestrator).
    """
    @abstractmethod
    def demand(self, *, reservation: ABCClientReservation):
        """
        Injects a new resource demand into the demand stream. The reservation
        must be pre-initialized with resource set, term, properties, etc. The
        policy should use this request as an indication that new resources are
        required. The exact mapping of the request to actual requests for
        resources is policy-specific. For example, the policy may choose to
        combine several reservation requests into one, split a reservation
        request onto multiple brokers, etc.

        @params reservation : reservation representing resource demand
        """

    @abstractmethod
    def formulate_bids(self, *, cycle: int) -> Bids:
        """
        Formulates bids to the upstream broker(s). The method should determine
        whether to issue bids in the current cycle. This method should consider
        the current demand, call broker(s) to obtain necessary information and
        decide how to distribute its resource demand. When deciding how to bid,
        also consider any expiring reservation and decide whether to renew and
        adjust their units. The code should only formulate the bids: the actor
        will then issue them.

        Here are some guidelines for implementing this method:
        - Determine the final demand for each resource type.
        - Obtain policy-specific information from upstream brokers.
        - Determine how to split the demand across the brokers and the
        currently renewing reservation.
        - Select candidates to request and renew, and prime them with suggested
        terms, unit counts, and brokers (for new reservations), and specify
        whether the reservation is renewable or not. Set properties as needed for
        e.g., economic bidding.
        - Return a ReservationSet of new reservations to be requested, and a
        ReservationSet of reservations to extend. The returned sets may be empty,
        but not null.

        @params cycle: The current time

        @return Two collections:
                - ticketing - set of new reservations
                - extending - set of reservations to be extended. Can be None if no action should be taken

        @raises Exception in case of error
        """

    @abstractmethod
    def ticket_satisfies(self, *, requested_resources: ResourceSet, actual_resources: ResourceSet,
                         requested_term: Term, actual_term: Term):
        """
        Checks if the resources and term received in a ticket are in compliance
        with what was initially requested. The policy can prevent the application
        of the incoming update if it disagrees with it.

        @params requested_resources: resources requested from broker
        @params actual_resources: resources received from broker
        @params requested_term: term requested from broker
        @params actual_term: term received from broker
        @raises Exception in case of error
        """

    @abstractmethod
    def update_ticket_complete(self, *, reservation: ABCClientReservation):
        """
        Notifies the policy that a ticket update operation has completed. The
        policy may use this upcall to update its internal state.

        @params reservation: reservation for which an update ticket operation has completed
        @raises Exception in case of error
        """

    @abstractmethod
    def update_delegation_complete(self, *, delegation: ABCDelegation, reclaim: bool = False):
        """
        Notifies the policy that a delegation update operation has completed. The
        policy may use this upcall to update its internal state.

        @param delegation: delegation for which an update delegation operation has completed
        @param reclaim: Indicates if delegation is being reclaimed
        @raises Exception in case of error
        """
