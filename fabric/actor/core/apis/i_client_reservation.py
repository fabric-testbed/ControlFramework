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
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from fabric.actor.core.apis.i_authority_proxy import IAuthorityProxy
    from fabric.actor.core.apis.i_broker_proxy import IBrokerProxy
    from fabric.actor.core.apis.i_client_callback_proxy import IClientCallbackProxy
    from fabric.actor.core.apis.i_client_policy import IClientPolicy
    from fabric.actor.core.time.term import Term
    from fabric.actor.core.kernel.sesource_set import ResourceSet
    from fabric.actor.core.util.resource_type import ResourceType

from fabric.actor.core.apis.i_reservation import IReservation


class IClientReservation(IReservation):
    """
    IClientReservation defines the reservation interface for actors acting as clients of other actors.
    """
    def get_authority(self) -> IAuthorityProxy:
        """
        Returns the authority who issued the lease for this reservation.
        @returns site authority or null
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_broker(self) -> IBrokerProxy:
        """
        Returns a proxy to the broker linked to this reservation. Can be None
        @returns broker linked to this reservation. Can be None
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_previous_ticket_term(self) -> Term:
        """
        Returns the previous ticket term.
        @returns previous ticket term
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_renew_time(self) -> int:
        """
        Returns the cached reservation renewal time. Used during recovery
        @returns cached reservation renewal time
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_suggested_resources(self) -> ResourceSet:
        """
        Returns the resources suggested to the policy for a new/extend request for the reservation.
        @returns suggested resources
        """
        raise NotImplementedError("Should have implemented this")

    def get_suggested_term(self) -> Term:
        """
        Returns the term suggested to the policy for a new/extend request for the reservation.
        @returns suggested term
        """
        raise NotImplementedError("Should have implemented this")

    def get_suggested_type(self) -> ResourceType:
        """
        Returns the most recently suggest resource type.
        @returns suggested type
        """
        raise NotImplementedError("Should have implemented this")

    def get_ticket_sequence_in(self) -> int:
        """
        Returns the reservation sequence number for incoming ticket/extend ticket messages.
        @returns reservation sequence number for incoming ticket/extend ticket messages
        """
        raise NotImplementedError("Should have implemented this")

    def get_ticket_sequence_out(self) -> int:
        """
        Returns the reservation sequence number for outgoing ticket/extend ticket messages.
        @returns reservation sequence number for outgoing ticket/extend ticket messages
        """
        raise NotImplementedError("Should have implemented this")

    def get_ticket_term(self) -> Term:
        """
        Returns the current ticket term. Note that getTerm will return the currently active term.
        This can be either the ticket term or the lease term.
        @returns current ticket term
        """
        raise NotImplementedError("Should have implemented this")

    def is_exported(self) -> bool:
        """
        Checks if the reservation represents exported resources.
        @returns true if the reservation represents exported resources
        """
        raise NotImplementedError("Should have implemented this")

    def is_renewable(self) -> bool:
        """
        Checks if the reservation is renewable.
        @returns true if the reservation is renewable
        """
        raise NotImplementedError("Should have implemented this")

    def set_broker(self, broker: IBrokerProxy) -> bool:
        """
        Sets the broker who will issue tickets for the reservation. This
        method can be called only for reservations in the Nascent state.
        @params broker broker request tickets from
        @raises Exception if the reservation is in the wrong state
        """
        raise NotImplementedError("Should have implemented this")

    def set_exported(self, exported: bool):
        """
        Sets the exported flag.
        @params exported: flag value
        """
        raise NotImplementedError("Should have implemented this")

    def set_renewable(self, renewable: bool):
        """
        Sets the renewable flag.
        @params renewable: flag value
        """
        raise NotImplementedError("Should have implemented this")

    def get_renewable(self) -> bool:
        """
        Gets the renewable flag.
        """
        raise NotImplementedError("Should have implemented this")

    def set_renew_time(self, time: int):
        """
        Caches the reservation renewal time. This information is used to simplify recovery.
        @params time: reservation renewal time
        """
        raise NotImplementedError("Should have implemented this")

    def set_suggested(self, term: Term, resources: ResourceSet):
        """
        Sets the term and resources suggested to the policy for a new/extend request for the reservation.
        @params term : suggested term
        @params resources : suggested resources
        """
        raise NotImplementedError("Should have implemented this")

    def set_suggested_term(self, term: Term):
        """
        Sets the term suggested to the policy for a new/extend request for the reservation.
        @params term : suggested term
        """
        raise NotImplementedError("Should have implemented this")

    def set_suggested_resources(self, resources: ResourceSet):
        """
        Sets the resources suggested to the policy for a new/extend request for the reservation.
        @params resources : suggested resources
        """
        raise NotImplementedError("Should have implemented this")

    def set_ticket_sequence_in(self, sequence: int):
        """
        Sets the reservation sequence number for incoming ticket/extend ticket messages.
        @params sequence : sequence number
        """
        raise NotImplementedError("Should have implemented this")

    def set_ticket_sequence_out(self, sequence: int):
        """
        Sets the reservation sequence number for outgoing ticket/extend ticket messages.
        @params sequence : sequence number
        """
        raise NotImplementedError("Should have implemented this")

    def get_update_notices(self) -> str:
        """
        Returns a string describing the reservation status.
        @returns status string
        """
        raise NotImplementedError("Should have implemented this")

    def set_policy(self, policy: IClientPolicy):
        """
        Sets the policy associated with this reservation.
        @params policy : policy
        """
        raise NotImplementedError("Should have implemented this")

    def get_client_callback_proxy(self) -> IClientCallbackProxy:
        """
        Returns the client callback proxy for this reservation.
        @returns IClientCallbackProxy
        """
        raise NotImplementedError("Should have implemented this")
