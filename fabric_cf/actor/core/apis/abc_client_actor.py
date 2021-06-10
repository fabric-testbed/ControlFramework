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

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
    from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.util.reservation_set import ReservationSet
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.security.auth_token import AuthToken


class ABCClientActor(ABCActorMixin):
    """
    IClientActor defines the common functionality for actors acting
    as clients of other actors (controllers and brokers). Every client actor
    is connected to one or more server actors. Each server actor is represented
    as a proxy object. Client actors maintain a registry of proxies to server
    actors that they are connected to. Proxies to actors acting in the broker
    role are explicitly managed. Proxies for actors acting in the site authority
    role are automatically managed as they are embedded in tickets sent from brokers.
    """

    @abstractmethod
    def add_broker(self, *, broker: ABCBrokerProxy):
        """
        Registers a broker. If this is the first broker to be registered, it is
        set as the default broker.

        @params broker broker to register
        """

    @abstractmethod
    def demand(self, *, rid: ID):
        """
        Demand a reservation
        @params reservation: reservation
        """

    @abstractmethod
    def extend_ticket_client(self, *, reservation: ABCClientReservation):
        """
        Issues a ticket extend request for the given reservation. Note: the
        reservation must have already been registered with the actor.

        @param reservation reservation to extend the ticket for
        @param rset set of reservations to extend tickets for

        @throws Exception in case of error
        """

    @abstractmethod
    def extend_tickets_client(self, *, rset: ReservationSet):
        """
        Issues a ticket extend request for the given reservation. Note: the
        reservation must have already been registered with the actor.

        @param reservation reservation to extend the ticket for
        @param rset set of reservations to extend tickets for

        @throws Exception in case of error
        """

    @abstractmethod
    def get_broker(self, *, guid: ID) -> ABCBrokerProxy:
        """
        Gets the broker proxy with the given guid

        @param guid broker guid

        @return requested broker
        """

    @abstractmethod
    def get_brokers(self) -> list:
        """
        Returns all brokers registered with the actor.

        @return an array of brokers
        """

    @abstractmethod
    def get_default_broker(self) -> ABCBrokerProxy:
        """
        Returns the default broker.

        @return the default broker
        """

    @abstractmethod
    def ticket_client(self, *, reservation: ABCClientReservation):
        """
        Issues a ticket request for the given reservation. Note: the reservation
        must have already been registered with the actor.

        All exceptions are caught and logged but no exception is propagated. No information will
        be delivered to indicate that some failure has taken place, e.g., failure
        to communicate with a broker. Inspect the state of individual
        reservations to determine whether/what failures have taken place.

        @param reservation reservation to obtain a ticket for

        @throws Exception in case of error
        """

    @abstractmethod
    def tickets_client(self, *, rset: ReservationSet):
        """
        Issues a ticket request for the given reservation. Note: the reservation
        must have already been registered with the actor.

        All exceptions are caught and logged but no exception is propagated. No information will
        be delivered to indicate that some failure has taken place, e.g., failure
        to communicate with a broker. Inspect the state of individual
        reservations to determine whether/what failures have taken place.

        @param rset set of reservations to obtain tickets for

        @throws Exception in case of error
        """

    @abstractmethod
    def modify(self, *, reservation_id: ID, modify_properties: dict):
        """
        Issue modify request for given reservation. Note: the reservation
        must have already been registered with the actor.

        @param reservation_id reservationID for the reservation to modify
        @param modify_properties property list for modify
        @throws Exception in case of error
        """

    @abstractmethod
    def claim_delegation_client(self, *, delegation_id: str = None, slice_object: ABCSlice = None,
                                broker: ABCBrokerProxy = None, id_token: str = None) -> ABCDelegation:
        """
        Claims already exported resources from the given broker. The delegation
        will be stored in the default slice.

        @param delegation_id delegation identifier of the exported delegation
        @param slice_object slice
        @param broker broker proxy
        @param id_token id token

        @returns delegation
        @raises Exception in case of failure
        """

    @abstractmethod
    def reclaim_delegation_client(self, *, delegation_id: str = None, slice_object: ABCSlice = None,
                                  broker: ABCBrokerProxy = None, id_token: str = None) -> ABCDelegation:
        """
        Reclaims already exported resources from the given broker. The delegation
        will be stored in the default slice.

        @param delegation_id delegation identifier of the exported delegation
        @param slice_object slice
        @param broker broker proxy
        @param id_token id token

        @returns delegation
        @raises Exception in case of failure
        """

    @abstractmethod
    def update_ticket(self, *, reservation: ABCReservationMixin, update_data, caller: AuthToken):
        """
        Handles an incoming ticket update.

        @param reservation reservation represented by this update. The
               reservation object will contain the ticket (if any) as well
               information about the actually allocated resources.
        @param update_data status of the remote operation.
        @param caller identity of the caller

        @throws Exception in case of error
        """

    @abstractmethod
    def update_delegation(self, *, delegation: ABCDelegation, update_data, caller: AuthToken):
        """
        Handles an incoming delegation update.

        @param delegation delegation represented by this update. The
               delegation object will contain the delegation (if any) as well
               information about the actually allocated resources.
        @param update_data status of the remote operation.
        @param caller identity of the caller

        @throws Exception in case of error
        """

    @abstractmethod
    def update_lease(self, *, reservation: ABCReservationMixin, update_data, caller: AuthToken):
        """
        Handles an incoming lease update.

        @param reservation reservation represented by this update. The
               reservation object will contain the lease (if any) as well
               information about the actually leased resources.
        @param update_data status of the remote operation.
        @param caller identity of the caller

        @throws Exception in case of error
        """