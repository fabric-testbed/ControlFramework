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

from fim.graph.abc_property_graph import ABCPropertyGraph

from fabric_cf.actor.core.apis.i_actor import IActor
from fabric_cf.actor.core.apis.i_delegation import IDelegation
from fabric_cf.actor.core.apis.i_server_public import IServerPublic

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_client_callback_proxy import IClientCallbackProxy
    from fabric_cf.actor.core.apis.i_client_reservation import IClientReservation
    from fabric_cf.actor.core.apis.i_reservation import IReservation
    from fabric_cf.actor.core.apis.i_slice import ISlice
    from fabric_cf.actor.core.util.client import Client
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.security.auth_token import AuthToken


class IServerActor(IActor, IServerPublic):
    """
    IServerActor defines the common functionality for actors
    acting as servers for other actors (brokers and site authorities).
    """
    @abstractmethod
    def donate_reservation(self, *, reservation: IClientReservation):
        """
        Accepts ticketed resources to be used for allocation of client
        requests.

        @params reservation: reservation representing resources to be used for
               allocation

        @raises Exception in case of error
        """

    @abstractmethod
    def register_client_slice(self, *, slice_obj: ISlice):
        """
        Registers a new client slice.
        @params slice_obj: client slice
        @raises Exception in case of error
        """

    @abstractmethod
    def register_client(self, *, client: Client):
        """
        Registers the specified client.
        @param client client to register
        @throws Exception in case of error
        """

    @abstractmethod
    def unregister_client(self, *, guid: ID):
        """
        Unregisters the specified client.
        @params guid : client guid
        @raises Exception in case of error
        """

    @abstractmethod
    def get_client(self, *, guid: ID) -> Client:
        """
        Get a client specified by GUID
        @params guid: guid
        @returns client: specified by GUID
        @raises Exception in case of error
        """

    @abstractmethod
    def claim_delegation(self, *, delegation: IDelegation, callback: IClientCallbackProxy, caller: AuthToken,
                         id_token: str = None):
        """
        Processes an incoming claim request.
        @params delegation: delegation
        @params callback : callback
        @params caller: caller
        @param id_token id token
        @raises Exception in case of error
        """

    @abstractmethod
    def reclaim_delegation(self, *, delegation: IDelegation, callback: IClientCallbackProxy, caller: AuthToken,
                           id_token: str = None):
        """
        Processes an incoming claim request.
        @params delegation: delegation
        @params callback : callback
        @params caller: caller
        @param id_token id token
        @raises Exception in case of error
        """

    @abstractmethod
    def ticket(self, *, reservation: IReservation, callback: IClientCallbackProxy, caller: AuthToken):
        """
        Processes an incoming ticket request.
        @params reservation: reservation
        @params callback : callback
        @params caller: caller
        @raises Exception in case of error
        """

    @abstractmethod
    def extend_ticket(self, *, reservation: IReservation, caller: AuthToken):
        """
        Processes an incoming extend_ticket request.
        @params reservation: reservation
        @params caller: caller
        @raises Exception in case of error
        """

    @abstractmethod
    def relinquish(self, *, reservation: IReservation, caller: AuthToken):
        """
        Processes an incoming relinquish request.
        @params reservation: reservation
        @params caller: caller
        @raises Exception in case of error
        """

    @abstractmethod
    def advertise(self, *, delegation: ABCPropertyGraph, client: AuthToken) -> ID:
        """
        Exports the resources described by the delegation to the client.
        @param delegation delegation describing resources to export
        @param client identity of the client resources will be exported to
        @raises Exception in case of error
        """
