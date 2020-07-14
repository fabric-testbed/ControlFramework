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

from fabric.actor.core.apis.IActor import IActor
from fabric.actor.core.apis.IServerPublic import IServerPublic

if TYPE_CHECKING:
    from fabric.actor.core.apis.IBrokerReservation import IBrokerReservation
    from fabric.actor.core.apis.IClientCallbackProxy import IClientCallbackProxy
    from fabric.actor.core.apis.IClientReservation import IClientReservation
    from fabric.actor.core.apis.IReservation import IReservation
    from fabric.actor.core.apis.ISlice import ISlice
    from fabric.actor.core.time.Term import Term
    from fabric.actor.core.kernel.ResourceSet import ResourceSet
    from fabric.actor.core.util.Client import Client
    from fabric.actor.core.util.ID import ID
    from fabric.actor.security.AuthToken import AuthToken


class IServerActor(IActor, IServerPublic):
    """
    IServerActor defines the common functionality for actors
    acting as servers for other actors (brokers and site authorities).
    """
    def donate_reservation(self, reservation: IClientReservation):
        """
        Accepts ticketed resources to be used for allocation of client
        requests.

        @params reservation: reservation representing resources to be used for
               allocation

        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def export(self, reservation: IBrokerReservation, resources: ResourceSet, term: Term, client: AuthToken) -> ID:
        """
        Exports the specified resources for the given period of time to
        the client.
        @params reservation: reservation describing resources to export
        @params resources: resources to export
        @params term: period the export will be valid
        @params client: identity of the client resources will be exported to

        @return reservation id

        @throws Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def register_client_slice(self, slice:ISlice):
        """
        Registers a new client slice.
        @params slice: client slice
        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def register_client(self, client: Client):
        """
        Registers the specified client.
        @param client client to register
        @throws Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def unregister_client(self, guid:ID):
        """
        Unregisters the specified client.
        @params guid : client guid
        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def get_client(self, guid: ID) -> Client:
        """
        Get a client specified by GUID
        @params guid: guid
        @returns client: specified by GUID
        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def claim(self, reservation: IReservation, callback: IClientCallbackProxy, caller: AuthToken):
        """
        Processes an incoming claim request.
        @params reservation: reservation
        @params callback : callback
        @params caller: caller
        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def ticket(self, reservation: IReservation, callback: IClientCallbackProxy, caller: AuthToken):
        """
        Processes an incoming ticket request.
        @params reservation: reservation
        @params callback : callback
        @params caller: caller
        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def extend_ticket(self, reservation: IReservation, caller: AuthToken):
        """
        Processes an incoming extend_ticket request.
        @params reservation: reservation
        @params caller: caller
        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def relinquish(self, reservation: IReservation, caller: AuthToken):
        """
        Processes an incoming relinquish request.
        @params reservation: reservation
        @params caller: caller
        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

