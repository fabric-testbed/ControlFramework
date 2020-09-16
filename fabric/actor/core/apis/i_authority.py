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

from fabric.actor.core.apis.i_authority_public import IAuthorityPublic
from fabric.actor.core.apis.i_server_actor import IServerActor

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
    from fabric.actor.core.apis.i_controller_callback_proxy import IControllerCallbackProxy
    from fabric.actor.core.apis.i_reservation import IReservation
    from fabric.actor.core.kernel.resource_set import ResourceSet
    from fabric.actor.security.auth_token import AuthToken


class IAuthority(IServerActor, IAuthorityPublic):
    """
    IAuthority defines the interface for an actor acting in the authority role.
    """
    def available(self, resources: ResourceSet):
        """
        Informs the actor that the following resources are available for
        allocation.

        @params resources resources

        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def donate(self, resources: ResourceSet):
        """
        Accepts concrete resources to be used for allocation of client
        requests.

        @params resources resource set representing resources to be used for
               allocation

        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def eject(self, resources: ResourceSet):
        """
        Ejects the specified resources from the inventory.

        @params resources resources

        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def extend_lease(self, reservation:IAuthorityReservation, caller: AuthToken):
        """
        Processes an extend lease request for the reservation.

        @params reservation reservation representing a request for a lease
               extension
        @params caller : caller

        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def modify_lease(self, reservation:IAuthorityReservation, caller: AuthToken):
        """
        Processes an modify lease request for the reservation.

        @params reservation :  reservation representing a request for a lease
               modification
        @params caller : caller

        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def freed(self, resources: ResourceSet):
        """
        Informs the actor that the given resources are no longer in use
        and can be considered as free, regardless of the state of the
        individual units.

        @params resources resource set representing freed resources

        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def redeem(self, reservation:IReservation, callback: IControllerCallbackProxy, caller: AuthToken):
        """
        Processes a redeem request for the reservation.

        @params reservation reservation representing a request for a new lease
        @params callback callback
        @params caller caller
        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def unavailable(self, resources: ResourceSet) -> int:
        """
        Informs the actor that previously donated resources are no
        longer available for allocation.

        @param resources resources

        @returns 

        @raises Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")