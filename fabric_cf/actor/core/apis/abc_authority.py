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

from fabric_cf.actor.core.apis.abc_server_actor import ABCServerActor

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
    from fabric_cf.actor.core.apis.abc_controller_callback_proxy import ABCControllerCallbackProxy
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.security.auth_token import AuthToken


class ABCAuthority(ABCServerActor):
    """
    IAuthority defines the interface for an actor acting in the authority role.
    """

    @abstractmethod
    def eject(self, *, resources: ResourceSet):
        """
        Ejects the specified resources from the inventory.

        @param resources resources

        @raises Exception in case of error
        """

    @abstractmethod
    def extend_lease(self, *, reservation: ABCAuthorityReservation, caller: AuthToken = None,
                     callback: ABCControllerCallbackProxy = None,):
        """
        Processes an extend lease request for the reservation.

        @param reservation reservation representing a request for a lease
               extension
        @param caller : caller
        @param callback : callback

        @raises Exception in case of error
        """

    @abstractmethod
    def modify_lease(self, *, reservation: ABCAuthorityReservation, caller: AuthToken,
                     callback: ABCControllerCallbackProxy = None,):
        """
        Processes an modify lease request for the reservation.

        @param reservation :  reservation representing a request for a lease
               modification
        @param caller : caller
        @param callback : callback

        @raises Exception in case of error
        """

    @abstractmethod
    def freed(self, *, resources: ResourceSet):
        """
        Informs the actor that the given resources are no longer in use
        and can be considered as free, regardless of the state of the
        individual units.

        @param resources resource set representing freed resources

        @raises Exception in case of error
        """

    @abstractmethod
    def redeem(self, *, reservation: ABCReservationMixin, callback: ABCControllerCallbackProxy = None,
               caller: AuthToken = None):
        """
        Processes a redeem request for the reservation.

        @param reservation reservation representing a request for a new lease
        @param callback callback
        @param caller caller
        @raises Exception in case of error
        """

    @abstractmethod
    def close_by_caller(self, *, reservation: ABCReservationMixin, caller: AuthToken):
        """
        Closes the reservation.

        @params reservation:  the reservation
        @params caller : the slice owner

        @raises Exception in case of error
        """
