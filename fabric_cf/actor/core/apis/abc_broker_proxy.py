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

from fabric_cf.actor.core.apis.abc_actor_proxy import ABCActorProxy
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_client_callback_proxy import ABCClientCallbackProxy
    from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.security.auth_token import AuthToken


class ABCBrokerProxy(ABCActorProxy):
    """
    IBrokerProxy represents the proxy interface to an actor acting in the broker role.
    """

    @abstractmethod
    def prepare_ticket(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy,
                       caller: AuthToken) -> ABCRPCRequestState:
        """
        Prepare a ticket
        @params reservation: reservation
        @params callback: callback
        @params caller: caller
        """

    @abstractmethod
    def prepare_extend_ticket(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy,
                              caller: AuthToken) -> ABCRPCRequestState:
        """
        Prepare an extend ticket
        @params reservation: reservation
        @params callback: callback
        @params caller: caller
        """

    @abstractmethod
    def prepare_relinquish(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy,
                           caller: AuthToken) -> ABCRPCRequestState:
        """
        Prepare a relinquish
        @params reservation: reservation
        @params callback: callback
        @params caller: caller
        """

    @abstractmethod
    def prepare_claim_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy,
                                 caller: AuthToken, id_token: str = None) -> ABCRPCRequestState:
        """
        Prepare a claim delegation
        @params delegation: delegation
        @params callback: callback
        @params caller: caller
        """

    @abstractmethod
    def prepare_reclaim_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy,
                                   caller: AuthToken, id_token: str = None) -> ABCRPCRequestState:
        """
        Prepare a reclaim delegation
        @params delegation: delegation
        @params callback: callback
        @params caller: caller
        """
