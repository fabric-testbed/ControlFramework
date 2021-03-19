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

from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_controller_callback_proxy import ABCControllerCallbackProxy
    from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
    from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
    from fabric_cf.actor.security.auth_token import AuthToken


class ABCAuthorityProxy(ABCBrokerProxy):
    """
    IAuthorityProxy represents the proxy interface to an actor acting in the authority role.
    """
    @abstractmethod
    def prepare_close(self, *, reservation: ABCControllerReservation, callback: ABCControllerCallbackProxy,
                      caller: AuthToken) -> ABCRPCRequestState:

        """
        Prepare a close
        @params reservation: reservation
        @params callback: callback
        @params caller: caller
        """

    @abstractmethod
    def prepare_extend_lease(self, *, reservation: ABCControllerReservation, callback: ABCControllerCallbackProxy,
                             caller: AuthToken) -> ABCRPCRequestState:
        """
        Prepare an extend lease
        @params reservation: reservation
        @params callback: callback
        @params caller: caller
        """

    @abstractmethod
    def prepare_modify_lease(self, *, reservation: ABCControllerReservation, callback: ABCControllerCallbackProxy,
                             caller: AuthToken) -> ABCRPCRequestState:
        """
        Prepare a modify lease
        @params reservation: reservation
        @params callback: callback
        @params caller: caller
        """

    @abstractmethod
    def prepare_redeem(self, *, reservation: ABCControllerReservation, callback: ABCControllerCallbackProxy,
                       caller: AuthToken) -> ABCRPCRequestState:
        """
        Prepare a redeem
        @params reservation: reservation
        @params callback: callback
        @params caller: caller
        """
