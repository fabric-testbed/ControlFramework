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


from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
    from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
    from fabric_cf.actor.core.util.update_data import UpdateData
    from fabric_cf.actor.security.auth_token import AuthToken


class ABCClientCallbackProxy(ABCCallbackProxy):
    @abstractmethod
    def prepare_update_ticket(self, *, reservation: ABCBrokerReservation, update_data: UpdateData,
                              callback: ABCCallbackProxy, caller: AuthToken) -> ABCRPCRequestState:
        """
        Prepare an update ticket
        @params reservation: reservation
        @params update_data: update data
        @params callback: callback
        @params caller: caller
        """

    @abstractmethod
    def prepare_update_delegation(self, *, delegation: ABCDelegation, update_data: UpdateData,
                                  callback: ABCCallbackProxy, caller: AuthToken) -> ABCRPCRequestState:
        """
        Prepare an update delegation
        @params delegation: delegation
        @params update_data: update data
        @params callback: callback
        @params caller: caller
        """
