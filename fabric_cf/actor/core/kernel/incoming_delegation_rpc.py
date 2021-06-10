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

from fabric_cf.actor.core.kernel.incoming_rpc import IncomingRPC

if TYPE_CHECKING:
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
    from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
    from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
    from fabric_cf.actor.core.util.update_data import UpdateData
    from fabric_cf.actor.security.auth_token import AuthToken


class IncomingDelegationRPC(IncomingRPC):
    """
    Represents Incoming RPC message carrying a delegation
    """
    def __init__(self, *, message_id: ID, request_type: RPCRequestType, delegation: ABCDelegation,
                 callback: ABCCallbackProxy = None, update_data: UpdateData = None, caller: AuthToken = None):
        super().__init__(message_id=message_id, request_type=request_type, callback=callback, caller=caller)
        self.delegation = delegation
        self.update_data = update_data

    #def get_delegation(self) -> ABCDelegation:
    def get(self):
        """
        Get delegation
        @return delegation
        """
        return self.delegation

    def get_update_data(self) -> UpdateData:
        """
        Get Update Data
        @return update data
        """
        return self.update_data

    def __str__(self):
        return "{} rid={}".format(super().__str__(), self.delegation)
