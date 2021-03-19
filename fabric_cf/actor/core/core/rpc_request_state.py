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
from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.security.auth_token import AuthToken


class RPCRequestState(ABCRPCRequestState):
    """
    Implements RPC Request which carries state of an incoming/outgoing message
    """
    def __init__(self):
        self.caller = None
        self.rtype = None
        self.message_id = ID()
        self.id_token = None

    def get_caller(self) -> AuthToken:
        return self.caller

    def set_caller(self, *, caller: AuthToken):
        self.caller = caller

    def get_type(self) -> RPCRequestType:
        return self.rtype

    def set_type(self, *, rtype: RPCRequestType):
        self.rtype = rtype

    def get_message_id(self) -> ID:
        return self.message_id

    def get_id_token(self) -> str:
        """
        Returns id token
        @return  id token
        """
        return self.id_token

    def set_id_token(self, id_token: str):
        """
        Set id token
        @param id_token id token
        """
        self.id_token = id_token
