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

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_callback_proxy import ICallbackProxy
    from fabric.actor.core.apis.i_rpc_response_handler import IRPCResponseHandler
    from fabric.actor.core.kernel.rpc_request_type import RPCRequestType
    from fabric.actor.security.auth_token import AuthToken


class IncomingRPC:
    def __init__(self, message_id: str, request_type: RPCRequestType, callback: ICallbackProxy, caller: AuthToken):
        self.request_type = request_type
        self.message_id = message_id
        self.callback = callback
        self.caller = caller
        self.response_handler = None
        self.error = None
        self.request_id = None

    def get_request_type(self) -> RPCRequestType:
        return self.request_type

    def get_callback(self) -> ICallbackProxy:
        return self.callback

    def get_caller(self) -> AuthToken:
        return self.caller

    def get_message_id(self) -> str:
        return self.message_id

    def set_response_handler(self, response_handler: IRPCResponseHandler):
        self.response_handler = response_handler

    def get_response_handler(self) -> IRPCResponseHandler:
        return self.response_handler

    def set_error(self, error: Exception):
        self.error = error

    def get_error(self) -> Exception:
        return self.error

    def set_request_id(self, request_id: str):
        self.request_id = request_id

    def get_request_id(self) -> str:
        return self.request_id

    def __str__(self):
        return "MessageID={} requestType={} caller={}:{}".format(self.message_id, self.request_type,
                                                                 self.caller.get_name(), self.caller.getGuid())
