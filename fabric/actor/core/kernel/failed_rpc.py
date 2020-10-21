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
    from fabric.actor.core.apis.i_rpc_response_handler import IRPCResponseHandler
    from fabric.actor.core.kernel.rpc_request import RPCRequest
    from fabric.actor.core.kernel.rpc_request_type import RPCRequestType
    from fabric.actor.core.util.id import ID
    from fabric.actor.core.util.rpc_exception import RPCException, RPCError
    from fabric.actor.security.auth_token import AuthToken


class FailedRPC:
    def __init__(self, *, e: RPCException = None, request_type: RPCRequestType = None, rid: ID=None,
                 auth: AuthToken=None, request: RPCRequest=None):
        if e is None:
            raise Exception("error cannot be None")

        if rid is None and request is None:
            raise Exception("Both request and rid cannot be None")

        self.error = e
        self.request_type = request_type
        self.rid = rid
        self.remote_auth = auth
        self.request = request

        if request is not None:
            if self.rid is None:
                if request.get_reservation() is not None:
                    self.rid = request.get_reservation().get_reservation_id()
                elif request.get_delegation() is not None:
                    self.rid = request.get_delegation().get_delegation_id()
            if self.request_type is None:
                self.request_type = request.get_request_type()

    def is_reservation_rpc(self) -> bool:
        return self.rid is not None

    def has_request(self) -> bool:
        return self.request is not None

    def get_request(self) -> RPCRequest:
        return self.request

    def get_error(self) -> Exception:
        return self.error

    def get_request_type(self) -> RPCRequestType:
        return self.request_type

    def get_reservation_id(self) -> ID:
        return self.rid

    def get_retry_count(self) -> int:
        return self.request.retry_count

    def get_handler(self) -> IRPCResponseHandler:
        if self.request is None:
            return None
        return self.request.get_handler()

    def get_remote_auth(self) -> AuthToken:
        return self.remote_auth

    def get_error_type(self) -> RPCError:
        return self.error.get_error_type()