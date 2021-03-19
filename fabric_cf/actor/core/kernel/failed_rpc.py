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
    from fabric_cf.actor.core.apis.abc_response_handler import ABCResponseHandler
    from fabric_cf.actor.core.kernel.rpc_request import RPCRequest
    from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.util.rpc_exception import RPCException, RPCError
    from fabric_cf.actor.security.auth_token import AuthToken


class FailedRPC:
    """
    Represents Failed RPC Message
    """
    def __init__(self, *, e: RPCException = None, request_type: RPCRequestType = None, rid: ID = None,
                 auth: AuthToken = None, request: RPCRequest = None):
        if e is None:
            raise RPCException(message="error cannot be None")

        if rid is None and request is None:
            raise RPCException(message="Both request and rid cannot be None")

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
        """
        Check is RPC contains a reservation
        @return true if rid is present, false otherwise
        """
        return self.rid is not None

    def has_request(self) -> bool:
        """
        Check if RPC has request
        @return true if request is present, false otherwise
        """
        return self.request is not None

    def get_request(self) -> RPCRequest:
        """
        Get Request
        @return request
        """
        return self.request

    def get_error(self) -> Exception:
        """
        Get Error
        @return error
        """
        return self.error

    def get_request_type(self) -> RPCRequestType:
        """
        Get Request Type
        @return request type
        """
        return self.request_type

    def get_reservation_id(self) -> ID:
        """
        Get Reservation id
        @return reservation id
        """
        return self.rid

    def get_retry_count(self) -> int:
        """
        Get Retry count
        @return retry count
        """
        return self.request.retry_count

    def get_handler(self) -> ABCResponseHandler:
        """
        Get handler
        @return response handler
        """
        if self.request is None:
            return None
        return self.request.get_handler()

    def get_remote_auth(self) -> AuthToken:
        """
        Get Remote Auth
        @return remote auth token
        """
        return self.remote_auth

    def get_error_type(self) -> RPCError:
        """
        Get error type
        @return error type
        """
        return self.error.get_error_type()
