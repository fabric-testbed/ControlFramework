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


if TYPE_CHECKING:
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
    from fabric_cf.actor.core.apis.abc_response_handler import ABCResponseHandler
    from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
    from fabric_cf.actor.security.auth_token import AuthToken


class IncomingRPC:
    """
    Represents an incoming RPC message
    """
    def __init__(self, *, message_id: ID, request_type: RPCRequestType,
                 callback: ABCCallbackProxy, caller: AuthToken,
                 id_token: str = None):
        self.request_type = request_type
        self.message_id = message_id
        self.callback = callback
        self.caller = caller
        self.response_handler = None
        self.error = None
        self.request_id = None
        self.id_token = id_token

    def get_request_type(self) -> RPCRequestType:
        """
        Get request type
        @return request type
        """
        return self.request_type

    def get_callback(self) -> ABCCallbackProxy:
        """
        Get callback
        @return callback
        """
        return self.callback

    def get_caller(self) -> AuthToken:
        """
        Get caller
        @return caller
        """
        return self.caller

    def get_message_id(self) -> ID:
        """
        Get message_id
        @return message_id
        """
        return self.message_id

    def set_response_handler(self, *, response_handler: ABCResponseHandler):
        """
        Set response_handler
        @param response_handler response_handler
        """
        self.response_handler = response_handler

    def get_response_handler(self) -> ABCResponseHandler:
        """
        Get response_handler
        @return response_handler
        """
        return self.response_handler

    def set_error(self, *, error: Exception):
        """
        Set error
        @param error error
        """
        self.error = error

    def get_error(self) -> Exception:
        """
        Get error
        @return error
        """
        return self.error

    def set_request_id(self, *, request_id: ID):
        """
        Set request id
        @param request_id request id
        """
        self.request_id = request_id

    def get_request_id(self) -> str:
        """
        Get request_id
        @return request_id
        """
        return self.request_id

    def get_id_token(self) -> str:
        """
        Get id_token
        @return id_token
        """
        return self.id_token

    def __str__(self):
        return "MessageID={} requestType={} caller={}:{}".format(self.message_id, self.request_type,
                                                                 self.caller.get_name(), self.caller.get_guid())

    @abstractmethod
    def get(self):
        """
        Return delegation or reservation or dictionary containing the request
        """

    @abstractmethod
    def get_update_data(self):
        """
        Return updated data
        """