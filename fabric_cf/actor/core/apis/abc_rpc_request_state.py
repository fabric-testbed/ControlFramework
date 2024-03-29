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

from abc import abstractmethod, ABC
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.security.auth_token import AuthToken


class ABCRPCRequestState(ABC):
    @abstractmethod
    def get_caller(self) -> AuthToken:
        """
        Return the caller
        @return caller
        """

    @abstractmethod
    def set_caller(self, *, caller: AuthToken):
        """
        Set the caller
        @param caller caller
        """

    @abstractmethod
    def get_type(self) -> RPCRequestType:
        """
        Return RPC request type
        @return rpc request type
        """

    @abstractmethod
    def set_type(self, *, rtype: RPCRequestType):
        """
        Set Request Type
        @param rtype: rpc request type
        """

    @abstractmethod
    def get_message_id(self) -> ID:
        """
        Return Message Id
        @return message id
        """
