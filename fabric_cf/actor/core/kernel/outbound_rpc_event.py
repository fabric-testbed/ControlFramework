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
from fabric_cf.actor.core.apis.abc_event import ABCEvent

if TYPE_CHECKING:
    from fabric_cf.actor.core.kernel.rpc_request import RPCRequest
    from fabric_cf.actor.core.util.id import ID


class OutboundRPCEvent(ABCEvent):
    """
    Represents outbound RPC event
    """
    def __init__(self, *, request: RPCRequest):
        self.request = request

    def get_actor_id(self) -> ID:
        """
        Get Actor id
        @return actor id
        """
        return self.request.get_actor().get_guid()

    def get_properties(self) -> dict:
        """
        Get Properties
        @return properties
        """
        return None

    def get_request(self) -> RPCRequest:
        """
        Get request
        @return request
        """
        return self.request
