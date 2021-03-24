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
from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_proxy import ABCProxy
from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
from fabric_cf.actor.core.apis.abc_response_handler import ABCResponseHandler
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType


class RPCRequest:
    """
    Represents a RPC request being sent across Kafka
    """
    def __init__(self, *, request: ABCRPCRequestState, actor: ABCActorMixin, proxy: ABCProxy,
                 sequence: int = None, handler: ABCResponseHandler = None, reservation: ABCReservationMixin = None,
                 delegation: ABCDelegation = None):
        self.request = request
        self.actor = actor
        self.proxy = proxy
        self.reservation = reservation
        self.delegation = delegation
        self.sequence = sequence
        self.handler = handler
        self.retry_count = 0
        self.timer = None

    def get_actor(self) -> ABCActorMixin:
        """
        Get actor
        @return actor
        """
        return self.actor

    def get_delegation(self) -> ABCDelegation:
        """
        Get delegation
        @return delegation
        """
        return self.delegation

    def get_reservation(self) -> ABCReservationMixin:
        """
        Get Reservation
        @return reservation
        """
        return self.reservation

    def get_handler(self) -> ABCResponseHandler:
        """
        Get Response Handler
        @return response handler
        """
        return self.handler

    def get_request_type(self) -> RPCRequestType:
        """
        Get Request Type
        @return request type
        """
        return self.request.get_type()

    def cancel_timer(self):
        """
        Cancel a timer if started
        """
        if self.timer is not None:
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            GlobalsSingleton.get().timer_scheduler.cancel(self.timer)
