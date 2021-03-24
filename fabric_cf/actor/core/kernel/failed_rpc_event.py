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

from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.util.rpc_exception import RPCException
from fabric_cf.actor.core.apis.abc_actor_event import ABCActorEvent

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC


class FailedRPCEvent(ABCActorEvent):
    """
    Represents Failed RPC Event
    """
    def __init__(self, *, actor: ABCActorMixin, failed: FailedRPC):
        self.actor = actor
        self.failed = failed

    def process(self):
        """
        Process Failed RPC Event
        """
        self.actor.get_logger().debug("Processing failed RPC ({})".format(self.failed.get_request_type()))
        if self.failed.get_request_type() == RPCRequestType.Query:
            handler = self.failed.get_request().get_handler()
            if handler is not None:
                handler.handle(status=self.failed.get_error(), result=None)
            else:
                self.actor.get_logger().warning("Query failed, but no handler is present")
        elif self.failed.get_request_type() == RPCRequestType.QueryResult:
            if self.failed.get_retry_count() < 10:
                from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
                RPCManagerSingleton.get().retry(request=self.failed.get_request())
            else:
                self.actor.get_logger().warning("Cannot send query response. Giving up after 10 retries {}".format(
                    self.failed.get_error()))
        elif self.failed.get_request_type() == RPCRequestType.ClaimDelegation or \
            self.failed.get_request_type() == RPCRequestType.ReclaimDelegation or \
            self.failed.get_request_type() == RPCRequestType.Ticket or \
            self.failed.get_request_type() == RPCRequestType.ExtendTicket or \
            self.failed.get_request_type() == RPCRequestType.Relinquish or \
            self.failed.get_request_type() == RPCRequestType.UpdateTicket or \
            self.failed.get_request_type() == RPCRequestType.Redeem or \
            self.failed.get_request_type() == RPCRequestType.ExtendLease or \
            self.failed.get_request_type() == RPCRequestType.ModifyLease:
            self.actor.get_logger().debug("Failed RPC for {}: in FailedRPCEvent.process()".format(
                self.failed.get_request_type()))
        elif self.failed.get_request_type() == RPCRequestType.Close:
            rid = self.failed.get_reservation_id()
            if rid is None:
                self.actor.get_logger().error("Could not process failed RPC: reservation id is null")
            self.actor.handle_failed_rpc(rid=rid, rpc=self.failed)
        else:
            raise RPCException(message="Unsupported RPC request type: {}".format(
                self.failed.get_request().get_request_type()))
