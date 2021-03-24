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

import traceback
from typing import TYPE_CHECKING
from fabric_cf.actor.core.apis.abc_timer_task import ABCTimerTask
from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
from fabric_cf.actor.core.kernel.failed_rpc_event import FailedRPCEvent
from fabric_cf.actor.core.util.rpc_exception import RPCException, RPCError

if TYPE_CHECKING:
    from fabric_cf.actor.core.kernel.rpc_request import RPCRequest


class QueryTimeout(ABCTimerTask):
    """
    Query timeout
    """
    def __init__(self, *, req: RPCRequest):
        self.req = req

    def execute(self):
        """
        Process a query timeout
        """
        try:
            from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
            pending = RPCManagerSingleton.get().remove_pending_request(guid=self.req.request.get_message_id())
            if pending is not None:
                self.req.actor.get_logger().debug("Query timeout. Responding with FailedRPC RPC={}".format(self.req))
                failed = FailedRPC(e=RPCException(message="Timeout while waiting for query response",
                                                  error=RPCError.Timeout), request=self.req)
                self.req.actor.queue_event(incoming=FailedRPCEvent(actor=self.req.actor, failed=failed))
            else:
                self.req.actor.get_logger().debug("Query timeout. Query already completed RPC={}".format(self.req))
        except Exception as e:
            self.req.actor.get_logger().error("Query timeout. RPC={} e: {}".format(self.req.reservation, e))
            self.req.actor.get_logger().error(traceback.format_exc())
