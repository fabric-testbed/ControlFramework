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
    from fabric.actor.core.kernel.rpc_request import RPCRequest

from fabric.actor.core.util.rpc_exception import RPCException
from fabric.actor.core.kernel.failed_rpc import FailedRPC
from fabric.actor.core.kernel.failed_rpc_event import FailedRPCEvent


class RPCExecutor:
    def __init__(self, *, request: RPCRequest):
        self.request = request
        from fabric.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()

    def post_exception(self, *, e: RPCException):
        try:
            self.logger.error("An error occurred while performing RPC. Error type={} {}".format(e.get_error_type(), e))
            from fabric.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
            RPCManagerSingleton.get().remove_pending_request(self.request.request.get_message_id())
            failed = FailedRPC(e=e, request=self.request)
            self.request.actor.queue_event(FailedRPCEvent(actor=self.request.actor, failed=failed))
        except Exception as e:
            self.logger.error("postException failed = {}".format(e))

    def run(self):
        self.logger.debug("Performing RPC: type={} to:{}".format(self.request.request.get_type(),
                                                                 self.request.proxy.get_name()))
        try:
            self.request.proxy.execute(request=self.request.request)
            #self.request.cancel_timer()
        except RPCException as e:
            self.post_exception(e=e)
        finally:
            self.logger.debug("Completed RPC: type= {} to: {}".format(self.request.request.get_type(),
                                                                      self.request.proxy.get_name()))
            from fabric.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
            RPCManagerSingleton.get().de_queued()
