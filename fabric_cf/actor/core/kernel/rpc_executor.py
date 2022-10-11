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

import threading
from typing import TYPE_CHECKING

from fabric_mb.message_bus.producer import AvroProducerApi

from fabric_cf.actor.core.util.rpc_exception import RPCException
from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
from fabric_cf.actor.core.kernel.failed_rpc_event import FailedRPCEvent

if TYPE_CHECKING:
    from fabric_cf.actor.core.kernel.rpc_request import RPCRequest


class RPCExecutor:
    """
    Execute an RPC
    """
    @staticmethod
    def post_exception(request: RPCRequest, e: RPCException):
        """
        Handle any exception raised during processing
        """
        logger = request.actor.get_logger()
        try:
            logger.error("An error occurred while performing RPC. Error type={} {}".format(e.get_error_type(), e))
            from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
            RPCManagerSingleton.get().remove_pending_request(request.request.get_message_id())
            failed = FailedRPC(e=e, request=request)
            request.actor.queue_event(FailedRPCEvent(actor=request.actor, failed=failed))
        except Exception as e:
            logger.error("postException failed = {}".format(e))

    @staticmethod
    def run(request: RPCRequest, producer: AvroProducerApi):
        """
        Execute RPC
        """
        logger = request.actor.get_logger()
        logger.debug(f"Performing RPC: type={request.request.get_type()} to:{request.proxy.get_name()}")
        try:
            request.proxy.execute(request=request.request, producer=producer)
            if request.handler is None:
                if request.timer is not None:
                    logger.debug("Canceling the timer: {}".format(request.timer))
                request.cancel_timer()
        except RPCException as e:
            RPCExecutor.post_exception(request=request, e=e)
        finally:
            logger.debug("Completed RPC: type= {} to: {}".format(request.request.get_type(),
                                                                 request.proxy.get_name()))
            from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
            RPCManagerSingleton.get().de_queued()
