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

from fabric_cf.actor.core.apis.abc_delegation import DelegationState
from fabric_cf.actor.core.apis.abc_timer_task import ABCTimerTask

if TYPE_CHECKING:
    from fabric_cf.actor.core.kernel.rpc_request import RPCRequest


class ClaimTimeout(ABCTimerTask):
    """
    Claim Timeout
    """
    def __init__(self, *, req: RPCRequest):
        self.req = req

    def execute(self):
        """
        Process a claim timeout
        """
        self.req.actor.get_logger().debug("Claim timeout. Delegation= {}".format(self.req.delegation))
        self.req.actor.get_logger().error("Failing delegation {} due to expired claim timeout".format(
            self.req.delegation))
        self.req.actor.fail_delegation(did=self.req.get_delegation().get_delegation_id(),
                                       message="Timeout during claim. Please remove the delegation and retry later")


class ReclaimTimeout(ABCTimerTask):
    """
    Reclaim timeout
    """
    def __init__(self, *, req: RPCRequest):
        self.req = req

    def execute(self):
        """
        Process a reclaim timeout
        """
        self.req.actor.get_logger().debug("Reclaim timeout. delegation= {}".format(self.req.delegation))
        if self.req.delegation.get_state() == DelegationState.Delegated:
            self.req.actor.get_logger().error("Failing delegation {} due to expired reclaim timeout".format(
                self.req.reservation))
            self.req.actor.fail_delegation(did=self.req.get_delegation().get_delegation_id(),
                                           message="Timeout during claim. Please remove the delegation and retry later")
        else:
            self.req.actor.get_logger().debug("Reclaim has already completed")
