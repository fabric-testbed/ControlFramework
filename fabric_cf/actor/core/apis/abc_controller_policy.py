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
from fabric_cf.actor.core.apis.abc_client_policy import ABCClientPolicy

if TYPE_CHECKING:
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.core.time.term import Term
    from fabric_cf.actor.core.util.reservation_set import ReservationSet


class ABCControllerPolicy(ABCClientPolicy):
    """
    IControllerPolicy defines the policy interface for an actor acting in the orchestrator role.
    """

    @abstractmethod
    def get_redeeming(self, *, cycle: int) -> ReservationSet:
        """
        Returns a set of reservations that must be redeemed.
        @params cycle the current cycle
        @returns reservations to redeem
        """

    @abstractmethod
    def lease_satisfies(self, *, request_resources: ResourceSet, actual_resources: ResourceSet, requested_term: Term,
                        actual_term: Term):
        """
        Checks if the resources and term received in a lease are in compliance
        with what was initially requested. The policy can prevent the application
        of the incoming update if it disagrees with it.

        @param request_resources
                   resources requested from site authority
        @param actual_resources
                   resources received from site authority
        @param requested_term
                   term requested from site authority
        @param actual_term
                   term received from site authority
        @raises Exception in case of error
        """
