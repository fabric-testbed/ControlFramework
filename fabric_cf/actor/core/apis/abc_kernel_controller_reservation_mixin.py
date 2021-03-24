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
from abc import abstractmethod

from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
from fabric_cf.actor.core.apis.abc_kernel_client_reservation_mixin import ABCKernelClientReservationMixin


class ABCKernelControllerReservationMixin(ABCKernelClientReservationMixin, ABCControllerReservation):
    """
    Kernel-level interface for orchestrator reservations.
    """

    @abstractmethod
    def prepare_join(self):
        """
        Prepares the reservation for processing a join operation.
        Invoked internally before processing joins on an arriving initial lease
        for a reservation. This gives subclasses an opportunity to manipulate
        the property list or other attributes prior to the join. void.
        @throws Exception in case of error
        """

    @abstractmethod
    def prepare_redeem(self):
        """
        Prepares the reservation for processing a redeem operation.
        Invoked internally before any initial redeem operation on a
        reservation. This gives subclasses an opportunity to manipulate the
        property list or other attributes prior to the redeem.
        @throws Exception in case of error
        """

    @abstractmethod
    def validate_redeem(self):
        """
        Validates an outgoing redeem request.

        @throws Exception if validation fails
        """
