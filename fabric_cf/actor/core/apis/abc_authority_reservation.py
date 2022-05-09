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

from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
from fabric_cf.actor.core.apis.abc_server_reservation import ABCServerReservation
if TYPE_CHECKING:
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet


class ABCAuthorityReservation(ABCServerReservation):
    """
    IAuthorityReservation defines the reservation interface for authorities(AM) processing requests for resources.
    """
    @abstractmethod
    def get_deficit(self):
        """
        Returns the number of concrete units this reservation is short or in excess of.
        @returns number of concrete units this reservation is short or in excess of
        """

    @abstractmethod
    def get_ticket(self) -> ResourceSet:
        """
        Returns the ticket backing the reservation.
        @returns ticket
        """

    @abstractmethod
    def set_send_with_deficit(self, *, value: bool):
        """
        Sets the "send with deficit" flag.
        @params value : true, reservations with no pending operations but with a
                 deficit will be sent back without attempting to fix the deficit,
                false - if a deficit exists, the system will attempt to fix it.
        """

    @abstractmethod
    def set_broker_callback(self, *, broker_callback: ABCCallbackProxy):
        """
        Set the Broker Callback Proxy
        @param broker_callback Broker callback
        """

    @abstractmethod
    def get_broker_callback(self) -> ABCCallbackProxy:
        """
        Get the Broker Callback Proxy
        """

    @abstractmethod
    def prepare_extend_lease(self):
        """
        Prepare for an incoming extend request on this existing
        reservation. Note: unlocked

        @throws Exception thrown if request is rejected (e.g., ticket not valid)
        """

    @abstractmethod
    def prepare_modify_lease(self):
        """
        Prepare for an incoming modify request on this existing
        reservation. Note: unlocked

        @throws Exception thrown if request is rejected (e.g., ticket not valid)
        """
