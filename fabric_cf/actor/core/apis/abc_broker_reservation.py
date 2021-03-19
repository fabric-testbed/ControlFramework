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

from fabric_cf.actor.core.apis.abc_server_reservation import ABCServerReservation

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_authority_proxy import ABCAuthorityProxy
    from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation


class ABCBrokerReservation(ABCServerReservation):
    """
    IBrokerReservation defines the reservation interface for brokers processing requests for resources.
    """
    @abstractmethod
    def get_authority(self) -> ABCAuthorityProxy:
        """
        Returns a proxy to the authority in control of the resources represented by the reservation.

        @returns authority proxy
        """

    @abstractmethod
    def get_source(self) -> ABCDelegation:
        """
        Returns source for this delegation. For optional use by policy
        to track where it filled this reservation from, e.g., for use on
        extends.

        @returns the source delegation
        """

    @abstractmethod
    def is_closed_in_priming(self) -> bool:
        """
        Checks if the reservation was closed while it was in the Priming
        state. Reservations closed in the priming state have a resources field
        that does not accurately represent the last allocation. This method is
        intended for use by policy classes when processing the closed() event.
        If the policy class does not keep track what resources it last
        allocated to a given reservation, the policy class must then use this
        method to determine where the information about allocated resources for
        the reservation is stored. If this method returns true, the last
        allocation, before the current update, will be in
        get_previous_resources(), and the current update that was
        applied to the reservation will be in get_approved_resources()
        """

    @abstractmethod
    def set_source(self, *, source: ABCDelegation):
        """
        Sets the source for this reservation. For optional use by policy
        to track where it filled this reservation from, e.g., for use on
        extends.

        @params source : the source delegation.
        """
