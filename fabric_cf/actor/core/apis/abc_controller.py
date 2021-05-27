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

from fabric_cf.actor.core.apis.abc_client_actor import ABCClientActor

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
    from fabric_cf.actor.core.util.reservation_set import ReservationSet
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin


class ABCController(ABCClientActor):
    """
    IController defines the interface for a actor acting in the orchestrator role.
    """
    @abstractmethod
    def extend_lease(self, *, reservation: ABCControllerReservation = None, rset: ReservationSet = None):
        """
        Issues an extend lease request for the given reservation. Note:
        the reservation must have already been registered with the actor.

        All exceptions are caught and logged but no exception is propagated. No
        information will be delivered to indicate that some failure has taken
        place, e.g., failure to communicate with a broker. Inspect the state of
        individual reservations to determine whether/what failures have taken
        place.

        @param reservation reservation to be redeemed
        @param rset set of reservations to extend the lease for

        @throws Exception in case of error
        """

    @abstractmethod
    def redeem(self, *, reservation: ABCControllerReservation):
        """
        Issues a redeem request for the given reservation. Note: the
        reservation must have already been registered with the actor.

        @param reservation reservation to be redeemed

        @throws Exception in case of error
        """

    @abstractmethod
    def redeem_reservations(self, *, rset: ReservationSet):
        """
        Issues a redeem request for every reservation in the set. All
        exceptions are caught and logged but no exception is propagated. No
        information will be delivered to indicate that some failure has taken
        place, e.g., failure to communicate with a broker. Inspect the state of
        individual reservations to determine whether/what failures have taken
        place.

        @param rset set of reservations to redeem
        """
