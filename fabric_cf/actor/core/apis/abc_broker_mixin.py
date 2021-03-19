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
from fabric_cf.actor.core.apis.abc_server_actor import ABCServerActor

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation


class ABCBrokerMixin(ABCClientActor, ABCServerActor):
    """
    IBroker defines the interface for a actor acting in the broker role
    """
    @abstractmethod
    def extend_ticket_broker(self, *, reservation: ABCBrokerReservation):
        """
        Processes an extend ticket request for the reservation.

        @param reservation reservation representing a request for a ticket
               extension

        @throws Exception in case of error
        """

    @abstractmethod
    def ticket_broker(self, *, reservation: ABCBrokerReservation):
        """
        Processes a ticket request for the reservation.

        @param reservation reservation representing a request for a new ticket

        @throws Exception in case of error
        """
