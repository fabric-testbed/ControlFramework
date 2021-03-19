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

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.kernel.request_types import RequestTypes

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
    from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
    from fabric_cf.actor.core.util.update_data import UpdateData


class ABCKernelReservation(ABCReservationMixin):
    """
    Kernel-level reservation interface.
    """

    @abstractmethod
    def set_slice(self, *, slice_object: ABCSlice):
        """
        Sets the slice the reservation belongs to.

        @param slice_object slice the reservation belongs to
        """

    @abstractmethod
    def can_redeem(self) -> bool:
        """
        Checks if the reservation can be redeemed at the current time.

        @return true if the reservation's current state allows it to be redeemed
        """

    @abstractmethod
    def can_renew(self) -> bool:
        """
        Checks if this reservation can be renewed at the current time.

        @return true if the reservation's current state allows it to be renewed
        """

    @abstractmethod
    def close(self):
        """
        Closes the reservation. Locked with the kernel lock.
        """

    @abstractmethod
    def extend_lease(self):
        """
        Extends the reservation.

        @throws Exception in case of error
        """

    @abstractmethod
    def modify_lease(self):
        """
        Modifies the reservation.

        @throws Exception in case of error
        """

    @abstractmethod
    def extend_ticket(self, *, actor: ABCActorMixin):
        """
        Extends the ticket.

        @param actor actor

        @throws Exception in case of error
        """

    @abstractmethod
    def get_kernel_slice(self):
        """
        Returns the kernel slice.

        @return kernel slice
        """

    @abstractmethod
    def handle_duplicate_request(self, *, operation: RequestTypes):
        """
        Handles a duplicate request.

        @param operation operation type code
        @throws Exception in case of error
        """

    @abstractmethod
    def prepare(self, *, callback: ABCCallbackProxy, logger):
        """
        Prepares for a ticket request on a new reservation object.

        @param callback callback object
        @param logger for diagnostic logging
        @throws Exception in case of error
        """

    @abstractmethod
    def prepare_probe(self):
        """
        Prepares a reservation probe.

        @throws Exception in case of error
        """

    @abstractmethod
    def probe_pending(self):
        """
        Probe a reservation with a pending request. On server, if the
        operation completed, handle it and generate an update. If no pending
        request completed then do nothing.

        @throws Exception in case of error
        """

    @abstractmethod
    def reserve(self, *, policy: ABCPolicy):
        """
        Reserve resources: ticket() initiate or request, or redeem()
        request. New reservation.

        @param policy the mapper for the reservation

        @throws Exception in case of error
        """

    @abstractmethod
    def service_claim(self):
        """
        Finishes processing claim.
        @throws Exception in case of error
        """

    @abstractmethod
    def service_reclaim(self):
        """
        Finishes processing reclaim.
        @throws Exception in case of error
        """

    @abstractmethod
    def service_close(self):
        """
        Finishes processing close.
        @throws Exception in case of error
        """

    @abstractmethod
    def service_extend_lease(self):
        """
        Finishes processing extend lease.
        @throws Exception in case of error
        """

    @abstractmethod
    def service_modify_lease(self):
        """
        Finishes processing modify lease.
        @throws Exception in case of error
        """

    @abstractmethod
    def service_extend_ticket(self):
        """
        Finishes processing extend ticket.
        @throws Exception in case of error
        """

    @abstractmethod
    def service_probe(self):
        """
        Finishes processing probe.
        @throws Exception in case of error
        """

    @abstractmethod
    def service_reserve(self):
        """
        Finishes processing reserve.
        @throws Exception in case of error
        """

    @abstractmethod
    def service_update_lease(self):
        """
        Finishes processing update lease.
        @throws Exception in case of error
        """

    @abstractmethod
    def service_update_ticket(self):
        """
        Finishes processing update ticket.
        @throws Exception in case of error
        """

    @abstractmethod
    def set_actor(self, *, actor: ABCActorMixin):
        """
        Sets the actor in control of the reservation.

        @param actor actor in control of the reservation
        """

    @abstractmethod
    def set_logger(self, *, logger):
        """
        Attaches the logger to use for the reservation.

        @param logger logger object
        """

    @abstractmethod
    def set_service_pending(self, *, code: int):
        """
        Indicates there is a pending operation on the reservation.

        @param code operation code
        """

    @abstractmethod
    def update_lease(self, *, incoming: ABCReservationMixin, update_data: UpdateData):
        """
        Handles an incoming lease update.

        @param incoming incoming lease update
        @param update_data update data

        @throws Exception thrown if lease update is for an un-ticketed
                reservation
        @throws Exception thrown if lease update is for closed reservation
        """

    @abstractmethod
    def update_ticket(self, *, incoming: ABCReservationMixin, update_data: UpdateData):
        """
        Handles an incoming ticket update.

        @param incoming incoming ticket update
        @param update_data update data

        @throws Exception in case of error
        """

    @abstractmethod
    def validate_incoming(self):
        """
        Validates a reservation as it arrives at an actor.
        @throws Exception in case of error
        """

    @abstractmethod
    def validate_outgoing(self):
        """
        Validates a reservation as it is about to leave an actor.

        @throws Exception in case of error
        """

    @abstractmethod
    def handle_failed_rpc(self, *, failed: FailedRPC):
        """
        Processes a failed RPC request.
        @param failed failed
        """
