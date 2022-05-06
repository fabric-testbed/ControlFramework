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
from enum import Enum
from typing import TYPE_CHECKING

from fabric_cf.actor.core.apis.abc_reservation_resources import ABCReservationResources
from fabric_cf.actor.core.apis.abc_reservation_status import ABCReservationStatus


if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.util.reservation_state import ReservationState
    from fabric_cf.actor.core.util.resource_type import ResourceType
    from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
    from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
    from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
    from fabric_cf.actor.core.kernel.request_types import RequestTypes
    from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
    from fabric_cf.actor.core.util.update_data import UpdateData


class ReservationCategory(Enum):
    """
    Enumeration for Reservation Category
    """
    # Unspecified reservation category.
    All = 0
    # Client-side reservations.
    Client = 1
    # Broker-side reservations.
    Broker = 2
    # Site authority-side reservations.
    Authority = 3
    # Used only for Get Operations
    Inventory = 4


class ABCReservationMixin(ABCReservationResources, ABCReservationStatus):
    """
    IReservation defines the the core API for a reservation. Most of the methods described in the interface allow the
    programmer to inspect the state of the reservation, access some of its core objects,
    and wait for the occurrence of a particular event.
    """
    @abstractmethod
    def clear_dirty(self):
        """
        Marks that the reservation has no uncommitted updates or state transitions.
        """

    @abstractmethod
    def get_actor(self) -> ABCActorMixin:
        """
        Returns the actor in control of the reservation.

        Returns:
            the actor in control of the reservation.
        """

    @abstractmethod
    def get_category(self) -> ReservationCategory:
        """
        Returns the reservation category.

        Returns:
            the reservation category.
        """

    @abstractmethod
    def get_pending_state(self) -> ReservationPendingStates:
        """
        Returns the current pending reservation state.

        Returns:
            current pending reservation state.
        """

    @abstractmethod
    def get_pending_state_name(self) -> str:
        """
        Returns the current pending reservation state name

        Returns:
            current pending reservation state name
        """

    @abstractmethod
    def get_reservation_id(self) -> ID:
        """
        Returns the reservation id.

        Returns:
            the reservation id.
        """

    @abstractmethod
    def get_reservation_state(self) -> ReservationState:
        """
        Returns the current composite reservation state.

        Returns:
            current composite reservation state.
        """

    @abstractmethod
    def get_slice(self) -> ABCSlice:
        """
        Returns the slice the reservation belongs to.

        Returns:
            slice the reservation belongs to.
        """

    @abstractmethod
    def get_slice_id(self) -> ID:
        """
        Returns the slice GUID.

        Returns:
            slice guid
        """

    @abstractmethod
    def get_slice_name(self) -> str:
        """
        Returns the slice name.

        Returns:
            slice name
        """

    @abstractmethod
    def get_state(self) -> ReservationStates:
        """
        Returns the current reservation state.

        Returns:
            current reservation state.
        """

    @abstractmethod
    def get_state_name(self) -> str:
        """
        Returns the current reservation state name.

        Returns:
            current reservation state name.
        """

    @abstractmethod
    def get_type(self) -> ResourceType:
        """
        Returns the resource type allocated to the reservation. If no resources have yet been allocated to the
        reservation, this method will return None.

        Returns:
            resource type allocated to the reservation. None if no resources have been allocated to the reservation.
        """

    @abstractmethod
    def has_uncommitted_transition(self) -> bool:
        """
        Checks if the reservation has uncommitted state transitions.

        Returns:
            true if the reservation has an uncommitted transition
        """

    @abstractmethod
    def is_dirty(self) -> bool:
        """
        Checks if the reservation has uncommitted updates.

        Returns:
            true if the reservation has an uncommitted updates
        """

    @abstractmethod
    def is_pending_recover(self) -> bool:
        """
        Checks if a recovery operation is in progress for the reservation

        Returns:
            true if a recovery operation for the reservation is in progress
        """

    @abstractmethod
    def set_dirty(self):
        """
        Marks the reservation as containing uncommitted updates.
        """

    @abstractmethod
    def set_pending_recover(self, *, pending_recover: bool):
        """
        Indicates if a recovery operation for the reservation is going to be in progress.

        Args:
            pending_recover: true, a recovery operation is in progress, false - no recovery operation is in progress.
        """

    @abstractmethod
    def set_slice(self, *, slice_object):
        """
        Sets the slice the reservation belongs to.

        Args:
            slice_object: slice the reservation belongs to
        """

    @abstractmethod
    def transition(self, *, prefix: str, state: ReservationStates, pending: ReservationPendingStates):
        """
        Transitions this reservation into a new state.

        Args:
            prefix: prefix
            state: the new state
            pending: if reservation is pending
        """

    @abstractmethod
    def get_notices(self) -> str:
        """
        Returns the error message associated with this reservation.
        @return error message associated with this reservation
        """

    @abstractmethod
    def get_graph_node_id(self) -> str:
        """
        Returns the graph node id which represents the sliver for this reservation in the graph model
        @return graph node id
        """

    @abstractmethod
    def get_error_message(self) -> str:
        """
        Return Error Message
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
