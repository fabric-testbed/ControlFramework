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

from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.actor.core.apis.abc_reservation_resources import ABCReservationResources
from fabric_cf.actor.core.apis.abc_reservation_status import ABCReservationStatus

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.util.reservation_state import ReservationState
    from fabric_cf.actor.core.util.resource_type import ResourceType


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
