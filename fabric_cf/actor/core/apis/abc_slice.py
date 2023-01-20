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

from abc import abstractmethod, ABC
from datetime import datetime
from typing import TYPE_CHECKING, Tuple, Dict, List

if TYPE_CHECKING:
    from fim.graph.abc_property_graph import ABCPropertyGraph
    from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.util.reservation_set import ReservationSet
    from fabric_cf.actor.core.kernel.slice import SliceTypes
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.util.resource_type import ResourceType
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.kernel.slice_state_machine import SliceState, SliceOperation


class ABCSlice(ABC):
    """
    ISlice describes the programming interface to a slice object. Each slice has a name (not necessarily unique)
    and a globally unique identifier. Slices are used to organize groups of reservations. Each reservation belongs
    to exactly one slice. Slices information is passed to upstream actors as part of ticket and lease requests.
    There are several slice types:
        inventory slices: are used to organized reservations that represent an inventory.
                          For example, allocated resources, or resources to be used to satisfy client requests
        client slices: are used on server actors to group reservations representing client requests
                       (allocated/assigned resources).
        broker client slices: are client slices that represent the requests from a broker that acts as a
                              client of the containing actor

    Each slice contains a number of properties lists, which can be used to store properties applicable to all
    reservations associated with the slice. Properties defined in the slice are automatically inherited by reservations.
    Each reservation can also override a property inherited by the slice, but defining it in its appropriate
    properties list.
    """

    @abstractmethod
    def get_config_properties(self) -> dict:
        """
        Returns the slice configuration properties list

        Returns:
            configuration properties list
        """

    @abstractmethod
    def get_description(self) -> str:
        """
        Returns the slice description

        Returns:
            slice description
        """

    @abstractmethod
    def get_name(self) -> str:
        """
        Returns the slice name

        Returns:
            slice name
        """

    @abstractmethod
    def get_owner(self) -> AuthToken:
        """
        Returns the slice owner

        Returns:
            slice owner
        """

    @abstractmethod
    def get_resource_type(self) -> ResourceType:
        """
        Returns the resource type of the slice (if any).

        Returns:
            slice resource type
        """

    @abstractmethod
    def get_slice_id(self) -> ID:
        """
        Returns the slice id.

        Returns:
            slice id
        """

    @abstractmethod
    def is_broker_client(self) -> bool:
        """
        Checks if the slice is a broker client slice (a client slice within an authority that represents a broker).

        Returns:
            true if the slice is a broker client slice
        """

    @abstractmethod
    def is_client(self) -> bool:
        """
        Checks if the slice is a client slice.

        Returns:
            true if the slice is a client slice
        """

    @abstractmethod
    def is_inventory(self) -> bool:
        """
        Checks if the slice is a inventory slice.

        Returns:
            true if the slice is a inventory slice
        """

    @abstractmethod
    def set_broker_client(self):
        """
        Marks the slice as a broker client slice (a client slice within an authority that represents a broker).
        """

    @abstractmethod
    def set_client(self):
        """
        Marks the slice as a client slice.
        """

    @abstractmethod
    def set_inventory(self, *, value: bool):
        """
        Sets the inventory flag.

        Args:
            value: inventory status: true, inventory slice, false, client slice
        """

    @abstractmethod
    def set_description(self, *, description: str):
        """
        Sets the slice description.

        Args:
            description: description
        """

    @abstractmethod
    def set_name(self, *, name: str):
        """
        Sets the slice name.

        Args:
            name: name
        """

    @abstractmethod
    def set_owner(self, *, owner: AuthToken):
        """
        Sets the slice owner.

        Args:
            owner: owner
        """

    @abstractmethod
    def set_resource_type(self, *, resource_type: ResourceType):
        """
        Sets the resource type.

        Args:
            resource_type: resource type
        """

    @abstractmethod
    def clone_request(self):
        """
        Makes a minimal clone of the slice object sufficient for
        cross-actor calls.

        @return a slice object to use when making cross-actor calls.
        """

    @abstractmethod
    def set_config_properties(self, *, value: dict):
        """
        Set handlers properties
        @param value: value
        """

    @abstractmethod
    def get_slice_type(self) -> SliceTypes:
        """
        Return slice type
        """

    @abstractmethod
    def set_graph(self, *, graph: ABCPropertyGraph):
        """
        Sets the resource model graph.

        @param graph graph
        """

    @abstractmethod
    def get_graph(self) -> ABCPropertyGraph:
        """
        Gets the resource model graph.
        """

    @abstractmethod
    def set_graph_id(self, graph_id: str):
        """
        Set graph id
        @param graph_id:  graph_id
        """

    @abstractmethod
    def get_graph_id(self) -> str:
        """
        Returns the graph id

        @return graph id
        """

    @abstractmethod
    def get_state(self) -> SliceState:
        """
        Return Slice State
        @return slice state
        """

    @abstractmethod
    def transition_slice(self, *, operation: SliceOperation) -> Tuple[bool, SliceState]:
        """
        Attempt to transition a slice to a new state
        @param operation slice operation
        @return Slice State
        @throws Exception in case of error
        """

    @abstractmethod
    def is_dirty(self) -> bool:
        """
        Checks if the slice has uncommitted updates.

        Returns:
            true if the slice has an uncommitted updates
        """

    @abstractmethod
    def set_dirty(self):
        """
        Marks the slice as containing uncommitted updates.
        """

    @abstractmethod
    def clear_dirty(self):
        """
        Marks that the slice has no uncommitted updates or state transitions.
        """

    @abstractmethod
    def is_stable_ok(self) -> bool:
        """
        Is Slice in state Stable OK
        @return True if Slice state is Stable OK, false otherwise
        """

    def is_stable_error(self) -> bool:
        """
        Is Slice in state Stable Error
        @return True if Slice state is Stable Error, false otherwise
        """

    def is_stable(self) -> bool:
        """
        Is Slice in state StableOK/StableError
        @return True if Slice state is StableOK/StableError, false otherwise
        """

    @abstractmethod
    def is_modify_ok(self) -> bool:
        """
        Is Slice in state Modify OK
        @return True if Slice state is Modify OK, false otherwise
        """

    def is_modify_error(self) -> bool:
        """
        Is Slice in state Modify Error
        @return True if Slice state is Modify Error, false otherwise
        """

    def is_modified(self) -> bool:
        """
        Is Slice in state ModifyOK/ModifyError
        @return True if Slice state is ModifyOK/ModifyError, false otherwise
        """

    def is_dead_or_closing(self) -> bool:
        """
        Is Slice in state Dead/Closing
        @return True if Slice state is Dead/Closing, false otherwise
        """

    def is_dead(self) -> bool:
        """
        Is Slice in state Dead
        @return True if Slice state is Dead, false otherwise
        """

    def get_lease_end(self) -> datetime:
        """
        Return lease end time
        """

    def get_lease_start(self) -> datetime:
        """
        Return lease start time
        """

    def set_lease_end(self, *, lease_end: datetime):
        """
        Return lease end time
        """

    def set_lease_start(self, *, lease_start: datetime):
        """
        Return lease start time
        """

    def set_project_id(self, *, project_id: str):
        """
        Set project id
        @param project_id: project id
        """

    def get_project_id(self) -> str:
        """
        Return project id
        """

    def set_project_name(self, *, project_name: str):
        """
        Set project name
        @param project_name: project name
        """

    def get_project_name(self) -> str:
        """
        Return project name
        """

    @abstractmethod
    def get_reservations(self) -> ReservationSet:
        """
        Returns the reservation set.

        @return reservation set
        """

    @abstractmethod
    def get_delegations(self) -> Dict[str, ABCDelegation]:
        """
        Returns the delegations dict.

        @return delegations dict
        """

    @abstractmethod
    def get_reservations_list(self) -> List[ABCReservationMixin]:
        """
        Returns the reservation set represented as a list. Must be
        called with the kernel lock on to prevent exceptions due to concurrent
        iteration.

        @return a list of reservation
        """

    @abstractmethod
    def is_empty(self) -> bool:
        """
        Checks if the slice is empty.

        @return true if there are no reservations in the slice
        """

    @abstractmethod
    def prepare(self, *, recover: bool = False):
        """
        Prepares to register a new slice.  Clears previous state, such
        as list of reservations in the slice.

        @raises Exception if validity checks fail
        """

    @abstractmethod
    def register(self, *, reservation: ABCReservationMixin):
        """
        Registers a new reservation.

        @param reservation reservation to register

        @throws Exception in case of error
        """

    @abstractmethod
    def soft_lookup(self, *, rid: ID) -> ABCReservationMixin:
        """
        Looks up a reservation by ID but does not throw error if the
        reservation is not present in the slice.

        @params rid the reservation ID

        @returns the reservation with that ID
        """

    @abstractmethod
    def unregister(self, *, reservation: ABCReservationMixin):
        """
        Unregisters the reservation from the slice.

        @param reservation reservation to unregister
        """

    @abstractmethod
    def register_delegation(self, *, delegation: ABCDelegation):
        """
        Registers a new delegation.

        @param delegation delegation to register

        @throws Exception in case of error
        """

    @abstractmethod
    def soft_lookup_delegation(self, *, did: str) -> ABCDelegation:
        """
        Looks up a delegation by ID but does not throw error if the
        delegation is not present in the slice.

        @params did the delegation ID

        @returns the delegation with that ID
        """

    @abstractmethod
    def unregister_delegation(self, *, delegation: ABCDelegation):
        """
        Unregisters the delegation from the slice.

        @param delegation delegation to unregister
        """

    @abstractmethod
    def lock_slice(self):
        """
        Lock slice
        """

    @abstractmethod
    def unlock_slice(self):
        """
        Unlock slice
        """