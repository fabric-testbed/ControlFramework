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
from enum import Enum
from typing import TYPE_CHECKING

from fabric.actor.core.apis.i_callback_proxy import ICallbackProxy
from fabric.actor.core.apis.i_policy import IPolicy
from fabric.actor.core.util.update_data import UpdateData
from fim.graph.abc_property_graph import ABCPropertyGraph

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_actor import IActor
    from fabric.actor.core.util.id import ID
    from fabric.actor.core.apis.i_slice import ISlice


class DelegationState(Enum):
    Nascent = 1
    Delegated = 2
    Closed = 3
    Reclaimed = 4


class IDelegation(ABC):
    """
    IDelegation defines the the core API for a delegation. Most of the methods described in the interface allow the
    programmer to inspect the state of the delegation, access some of its core objects,
    and wait for the occurrence of a particular event.
    """

    @abstractmethod
    def set_graph(self, graph: ABCPropertyGraph):
        """
        Set the Graph associated
        @param graph: graph
        """

    @abstractmethod
    def get_graph(self) -> ABCPropertyGraph:
        """
        Return the Graph associated
        """

    @abstractmethod
    def get_actor(self) -> IActor:
        """
        Returns the actor in control of the delegation.

        Returns:
            the actor in control of the delegation.
        """

    @abstractmethod
    def get_delegation_id(self) -> ID:
        """
        Returns the delegation id.

        Returns:
            the delegation id.
        """

    @abstractmethod
    def get_slice_object(self) -> ISlice:
        """
        Returns the slice the delegation belongs to.

        Returns:
            slice the delegation belongs to.
        """

    @abstractmethod
    def get_slice_id(self) -> ID:
        """
        Returns the resource model GUID.

        Returns:
            resource model  guid
        """

    @abstractmethod
    def get_state(self) -> DelegationState:
        """
        Returns the current delegation state.

        Returns:
            current delegation state.
        """

    @abstractmethod
    def get_state_name(self) -> str:
        """
        Returns the current delegation state name.

        Returns:
            current delegation state name.
        """

    @abstractmethod
    def set_slice_object(self, *, slice_object: ISlice):
        """
        Sets the slice the delegation belongs to.

        Args:
            slice_object: slice_object the delegation belongs to
        """

    @abstractmethod
    def set_logger(self, *, logger):
        """
        Sets the logger
        @param logger: logger
        """

    @abstractmethod
    def transition(self, *, prefix: str, state: DelegationState):
        """
        Transitions this delegation into a new state.

        Args:
            prefix: prefix
            state: the new state
        """
        raise NotImplementedError("Should have implemented this")

    @abstractmethod
    def get_notices(self) -> str:
        """
        Returns the error message associated with this delegation.
        @return error message associated with this delegation
        """

    @abstractmethod
    def is_dirty(self) -> bool:
        """
        Checks if the delegation has uncommitted updates.

        Returns:
            true if the delegation has an uncommitted updates
        """

    @abstractmethod
    def set_dirty(self):
        """
        Marks the delegation as containing uncommitted updates.
        """

    @abstractmethod
    def clear_dirty(self):
        """
        Marks that the delegation has no uncommitted updates or state transitions.
        """

    @abstractmethod
    def has_uncommitted_transition(self) -> bool:
        """
        Checks if the delegation has uncommitted state transitions.

        Returns:
            true if the delegation has an uncommitted transition
        """

    @abstractmethod
    def set_actor(self, actor: IActor):
        """
        Set Actor
        @param actor: actor
        """

    @abstractmethod
    def prepare(self, *, callback: ICallbackProxy, logger):
        """
        Prepare the delegation
        """

    @abstractmethod
    def is_closed(self) -> bool:
        """
        Returns true if delegation is closed; false otherwise
        """

    @abstractmethod
    def delegate(self, policy: IPolicy):
        """
        Check if delegation can be delegated and state transition
        """

    @abstractmethod
    def claim(self):
        """
        Claim the delegation
        """

    @abstractmethod
    def reclaim(self):
        """
        Reclaim the delegation
        """

    @abstractmethod
    def close(self):
        """
        Close the delegation
        """

    @abstractmethod
    def probe_pending(self):
        """
        Probe a delegation with a pending request. On server, if the
        operation completed, handle it and generate an update. If no pending
        request completed then do nothing.

        @throws Exception in case of error
        """

    @abstractmethod
    def prepare_probe(self):
        """
        Prepares a delegation probe.

        @throws Exception in case of error
        """

    @abstractmethod
    def service_probe(self):
        """
        Finishes processing probe.
        @throws Exception in case of error
        """

    @abstractmethod
    def get_callback(self) -> ICallbackProxy:
        """
        Returns the callback proxy.
        @return callback proxy
        """

    @abstractmethod
    def get_update_data(self) -> UpdateData:
        """
        Returns data to be sent back to the client in an update message.
        @return data to be sent back to the client in an update message
        """

    @abstractmethod
    def get_sequence_in(self):
        """
        Returns the sequence number of the last received message.
        @returns sequence number of the last received message
        """

    @abstractmethod
    def get_sequence_out(self):
        """
        Returns the sequence number of the last sent message.
        @returns sequence number of the last sent message
        """

    @abstractmethod
    def restore(self, actor: IActor, slice_obj: ISlice, logger):
        """
        Restore a reservation after reading from database
        @param actor: actor
        @param slice_obj: slice object
        @param logger: logger
        """

    @abstractmethod
    def validate_incoming(self):
        """
        Validate an incoming delegation
        @raises Exception in case of failure
        """

    @abstractmethod
    def service_delegate(self):
        """
        Finishes processing delegation.
        @throws Exception in case of error
        """

    @abstractmethod
    def validate_outgoing(self):
        """
        Validate an outgoing delegation
        @raises Exception in case of failure
        """

    @abstractmethod
    def service_update_delegation(self):
        """
        Finishes processing update delegation.
        @throws Exception in case of error
        """

    @abstractmethod
    def update_delegation(self, *, incoming: IDelegation, update_data: UpdateData):
        """
        Handles an incoming delegation update.

        @param incoming incoming delegation update
        @param update_data update data

        @throws Exception in case of error
        """

    @abstractmethod
    def load_graph(self, *, graph_str: str):
        """
        Load Neo4j graph from string
        @param graph_str: graph_str
        """