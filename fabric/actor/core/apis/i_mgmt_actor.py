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
from typing import TYPE_CHECKING, List

from fabric.actor.core.apis.i_component import IComponent
from fabric.actor.core.manage.messages.event_mng import EventMng

if TYPE_CHECKING:
    from fabric.actor.core.util.id import ID
    from fabric.message_bus.messages.reservation_mng import ReservationMng
    from fabric.message_bus.messages.reservation_state_avro import ReservationStateAvro
    from fabric.message_bus.messages.slice_avro import SliceAvro


class IMgmtActor(IComponent):
    @abstractmethod
    def get_slices(self) -> List[SliceAvro]:
        """
        Obtains all slices.
        @return returns list of all the slices
        """

    @abstractmethod
    def get_slice(self, *, slice_id: ID) -> SliceAvro:
        """
        Obtains the specified slice
        @param slice_id slice id
        @return returns the specified slice
        """

    @abstractmethod
    def add_slice(self, *, slice_obj: SliceAvro) -> ID:
        """
        Adds a new slice
        @param slice_obj slice
        @return returns slice id
        """

    @abstractmethod
    def remove_slice(self, *, slice_id: ID) -> bool:
        """
        Removes the specified slice
        @param slice_id slice id
        @return true for success; false otherwise
        """

    @abstractmethod
    def update_slice(self, *, slice_obj: SliceAvro) -> bool:
        """
        Updates the specified slice.
        The only updatable slice attributes are:
        - description
        - all properties lists
        @param slice_obj slice
        @return true for success; false otherwise
        """

    @abstractmethod
    def get_reservations(self) -> List[ReservationMng]:
        """
        Obtains all reservations
        @return returns list of the reservations
        """

    @abstractmethod
    def get_reservations_by_state(self, *, state: int) -> List[ReservationMng]:
        """
        Obtains all reservations in the specified state.
        @param state state
        @return list of the reservations in the specified state
        """

    @abstractmethod
    def get_reservations_by_slice_id(self, *, slice_id: ID) -> List[ReservationMng]:
        """
        Obtains all reservations in the specified slice.
        @param slice_id slice ID
        @return list of the reservations for the specific slice
        """

    @abstractmethod
    def get_reservations_by_slice_id_and_state(self, *, slice_id: ID, state: int) -> List[ReservationMng]:
        """
        Obtains all reservations in the given slice in the specified state.
        @param slice_id slice id
        @param state state
        @return list of the reservations for specific slice in specific state
        """

    @abstractmethod
    def get_reservation(self, *, rid: ID) -> ReservationMng:
        """
        Obtains the specified reservation
        @param rid reservation id
        @return returns the reservation identified by id
        """

    @abstractmethod
    def update_reservation(self, *, reservation: ReservationMng) -> bool:
        """
        Updates the specified reservation
        The fields that can be updated are:

        - all properties lists

        @param reservation reservation to be updated
        @return true for success; false otherwise
        """

    @abstractmethod
    def close_reservation(self, *, rid: ID) -> bool:
        """
        Closes the specified reservation
        @param rid reservation id
        @return true for success; false otherwise
        """

    @abstractmethod
    def close_reservations(self, *, slice_id: ID) -> bool:
        """
        Closes all reservations in the specified slice.
        @param slice_id slice ID
        @return true for success; false otherwise
        """

    @abstractmethod
    def remove_reservation(self, *, rid: ID) -> bool:
        """
        Removes the specified reservation.
        Note only closed reservations can be removed.
        @param rid reservation id of the reservation to be removed
        @return true for success; false otherwise
        """

    @abstractmethod
    def get_reservation_state(self, *, rid: ID) -> ReservationStateAvro:
        """
        Returns the state of the specified reservation.
        @param rid reservation id
        @return returns the state of the specific reservation
        """

    @abstractmethod
    def get_reservation_state_for_reservations(self, *, reservation_list: List[ID]) -> List[ReservationStateAvro]:
        """
        Returns the state of each of the specified reservations.
        The order in the return list matches the order in the @reservations li
        @param reservation_list list of reservations
        @return list of state of the specified reservations
        """

    @abstractmethod
    def get_name(self) -> str:
        """
        Returns the name of the actor.
        @return returns name of the actor
        """

    @abstractmethod
    def get_guid(self) -> ID:
        """
        Returns the guid of the actor.
        @return returns guid of the actor
        """

    @abstractmethod
    def create_event_subscription(self) -> ID:
        """
        Creates an event subscription.
        @return the identity of the subscription.
        """

    @abstractmethod
    def delete_event_subscription(self, *, subscription_id: ID) -> bool:
        """
        Deletes the specified event subscription.
        @param subscriptionID subscription id
        @return true for success; false otherwise
        """

    @abstractmethod
    def drain_events(self, *, subscription_id: ID, timeout: int) -> List[EventMng]:
        """
        Drains all events from the specified subscription.
        @param subscription_id subscription id
        @param timeout timeout
        @return list of the events drained out
        """

    @abstractmethod
    def clone(self):
        """
        Creates clone of this proxy to make it possible to be used by a separate thread.
        @return returns the cloned actor
        """