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

from fabric_cf.actor.core.apis.i_component import IComponent
from fabric_cf.actor.core.manage.messages.event_mng import EventMng
from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro

if TYPE_CHECKING:
    from fabric_cf.actor.core.util.id import ID
    from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
    from fabric_mb.message_bus.messages.reservation_state_avro import ReservationStateAvro
    from fabric_mb.message_bus.messages.slice_avro import SliceAvro


class IMgmtActor(IComponent):
    @abstractmethod
    def get_slices(self, *, id_token: str = None, slice_id: ID = None) -> List[SliceAvro]:
        """
        Obtains all slices.
        @param id_token id token
        @param slice_id slice id
        @return returns list of all the slices
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
    def get_reservations(self, *, id_token: str = None, state: int = None,
                         slice_id: ID = None, rid: ID = None) -> List[ReservationMng]:
        """
        @param id_token id token
        @param state state
        @param slice_id slice ID
        @param rid reservation id
        Obtains all reservations
        @return returns list of the reservations
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
    def get_reservation_state_for_reservations(self, *, reservation_list: List[ID],
                                               id_token: str = None) -> List[ReservationStateAvro]:
        """
        Returns the state of each of the specified reservations.
        The order in the return list matches the order in the @reservations li
        @param reservation_list list of reservations
        @param id_token id token
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
    def clone(self):
        """
        Creates clone of this proxy to make it possible to be used by a separate thread.
        @return returns the cloned actor
        """

    @abstractmethod
    def get_delegations(self, *, slice_id: ID = None, state: int = None,
                        delegation_id: ID = None, id_token: str = None) -> List[DelegationAvro]:
        """
        Get Delegations
        @param slice_id slice id
        @param state state
        @param delegation_id delegation id
        @param id_token id token
        @return returns list of the delegations
        """
