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
from typing import TYPE_CHECKING

from fabric.actor.core.apis.IComponent import IComponent

if TYPE_CHECKING:
    from fabric.actor.core.util.ID import ID
    from fabric.message_bus.messages.ReservationMng import ReservationMng
    from fabric.actor.core.manage.messages.ReservationStateMng import ReservationStateMng
    from fabric.message_bus.messages.SliceAvro import SliceAvro


class IMgmtActor(IComponent):
    def get_slices(self) -> list:
        raise NotImplementedError

    def get_slice(self, slice_id: ID) -> SliceAvro:
        raise NotImplementedError

    def add_slice(self, slice_obj: SliceAvro) -> ID:
        raise NotImplementedError

    def remove_slice(self, slice_id: ID) -> bool:
        raise NotImplementedError

    def update_slice(self, slice_obj: SliceAvro) -> bool:
        raise NotImplementedError

    def get_reservations(self) -> list:
        raise NotImplementedError

    def get_reservations_by_state(self, state: int) -> list:
        raise NotImplementedError

    def get_reservations_by_slice_id(self, slice_id: ID) -> list:
        raise NotImplementedError

    def get_reservations_by_slice_id_and_state(self, slice_id: ID, state: int) -> list:
        raise NotImplementedError

    def get_reservation(self, rid: ID) -> ReservationMng:
        raise NotImplementedError

    def update_reservation(self, reservation: ReservationMng) -> bool:
        raise NotImplementedError

    def close_reservation(self, rid: ID) -> bool:
        raise NotImplementedError

    def close_reservations(self, slice_id: ID) -> bool:
        raise NotImplementedError

    def remove_reservation(self, rid: ID) -> bool:
        raise NotImplementedError

    def get_reservation_state(self, rid: ID) -> ReservationStateMng:
        raise NotImplementedError

    def get_reservation_state_for_reservations(self, reservation_list: list) -> list:
        raise NotImplementedError

    def get_name(self) -> str:
        raise NotImplementedError

    def get_guid(self) -> ID:
        raise NotImplementedError

    def create_event_subscription(self) -> ID:
        raise NotImplementedError

    def delete_event_subscription(self, subscription_id: ID) -> bool:
        raise NotImplementedError

    def drain_events(self, subscription_id: ID, timeout: int) -> list:
        raise NotImplementedError

    def clone(self):
        raise NotImplementedError