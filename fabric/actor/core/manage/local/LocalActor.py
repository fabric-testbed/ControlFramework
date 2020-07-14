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

from fabric.actor.core.manage.ActorManagementObject import ActorManagementObject
from fabric.actor.core.apis.IMgmtActor import IMgmtActor
from fabric.actor.core.manage.local.LocalProxy import LocalProxy

if TYPE_CHECKING:
    from fabric.actor.core.manage.ManagementObject import ManagementObject
    from fabric.actor.security.AuthToken import AuthToken
    from fabric.actor.core.util.ID import ID
    from fabric.message_bus.messages.ReservationMng import ReservationMng
    from fabric.actor.core.manage.messages.ReservationStateMng import ReservationStateMng
    from fabric.message_bus.messages.SliceAvro import SliceAvro


class LocalActor(LocalProxy, IMgmtActor):
    def __init__(self, manager: ManagementObject = None, auth: AuthToken = None):
        super().__init__(manager, auth)

        if not isinstance(manager, ActorManagementObject):
            raise Exception("Invalid manager object. Required: {}".format(type(ActorManagementObject)))

    def get_slices(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_slices(self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def get_slice(self, slice_id: ID) -> SliceAvro:
        self.clear_last()
        try:
            result = self.manager.get_slice(slice_id, self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def remove_slice(self, slice_id: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.remove_slice(slice_id, self.auth)
            self.last_status = result

            return result.code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def get_reservations(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_reservations(self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_reservations_by_state(self, state: int) -> list:
        self.clear_last()
        try:
            result = self.manager.get_reservations(self.auth, state)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def remove_reservation(self, rid: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.remove_reservation(self.auth, rid)
            self.last_status = result

            return result.code() == 0

        except Exception as e:
            self.last_exception = e

        return False

    def close_reservation(self, rid: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.close_reservation(self.auth, rid)
            self.last_status = result

            return result.code() == 0

        except Exception as e:
            self.last_exception = e

        return False

    def get_name(self) -> str:
        return self.manager.get_actor_name()

    def get_guid(self) -> ID:
        return self.manager.get_actor().get_guid()

    def add_slice(self, slice_obj: SliceAvro) -> ID:
        self.clear_last()
        try:
            result = self.manager.add_slice(slice_obj, self.auth)
            self.last_status = result.status

            if self.last_status.get_code() == 0 and result.result is not None:
                return ID(result.result)

        except Exception as e:
            self.last_exception = e

        return None

    def update_slice(self, slice_obj: SliceAvro) -> bool:
        self.clear_last()
        try:
            result = self.manager.add_slice(slice_obj, self.auth)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.last_exception = e

        return False

    def create_event_subscription(self) -> ID:
        self.clear_last()
        try:
            result = self.manager.create_event_subscription(self.auth)
            self.last_status = result.status

            if self.last_status.get_code() == 0 and result.result is not None:
                return ID(self.get_first(result.result))

        except Exception as e:
            self.last_exception = e

        return None

    def delete_event_subscription(self, subscription_id: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.delete_event_subscription(self.auth, subscription_id)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.last_exception = e

        return False

    def drain_events(self, subscription_id: ID, timeout: int) -> list:
        self.clear_last()
        try:
            result = self.manager.drain_events(self.auth, subscription_id, timeout)
            self.last_status = result.status

            if self.last_status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def clone(self):
        return LocalActor(self.manager, self.auth)

    def get_reservations_by_slice_id_and_state(self, slice_id: ID, state: int) -> list:
        self.clear_last()
        try:
            result = self.manager.get_reservations_by_slice_id_state(self.auth, slice_id, state)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def update_reservation(self, reservation: ReservationMng) -> bool:
        self.clear_last()
        try:
            result = self.manager.update_reservation(reservation, self.auth)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.last_exception = e

        return False

    def close_reservations(self, slice_id: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.close_slice_reservations(self.auth, slice_id)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.last_exception = e

        return False

    def get_reservation_state(self, rid: ID) -> ReservationStateMng:
        self.clear_last()
        try:
            result = self.manager.get_reservation_state(self.auth, rid)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_reservation_state_for_reservations(self, reservation_list: list) -> list:
        self.clear_last()
        try:
            result = self.manager.get_reservation_state_for_reservations(self.auth, reservation_list)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None