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
from typing import TYPE_CHECKING, List

from fabric.actor.core.manage.actor_management_object import ActorManagementObject
from fabric.actor.core.apis.i_mgmt_actor import IMgmtActor
from fabric.actor.core.manage.local.local_proxy import LocalProxy

if TYPE_CHECKING:
    from fabric.actor.core.manage.management_object import ManagementObject
    from fabric.actor.security.auth_token import AuthToken
    from fabric.actor.core.util.id import ID
    from fabric.message_bus.messages.reservation_mng import ReservationMng
    from fabric.message_bus.messages.reservation_state_avro import ReservationStateAvro
    from fabric.message_bus.messages.slice_avro import SliceAvro


class LocalActor(LocalProxy, IMgmtActor):
    def __init__(self, *, manager: ManagementObject, auth: AuthToken):
        super().__init__(manager=manager, auth=auth)

        if not isinstance(manager, ActorManagementObject):
            raise Exception("Invalid manager object. Required: {}".format(type(ActorManagementObject)))

    def get_slices(self, *, id_token: str = None) -> List[SliceAvro]:
        self.clear_last()
        try:
            result = self.manager.get_slices(caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def get_slice(self, *, slice_id: ID, id_token: str = None) -> SliceAvro:
        self.clear_last()
        try:
            result = self.manager.get_slice(slice_id=slice_id, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result_list=result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def remove_slice(self, *, slice_id: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.remove_slice(slice_id=slice_id, caller=self.auth)
            self.last_status = result

            return result.code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def get_reservations(self, *, id_token: str = None) -> List[ReservationMng]:
        self.clear_last()
        try:
            result = self.manager.get_reservations(caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_reservations_by_state(self, *, state: int, id_token: str = None) -> List[ReservationMng]:
        self.clear_last()
        try:
            result = self.manager.get_reservations(caller=self.auth, state=state)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def remove_reservation(self, *, rid: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.remove_reservation(caller=self.auth, rid=rid)
            self.last_status = result

            return result.code() == 0

        except Exception as e:
            self.last_exception = e

        return False

    def close_reservation(self, *, rid: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.close_reservation(caller=self.auth, rid=rid)
            self.last_status = result

            return result.code() == 0

        except Exception as e:
            self.last_exception = e

        return False

    def get_name(self) -> str:
        return self.manager.get_actor_name()

    def get_guid(self) -> ID:
        return self.manager.get_actor().get_guid()

    def add_slice(self, *, slice_obj: SliceAvro) -> ID:
        self.clear_last()
        try:
            result = self.manager.add_slice(slice_obj=slice_obj, caller=self.auth)
            self.last_status = result.status

            if self.last_status.get_code() == 0 and result.result is not None:
                return ID(id=result.result)

        except Exception as e:
            self.last_exception = e

        return None

    def update_slice(self, *, slice_obj: SliceAvro) -> bool:
        self.clear_last()
        try:
            result = self.manager.add_slice(slice_obj=slice_obj, caller=self.auth)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.last_exception = e

        return False

    def create_event_subscription(self) -> ID:
        self.clear_last()
        try:
            result = self.manager.create_event_subscription(caller=self.auth)
            self.last_status = result.status

            if self.last_status.get_code() == 0 and result.result is not None:
                return ID(id=self.get_first(result_list=result.result))

        except Exception as e:
            self.last_exception = e

        return None

    def delete_event_subscription(self, *, subscription_id: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.delete_event_subscription(caller=self.auth, subscription_id=subscription_id)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.last_exception = e

        return False

    def drain_events(self, *, subscription_id: ID, timeout: int) -> list:
        self.clear_last()
        try:
            result = self.manager.drain_events(caller=self.auth, subscription_id=subscription_id, timeout=timeout)
            self.last_status = result.status

            if self.last_status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def clone(self):
        return LocalActor(manager=self.manager, auth=self.auth)

    def get_reservations_by_slice_id_and_state(self, *, slice_id: ID, state: int, id_token: str = None) -> list:
        self.clear_last()
        try:
            result = self.manager.get_reservations_by_slice_id_state(caller=self.auth, slice_id=slice_id, state=state)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def update_reservation(self, *, reservation: ReservationMng) -> bool:
        self.clear_last()
        try:
            result = self.manager.update_reservation(reservation=reservation, caller=self.auth)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.last_exception = e

        return False

    def close_reservations(self, *, slice_id: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.close_slice_reservations(caller=self.auth, slice_id=slice_id)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.last_exception = e

        return False

    def get_reservation_state(self, *, rid: ID, id_token: str = None) -> ReservationStateAvro:
        self.clear_last()
        try:
            result = self.manager.get_reservation_state(caller=self.auth, rid=rid)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_reservation_state_for_reservations(self, *, reservation_list: List[ID], id_token: str = None) -> List[ReservationStateAvro]:
        self.clear_last()
        try:
            result = self.manager.get_reservation_state_for_reservations(caller=self.auth, rids=reservation_list)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None