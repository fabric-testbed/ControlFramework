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

import traceback
from typing import TYPE_CHECKING, List

from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro

from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.actor_management_object import ActorManagementObject
from fabric_cf.actor.core.apis.abc_mgmt_actor import ABCMgmtActor
from fabric_cf.actor.core.manage.local.local_proxy import LocalProxy
from fabric_cf.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
    from fabric_mb.message_bus.messages.reservation_state_avro import ReservationStateAvro
    from fabric_mb.message_bus.messages.slice_avro import SliceAvro
    from fabric_cf.actor.core.manage.management_object import ManagementObject
    from fabric_cf.actor.security.auth_token import AuthToken


class LocalActor(LocalProxy, ABCMgmtActor):
    def __init__(self, *, manager: ManagementObject, auth: AuthToken):
        super().__init__(manager=manager, auth=auth)

        if not isinstance(manager, ActorManagementObject):
            raise ManageException("Invalid manager object. Required: {}".format(type(ActorManagementObject)))

    def get_slices(self, *, id_token: str = None, slice_id: ID = None, slice_name: str = None,
                   email: str = None) -> List[SliceAvro]:
        self.clear_last()
        try:
            result = self.manager.get_slices(slice_id=slice_id, caller=self.auth, id_token=id_token,
                                             slice_name=slice_name, email=email)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.slices
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def remove_slice(self, *, slice_id: ID, id_token: str = None) -> bool:
        self.clear_last()
        try:
            result = self.manager.remove_slice(slice_id=slice_id, caller=self.auth, id_token=id_token)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def get_reservations(self, *, id_token: str = None, state: int = None, slice_id: ID = None,
                         rid: ID = None, oidc_claim_sub: str = None, email: str = None,
                         rid_list: List[str] = None) -> List[ReservationMng]:
        self.clear_last()
        try:
            result = self.manager.get_reservations(caller=self.auth, state=state, slice_id=slice_id, rid=rid,
                                                   id_token=id_token, oidc_claim_sub=oidc_claim_sub, email=email,
                                                   rid_list=rid_list)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.reservations

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def remove_reservation(self, *, rid: ID, id_token: str = None) -> bool:
        self.clear_last()
        try:
            result = self.manager.remove_reservation(caller=self.auth, rid=rid, id_token=id_token)
            self.last_status = result

            return result.get_code() == 0

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def close_reservation(self, *, rid: ID, id_token: str = None) -> bool:
        self.clear_last()
        try:
            result = self.manager.close_reservation(caller=self.auth, rid=rid, id_token=id_token)
            self.last_status = result

            return result.get_code() == 0

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def get_name(self) -> str:
        return self.manager.get_actor_name()

    def get_guid(self) -> ID:
        return self.manager.get_actor().get_guid()

    def add_slice(self, *, slice_obj: SliceAvro, id_token: str) -> ID:
        self.clear_last()
        try:
            result = self.manager.add_slice(slice_obj=slice_obj, caller=self.auth, id_token=id_token)
            self.last_status = result.status

            if self.last_status.get_code() == 0 and result.get_result() is not None:
                return ID(uid=result.get_result())

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def update_slice(self, *, slice_obj: SliceAvro) -> bool:
        self.clear_last()
        try:
            result = self.manager.add_slice(slice_obj=slice_obj, caller=self.auth)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def clone(self):
        return LocalActor(manager=self.manager, auth=self.auth)

    def update_reservation(self, *, reservation: ReservationMng) -> bool:
        self.clear_last()
        try:
            result = self.manager.update_reservation(reservation=reservation, caller=self.auth)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def close_reservations(self, *, slice_id: ID, id_token: str = None) -> bool:
        self.clear_last()
        try:
            result = self.manager.close_slice_reservations(caller=self.auth, slice_id=slice_id, id_token=id_token)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def get_reservation_state_for_reservations(self, *, reservation_list: List[str],
                                               id_token: str = None) -> List[ReservationStateAvro]:
        self.clear_last()
        try:
            result = self.manager.get_reservation_state_for_reservations(caller=self.auth, rids=reservation_list)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.reservation_states

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def get_delegations(self, *, slice_id: ID = None, state: int = None,
                        delegation_id: str = None, id_token: str = None) -> List[DelegationAvro]:
        self.clear_last()
        try:
            result = self.manager.get_delegations(caller=self.auth, slice_id=slice_id, did=delegation_id,
                                                  id_token=id_token)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.reservations

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None
