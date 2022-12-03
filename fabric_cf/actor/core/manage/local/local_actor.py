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
from typing import TYPE_CHECKING, List, Tuple, Dict

from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro

from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.container.maintenance import Site
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

    def get_slices(self, *, slice_id: ID = None, slice_name: str = None, email: str = None, project: str = None,
                   state: List[int] = None, limit: int = None, offset: int = None) -> List[SliceAvro] or None:
        self.clear_last()
        try:
            result = self.manager.get_slices(slice_id=slice_id, caller=self.auth, state=state, slice_name=slice_name,
                                             email=email, project=project, limit=limit, offset=offset)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.slices
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def remove_slice(self, *, slice_id: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.remove_slice(slice_id=slice_id, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def get_reservations(self, *, state: int = None, slice_id: ID = None,
                         rid: ID = None, oidc_claim_sub: str = None, email: str = None,
                         rid_list: List[str] = None, type: str = None, site: str = None) -> List[ReservationMng]:
        self.clear_last()
        try:
            result = self.manager.get_reservations(caller=self.auth, state=state, slice_id=slice_id, rid=rid,
                                                   oidc_claim_sub=oidc_claim_sub, email=email, rid_list=rid_list,
                                                   type=type, site=site)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.reservations

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def remove_reservation(self, *, rid: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.remove_reservation(caller=self.auth, rid=rid)
            self.last_status = result

            return result.get_code() == 0

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def close_reservation(self, *, rid: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.close_reservation(caller=self.auth, rid=rid)
            self.last_status = result

            return result.get_code() == 0

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

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

            if self.last_status.get_code() == 0 and result.get_result() is not None:
                return ID(uid=result.get_result())

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def update_slice(self, *, slice_obj: SliceAvro, modify_state: bool = False) -> bool:
        self.clear_last()
        try:
            result = self.manager.update_slice(slice_mng=slice_obj, caller=self.auth, modify_state=modify_state)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def accept_update_slice(self, *, slice_id: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.accept_update_slice(slice_id=slice_id, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
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

    def close_reservations(self, *, slice_id: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.close_slice_reservations(caller=self.auth, slice_id=slice_id)
            self.last_status = result

            if self.last_status.get_code() == 0:
                return True

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def get_reservation_state_for_reservations(self, *, reservation_list: List[str]) -> List[ReservationStateAvro]:
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
                        delegation_id: str = None) -> List[DelegationAvro]:
        self.clear_last()
        try:
            result = self.manager.get_delegations(caller=self.auth, slice_id=slice_id, did=delegation_id)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.reservations

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def close_delegation(self, *, did: str) -> bool:
        self.clear_last()
        try:
            result = self.manager.close_delegation(caller=self.auth, did=did)
            self.last_status = result

            return result.get_code() == 0

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def remove_delegation(self, *, did: str) -> bool:
        self.clear_last()
        try:
            result = self.manager.remove_delegation(caller=self.auth, did=did)
            self.last_status = result

            return result.get_code() == 0

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def is_testbed_in_maintenance(self) -> Tuple[bool, Dict[str, str] or None]:
        return self.manager.is_testbed_in_maintenance()

    def is_site_in_maintenance(self, *, site_name: str) -> Tuple[bool, Site or None]:
        return self.manager.is_site_in_maintenance(site_name=site_name)

    def is_slice_provisioning_allowed(self, *, project: str, email: str) -> bool:
        return self.manager.is_slice_provisioning_allowed(project=project, email=email)

    def is_sliver_provisioning_allowed(self, *, project: str, email: str, site: str,
                                       worker: str) -> Tuple[bool, str or None]:
        return self.manager.is_sliver_provisioning_allowed(project=project, email=email, site=site, worker=worker)