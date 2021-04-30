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

from fabric_mb.message_bus.messages.reservation_mng import ReservationMng

from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.authority_management_object import AuthorityManagementObject
from fabric_cf.actor.core.apis.abc_mgmt_authority import ABCMgmtAuthority
from fabric_cf.actor.core.manage.local.local_server_actor import LocalServerActor

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.unit_avro import UnitAvro

    from fabric_cf.actor.core.manage.management_object import ManagementObject
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.apis.abc_mgmt_actor import ABCMgmtActor


class LocalAuthority(LocalServerActor, ABCMgmtAuthority):
    def __init__(self, *, manager: ManagementObject, auth: AuthToken):
        super().__init__(manager=manager, auth=auth)

        if not isinstance(manager, AuthorityManagementObject):
            raise ManageException("Invalid manager object. Required: {}".format(type(AuthorityManagementObject)))

    def get_authority_reservations(self, *, id_token: str = None) -> List[ReservationMng]:
        self.clear_last()
        try:
            result = self.manager.get_authority_reservations(caller=self.auth)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.reservations
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def get_reservation_units(self, *, rid: ID, id_token: str = None) -> List[UnitAvro]:
        self.clear_last()
        try:
            result = self.manager.get_reservation_units(caller=self.auth, rid=rid)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.units
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def get_reservation_unit(self, *, uid: ID, id_token: str = None) -> UnitAvro:
        self.clear_last()
        try:
            result = self.manager.get_reservation_unit(caller=self.auth, uid=uid)
            self.last_status = result.status
            if result.get_status().get_code() == 0:
                return self.get_first(result_list=result.units)
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def clone(self) -> ABCMgmtActor:
        return LocalAuthority(manager=self.manager, auth=self.auth)
