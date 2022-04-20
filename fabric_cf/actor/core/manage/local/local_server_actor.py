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

from fim.graph.abc_property_graph import ABCPropertyGraph

from fabric_mb.message_bus.messages.reservation_mng import ReservationMng

from fabric_cf.actor.core.apis.abc_reservation_mixin import ReservationCategory
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.server_actor_management_object import ServerActorManagementObject
from fabric_cf.actor.core.apis.abc_mgmt_server_actor import ABCMgmtServerActor
from fabric_cf.actor.core.manage.local.local_actor import LocalActor
from fabric_cf.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.slice_avro import SliceAvro

    from fabric_cf.actor.core.manage.management_object import ManagementObject
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.manage.messages.client_mng import ClientMng
    from fabric_cf.actor.core.apis.abc_mgmt_actor import ABCMgmtActor


class LocalServerActor(LocalActor, ABCMgmtServerActor):
    def __init__(self, *, manager: ManagementObject, auth: AuthToken):
        super().__init__(manager=manager, auth=auth)

        if not isinstance(manager, ServerActorManagementObject):
            raise ManageException("Invalid manager object. Required: {}".format(type(ServerActorManagementObject)))

    def get_client_slices(self, *, id_token: str = None) -> List[SliceAvro]:
        self.clear_last()
        try:
            result = self.manager.get_client_slices(caller=self.auth)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.slices
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def get_clients(self, *, guid: ID = None, id_token: str = None) -> List[ClientMng]:
        self.clear_last()
        try:
            result = self.manager.get_clients(caller=self.auth, guid=guid)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.clients
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def register_client(self, *, client: ClientMng, kafka_topic: str) -> bool:
        self.clear_last()
        if client is None or kafka_topic is None:
            self.last_exception = Exception("Invalid arguments")
            return False

        try:
            result = self.manager.register_client(client=client, kafka_topic=kafka_topic, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def unregister_client(self, *, guid: ID) -> bool:
        self.clear_last()
        if guid is None:
            self.last_exception = Exception("Invalid arguments")
            return False

        try:
            result = self.manager.unregister_client(guid=guid, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def get_client_reservations(self, *, slice_id: ID = None, id_token: str = None) -> List[ReservationMng]:
        self.clear_last()
        try:
            result = self.manager.get_reservations_by_category(caller=self.auth,
                                                               category=ReservationCategory.Client,
                                                               slice_id=slice_id)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.reservations
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def get_broker_reservations(self, *, id_token: str = None) -> List[ReservationMng]:
        self.clear_last()
        try:
            result = self.manager.get_reservations_by_category(caller=self.auth,
                                                               category=ReservationCategory.Broker)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.reservations
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def get_inventory_slices(self, *, id_token: str = None) -> List[SliceAvro]:
        self.clear_last()
        try:
            result = self.manager.get_inventory_slices(caller=self.auth)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.slices
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def get_inventory_reservations(self, *, slice_id: ID = None, id_token: str = None) -> List[ReservationMng]:
        self.clear_last()
        try:
            result = self.manager.get_reservations_by_category(caller=self.auth,
                                                               category=ReservationCategory.Inventory,
                                                               slice_id=slice_id)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.reservations
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def add_client_slice(self, *, slice_mng: SliceAvro) -> ID:
        self.clear_last()
        try:
            result = self.manager.add_client_slice(caller=self.auth, slice_mng=slice_mng)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.get_result()
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return None

    def advertise_resources(self, *, delegation: ABCPropertyGraph, delegation_name: str, client: AuthToken) -> ID:
        try:
            result = self.manager.advertise_resources(delegation=delegation, delegation_name=delegation_name,
                                                      client=client, caller=self.auth)
            self.last_status = result.status
            if self.last_status.get_code() == 0 and result.get_result() is not None:
                return ID(uid=result.get_result())
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

    def clone(self) -> ABCMgmtActor:
        return LocalServerActor(manager=self.manager, auth=self.auth)