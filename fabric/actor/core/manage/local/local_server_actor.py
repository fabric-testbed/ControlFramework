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

from datetime import datetime
from typing import TYPE_CHECKING

from fabric.actor.core.manage.server_actor_management_object import ServerActorManagementObject
from fabric.actor.core.apis.i_mgmt_server_actor import IMgmtServerActor
from fabric.actor.core.manage.local.local_actor import LocalActor
from fabric.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric.actor.core.manage.management_object import ManagementObject
    from fabric.actor.security.auth_token import AuthToken
    from fabric.actor.core.manage.messages.ClientMng import ClientMng
    from fabric.actor.core.util.resource_type import ResourceType
    from fabric.actor.core.apis.i_mgmt_actor import IMgmtActor
    from fabric.message_bus.messages.slice_avro import SliceAvro


class LocalServerActor(LocalActor, IMgmtServerActor):
    def __init__(self, manager: ManagementObject = None, auth: AuthToken = None):
        super().__init__(manager, auth)

        if not isinstance(manager, ServerActorManagementObject):
            raise Exception("Invalid manager object. Required: {}".format(type(ServerActorManagementObject)))

    def get_client_slices(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_client_slices(self.auth)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def get_clients(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_clients(self.auth)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def get_client(self, guid: ID) -> ClientMng:
        self.clear_last()
        try:
            result = self.manager.get_client(self.auth, guid)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def register_client(self, client: ClientMng, kafka_topic: str) -> bool:
        self.clear_last()
        if client is None or kafka_topic is None:
            self.last_exception = Exception("Invalid arguments")
            return False

        try:
            result = self.manager.register_client(client, kafka_topic, self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def unregister_client(self, guid: ID) -> bool:
        self.clear_last()
        if guid is None:
            self.last_exception = Exception("Invalid arguments")
            return False

        try:
            result = self.manager.unregister_client(guid, self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def get_client_reservations(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_client_reservations(self.auth)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def get_broker_reservations(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_broker_reservations(self.auth)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def get_inventory_slices(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_inventory_slices(self.auth)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def get_inventory_reservations(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_inventory_reservations(self.auth)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def get_inventory_reservations_by_slice_id(self, slice_id: ID) -> list:
        self.clear_last()
        try:
            result = self.manager.get_inventory_reservations_by_slice_id(self.auth, slice_id)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def add_client_slice(self, slice_mng: SliceAvro) -> ID:
        self.clear_last()
        try:
            result = self.manager.add_client_slice(self.auth, slice_mng)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def get_client_reservations_by_slice_id(self, slice_id: ID) -> list:
        self.clear_last()
        try:
            result = self.manager.get_client_reservations_by_slice_id(self.auth, slice_id)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def export_resources_pool_client_slice(self, client_slice_id: ID, pool_id: ID, start: datetime, end: datetime,
                                           units: int, ticket_properties: dict, resource_properties: dict,
                                           source_ticket_id: ID) -> ID:
        try:
            result = self.manager.export_resources_pool_client_slice(client_slice_id, pool_id, start, end, units,
                                                                     ticket_properties, resource_properties,
                                                                     source_ticket_id, self.auth)
            self.last_status = result.status
            if self.last_status.get_code() == 0 and result.result is not None:
                return ID(result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def export_resources_pool(self, pool_id: ID, start: datetime, end: datetime, units: int,
                              ticket_properties: dict, resource_properties: dict, source_ticket_id: ID,
                              client: AuthToken) -> ID:
        try:
            result = self.manager.export_resources_pool(pool_id, start, end, units,
                                                        ticket_properties, resource_properties,
                                                        source_ticket_id, client, self.auth)
            self.last_status = result.status
            if self.last_status.get_code() == 0 and result.result is not None:
                return ID(result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def export_resources_client_slice(self, client_slice_id: ID, rtype: ResourceType, start: datetime, end: datetime,
                                      units: int, ticket_properties: dict, resource_properties: dict,
                                      source_ticket_id: ID) -> ID:
        try:
            result = self.manager.export_resources_client_slice(client_slice_id, rtype, start, end, units,
                                                                ticket_properties, resource_properties,
                                                                source_ticket_id, self.auth)
            self.last_status = result.status
            if self.last_status.get_code() == 0 and result.result is not None:
                return ID(result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def export_resources(self, rtype: ResourceType, start: datetime, end: datetime, units: int,
                         ticket_properties: dict, resource_properties: dict, source_ticket_id: ID,
                         client: AuthToken) -> ID:
        try:
            result = self.manager.export_resources(rtype, start, end, units,
                                                   ticket_properties, resource_properties,
                                                   source_ticket_id, client, self.auth)
            self.last_status = result.status
            if self.last_status.get_code() == 0 and result.result is not None:
                return ID(result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def clone(self) -> IMgmtActor:
        return LocalServerActor(self.manager, self.auth)