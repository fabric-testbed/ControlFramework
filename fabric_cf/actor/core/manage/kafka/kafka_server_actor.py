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

from fim.graph.abc_property_graph import ABCPropertyGraph

from fabric_mb.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric_mb.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric_mb.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.slice_avro import SliceAvro

from fabric_cf.actor.core.apis.abc_mgmt_server_actor import ABCMgmtServerActor
from fabric_cf.actor.core.apis.abc_reservation_mixin import ReservationCategory
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.kernel.slice import SliceTypes
from fabric_cf.actor.core.manage.kafka.kafka_actor import KafkaActor
from fabric_cf.actor.core.manage.messages.client_mng import ClientMng
from fabric_cf.actor.security.auth_token import AuthToken


if TYPE_CHECKING:
    from fabric_cf.actor.core.util.id import ID


class KafkaServerActor(KafkaActor, ABCMgmtServerActor):
    def get_broker_reservations(self, *, id_token: str = None) -> List[ReservationMng]:
        request = GetReservationsRequestAvro()
        request = self.fill_request_by_id_message(request=request, id_token=id_token)
        request.reservation_type = ReservationCategory.Broker.name
        status, response = self.send_request(request)

        if status.code == 0:
            return response.reservations
        return None

    def get_inventory_reservations(self, *, slice_id: ID = None, id_token: str = None) -> List[ReservationMng]:
        request = GetReservationsRequestAvro()
        request = self.fill_request_by_id_message(request=request, id_token=id_token)
        request.reservation_type = ReservationCategory.Inventory.name
        status, response = self.send_request(request)

        if status.code == 0:
            return response.reservations
        return None

    def get_client_reservations(self, *, slice_id: ID = None, id_token: str = None) -> List[ReservationMng]:
        request = GetReservationsRequestAvro()
        request = self.fill_request_by_id_message(request=request, id_token=id_token)
        request.reservation_type = ReservationCategory.Client.name
        status, response = self.send_request(request)

        if status.code == 0:
            return response.reservations
        return None

    def get_inventory_slices(self, *, id_token: str = None) -> List[SliceAvro]:
        request = GetReservationsRequestAvro()
        request = self.fill_request_by_id_message(request=request, id_token=id_token)
        request.type = SliceTypes.InventorySlice.name
        status, response = self.send_request(request)

        if status.code == 0:
            return response.slices
        return None

    def get_client_slices(self, *, id_token: str = None) -> List[SliceAvro]:
        request = GetSlicesRequestAvro()
        request = self.fill_request_by_id_message(request=request, id_token=id_token)
        request.type = SliceTypes.ClientSlice.name

        status, response = self.send_request(request)
        if response is not None:
            return response.slices

        return None

    def add_client_slice(self, *, slice_mng: SliceAvro) -> ID:
        ret_val = None
        request = AddSliceAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.callback_topic = self.callback_topic
        request.message_id = str(ID())
        request.slice_obj = slice_mng

        status, response = self.send_request(request)

        if status.code == 0:
            ret_val = ID(uid=response.get_result())

        return ret_val

    def advertise_resources(self, *, delegation: ABCPropertyGraph, delegation_name: str, client: AuthToken) -> ID:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_clients(self, *, guid: ID = None, id_token: str = None) -> List[ClientMng]:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def register_client(self, *, client: ClientMng, kafka_topic: str) -> bool:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def unregister_client(self, *, guid: ID) -> bool:
        raise ManageException(Constants.NOT_IMPLEMENTED)
