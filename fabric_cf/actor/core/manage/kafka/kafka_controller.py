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
from typing import List

from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.broker_query_model_avro import BrokerQueryModelAvro
from fabric_mb.message_bus.messages.get_actors_request_avro import GetActorsRequestAvro
from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
from fabric_mb.message_bus.messages.get_reservation_units_request_avro import GetReservationUnitsRequestAvro
from fabric_mb.message_bus.messages.unit_avro import UnitAvro
from fim.user import GraphFormat

from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.manage.kafka.kafka_actor import KafkaActor
from fabric_cf.actor.core.util.id import ID


class KafkaController(KafkaActor, ABCMgmtControllerMixin):
    def clone(self):
        return KafkaController(guid=self.management_id,
                               kafka_topic=self.kafka_topic,
                               auth=self.auth, logger=self.logger,
                               message_processor=self.message_processor,
                               producer=self.producer)

    def add_broker(self, *, broker: ProxyAvro) -> bool:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_brokers(self, *, broker: ID = None, id_token: str = None) -> List[ProxyAvro]:
        request = GetActorsRequestAvro()
        request = self.fill_request_by_id_message(request=request, id_token=id_token, broker_id=broker)
        request.type = ActorType.Broker.name
        status, response = self.send_request(request)

        if status.code == 0:
            return response.proxies
        return None

    def get_broker_query_model(self, *, broker: ID, id_token: str, level: int,
                               graph_format: GraphFormat) -> BrokerQueryModelAvro:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def claim_delegations(self, *, broker: ID, did: ID) -> DelegationAvro:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def reclaim_delegations(self, *, broker: ID, did: ID) -> DelegationAvro:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_reservation_units(self, *, rid: ID, id_token: str = None) -> List[UnitAvro]:
        request = GetReservationUnitsRequestAvro()
        request = self.fill_request_by_id_message(request=request, id_token=id_token, rid=rid)
        status, response = self.send_request(request)

        if status.code == 0:
            return response.units
        return None

    def add_reservation(self, *, reservation: TicketReservationAvro) -> ID:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def add_reservations(self, *, reservations: List[ReservationMng]) ->list:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def demand_reservation(self, *, reservation: ReservationMng) -> bool:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def demand_reservation_rid(self, *, rid: ID) -> bool:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def extend_reservation(self, *, reservation: ID, new_end_time: datetime) -> bool:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def modify_reservation(self, *, rid: ID, modify_properties: dict) -> bool:
        raise ManageException(Constants.NOT_IMPLEMENTED)
