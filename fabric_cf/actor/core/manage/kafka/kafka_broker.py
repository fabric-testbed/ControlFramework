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


from fabric_mb.message_bus.messages.add_reservation_avro import AddReservationAvro
from fabric_mb.message_bus.messages.add_reservations_avro import AddReservationsAvro
from fabric_mb.message_bus.messages.claim_resources_avro import ClaimResourcesAvro
from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.demand_reservation_avro import DemandReservationAvro
from fabric_mb.message_bus.messages.extend_reservation_avro import ExtendReservationAvro
from fabric_mb.message_bus.messages.get_actors_request_avro import GetActorsRequestAvro
from fabric_mb.message_bus.messages.get_broker_query_model_request_avro import GetBrokerQueryModelRequestAvro
from fabric_mb.message_bus.messages.broker_query_model_avro import BrokerQueryModelAvro
from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
from fabric_mb.message_bus.messages.reclaim_resources_avro import ReclaimResourcesAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.reservation_predecessor_avro import ReservationPredecessorAvro
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fim.slivers.base_sliver import BaseSliver
from fim.user import GraphFormat

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.apis.abc_mgmt_broker_mixin import ABCMgmtBrokerMixin
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.kafka.kafka_server_actor import KafkaServerActor
from fabric_cf.actor.core.util.id import ID


class KafkaBroker(KafkaServerActor, ABCMgmtBrokerMixin):
    def add_reservation(self, *, reservation: TicketReservationAvro) -> ID:
        request = AddReservationAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.message_id = str(ID())
        request.callback_topic = self.callback_topic
        request.reservation_obj = reservation
        status, response = self.send_request(request)

        if status.code == 0:
            return response.result_str

    def add_reservations(self, *, reservations: list) -> List[TicketReservationAvro]:
        request = AddReservationsAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.message_id = str(ID())
        request.callback_topic = self.callback_topic
        request.reservation_list = reservations
        status, response = self.send_request(request)

        if status.code == 0:
            return response.result

        return None

    def demand_reservation(self, *, reservation: ReservationMng) -> bool:
        request = DemandReservationAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.message_id = str(ID())
        request.callback_topic = self.callback_topic
        request.reservation_obj = reservation
        status, response = self.send_request(request)

        return status.code == 0

    def demand_reservation_rid(self, *, rid: ID) -> bool:
        request = DemandReservationAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.message_id = str(ID())
        request.callback_topic = self.callback_topic
        request.reservation_id = str(rid)
        status, response = self.send_request(request)

        return status.code == 0

    def get_brokers(self, *, broker: ID = None, id_token: str = None) -> List[ProxyAvro]:
        request = GetActorsRequestAvro()
        request = self.fill_request_by_id_message(request=request, id_token=id_token, broker_id=broker)
        status, response = self.send_request(request)

        if status.code == 0:
            return response.proxies
        return None

    def get_broker_query_model(self, *, broker: ID, id_token: str, level: int,
                               graph_format: GraphFormat) -> BrokerQueryModelAvro:
        request = GetBrokerQueryModelRequestAvro()
        request.id_token = id_token
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.message_id = str(ID())
        request.callback_topic = self.callback_topic
        request.broker_id = str(broker)
        request.level = level
        request.graph_format = graph_format.value
        status, response = self.send_request(request)

        if status.code == 0:
            return response.model
        return None

    def extend_reservation(self, *, reservation: ID, new_end_time: datetime, sliver: BaseSliver,
                           dependencies: List[ReservationPredecessorAvro] = None) -> bool:
        request = ExtendReservationAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.message_id = str(ID())
        request.callback_topic = self.callback_topic
        request.rid = str(reservation)
        request.sliver = sliver

        status, response = self.send_request(request)

        return status.code == 0

    def claim_delegations(self, *, broker: ID, did: ID) -> DelegationAvro:

        request = ClaimResourcesAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.broker_id = str(broker)
        request.delegation_id = did
        request.message_id = str(ID())
        request.callback_topic = self.callback_topic

        status, response = self.send_request(request)

        if status.code == 0 and response.delegations is not None and len(response.delegations) > 0:
            return next(iter(response.delegations))

        return None

    def reclaim_delegations(self, *, broker: ID, did: ID) -> DelegationAvro:
        request = ReclaimResourcesAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.broker_id = str(broker)
        request.delegation_id = did
        request.message_id = str(ID())
        request.callback_topic = self.callback_topic

        status, response = self.send_request(request)

        if status.code == 0 and response.delegations is not None and len(response.delegations) > 0:
            return next(iter(response.delegations))

        return None

    def clone(self):
        return KafkaBroker(guid=self.management_id,
                           kafka_topic=self.kafka_topic,
                           auth=self.auth, logger=self.logger,
                           message_processor=self.message_processor,
                           producer=self.producer)

    def add_broker(self, *, broker: ProxyAvro) -> bool:
        raise ManageException(Constants.NOT_IMPLEMENTED)
