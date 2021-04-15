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
from datetime import datetime
from typing import List

from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.pool_info_avro import PoolInfoAvro
from fabric_mb.message_bus.messages.get_actors_request_avro import GetActorsRequestAvro
from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
from fabric_mb.message_bus.messages.get_reservation_units_request_avro import GetReservationUnitsRequestAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.unit_avro import UnitAvro

from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
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
        self.clear_last()
        status = ResultAvro()
        rret_val = None

        try:
            request = GetActorsRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic
            request.type = ActorType.Broker.name
            request.id_token = id_token
            request.broker_id = broker

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug(Constants.MANAGEMENT_INTER_ACTOR_OUTBOUND_MESSAGE.format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.MANAGEMENT_API_TIMEOUT_IN_SECONDS)

                if not message_wrapper.done:
                    self.logger.debug(Constants.MANAGEMENT_API_TIMEOUT_OCCURRED)
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.interpret()
                else:
                    self.logger.debug(Constants.MANAGEMENT_INTER_ACTOR_INBOUND_MESSAGE.format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.proxies
            else:
                self.logger.debug(Constants.MANAGEMENT_INTER_ACTOR_MESSAGE_FAILED.format(
                    request.name, self.kafka_topic))
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.interpret()

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.interpret(exception=e)
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def get_pool_info(self, *, broker: ID, id_token: str, level: int) -> List[PoolInfoAvro]:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def claim_delegations(self, *, broker: ID, did: ID, id_token: str = None) -> DelegationAvro:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def reclaim_delegations(self, *, broker: ID, did: ID, id_token: str = None) -> DelegationAvro:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_reservation_units(self, *, rid: ID, id_token: str = None) -> List[UnitAvro]:
        self.clear_last()
        status = ResultAvro()
        rret_val = None

        try:
            request = GetReservationUnitsRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic
            request.reservation_id = str(rid)
            request.id_token = id_token

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug(Constants.MANAGEMENT_INTER_ACTOR_OUTBOUND_MESSAGE.format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.MANAGEMENT_API_TIMEOUT_IN_SECONDS)

                if not message_wrapper.done:
                    self.logger.debug(Constants.MANAGEMENT_API_TIMEOUT_OCCURRED)
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.interpret()
                else:
                    self.logger.debug(Constants.MANAGEMENT_INTER_ACTOR_INBOUND_MESSAGE.format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.units
            else:
                self.logger.debug(Constants.MANAGEMENT_INTER_ACTOR_MESSAGE_FAILED.format(
                    request.name, self.kafka_topic))
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.interpret()

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.interpret(exception=e)
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

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
