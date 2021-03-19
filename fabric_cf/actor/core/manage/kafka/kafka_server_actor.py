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

from fabric_mb.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric_mb.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric_mb.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro

from fabric_cf.actor.core.apis.abc_mgmt_server_actor import ABCMgmtServerActor
from fabric_cf.actor.core.apis.abc_reservation_mixin import ReservationCategory
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.kernel.slice import SliceTypes
from fabric_cf.actor.core.manage.kafka.kafka_actor import KafkaActor
from fabric_cf.actor.core.manage.messages.client_mng import ClientMng
from fabric_cf.actor.security.auth_token import AuthToken


if TYPE_CHECKING:
    from fabric_cf.actor.core.util.id import ID


class KafkaServerActor(KafkaActor, ABCMgmtServerActor):
    def get_broker_reservations(self, *, id_token: str = None) -> List[ReservationMng]:
        self.clear_last()
        status = ResultAvro()
        rret_val = None

        try:
            request = GetReservationsRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic
            request.type = ReservationCategory.Broker.name
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
                        rret_val = message_wrapper.response.reservations
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

    def get_inventory_reservations(self, *, slice_id: ID = None, id_token: str = None) -> List[ReservationMng]:
        self.clear_last()
        status = ResultAvro()
        rret_val = None

        try:
            request = GetReservationsRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic
            request.type = ReservationCategory.Inventory.name
            request.id_token = None
            request.slice_id = slice_id

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
                        rret_val = message_wrapper.response.reservations
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

    def get_client_reservations(self, *, slice_id: ID = None, id_token: str = None) -> List[ReservationMng]:
        self.clear_last()
        status = ResultAvro()
        rret_val = None

        try:
            request = GetReservationsRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic
            request.type = ReservationCategory.Client.name
            request.id_token = None
            request.slice_id = slice_id

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
                        rret_val = message_wrapper.response.reservations
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

    def get_inventory_slices(self, *, id_token: str = None) -> List[SliceAvro]:
        self.clear_last()

        status = ResultAvro()
        rret_val = None

        try:
            request = GetSlicesRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.type = SliceTypes.InventorySlice.name
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
                        rret_val = message_wrapper.response.slices

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

    def get_client_slices(self, *, id_token: str = None) -> List[SliceAvro]:
        self.clear_last()

        status = ResultAvro()
        rret_val = None

        try:
            request = GetSlicesRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.type = SliceTypes.ClientSlice.name
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
                        rret_val = message_wrapper.response.slices

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

    def add_client_slice(self, *, slice_mng: SliceAvro) -> ID:
        rret_val = None
        self.clear_last()
        status = ResultAvro()

        try:
            request = AddSliceAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.slice_obj = slice_mng
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
                        rret_val = ID(uid=message_wrapper.response.get_result())
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

    def advertise_resources(self, *, delegation: ABCPropertyGraph, delegation_name: str, client: AuthToken) -> ID:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_clients(self, *, guid: ID = None, id_token: str = None) -> List[ClientMng]:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def register_client(self, *, client: ClientMng, kafka_topic: str) -> bool:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def unregister_client(self, *, guid: ID) -> bool:
        raise ManageException(Constants.NOT_IMPLEMENTED)
