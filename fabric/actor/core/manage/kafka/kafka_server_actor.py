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

from fabric.actor.core.apis.i_mgmt_server_actor import IMgmtServerActor
from fabric.actor.core.apis.i_reservation import ReservationCategory
from fabric.actor.core.common.constants import Constants, ErrorCodes
from fabric.actor.core.kernel.slice import SliceTypes
from fabric.actor.core.manage.kafka.kafka_actor import KafkaActor
from fabric.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric.message_bus.messages.reservation_mng import ReservationMng
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.messages.slice_avro import SliceAvro

if TYPE_CHECKING:
    from fabric.actor.core.manage.kafka.kafka_mgmt_message_processor import KafkaMgmtMessageProcessor
    from fabric.actor.core.util.id import ID
    from fabric.message_bus.messages.auth_avro import AuthAvro
    from fabric.message_bus.producer import AvroProducerApi


class KafkaServerActor(KafkaActor, IMgmtServerActor):
    def __init__(self, *, guid: ID, kafka_topic: str, auth: AuthAvro, logger,
                 message_processor: KafkaMgmtMessageProcessor, producer: AvroProducerApi = None):
        super().__init__(guid=guid, kafka_topic=kafka_topic, auth=auth, logger=logger,
                         message_processor=message_processor, producer=producer)

    def get_broker_reservations(self) -> List[ReservationMng]:
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

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.reservations
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def get_inventory_slices(self) -> List[SliceAvro]:
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

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.slices

            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def get_inventory_reservations(self) -> List[ReservationMng]:
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

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.reservations
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def get_inventory_reservations_by_slice_id(self, *, slice_id: ID) -> List[ReservationMng]:
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
            request.slice_id = str(slice_id)

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.reservations
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def get_client_slices(self) -> List[SliceAvro]:
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

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.slices

            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
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

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = ID(id=message_wrapper.response.get_result())
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val