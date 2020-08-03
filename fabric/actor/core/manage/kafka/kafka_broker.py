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
from typing import TYPE_CHECKING


import traceback

from fabric.actor.core.common.constants import Constants, ErrorCodes
from fabric.actor.core.apis.i_mgmt_broker import IMgmtBroker
from fabric.actor.core.manage.kafka.kafka_mgmt_message_processor import KafkaMgmtMessageProcessor
from fabric.actor.core.manage.kafka.kafka_server_actor import KafkaServerActor
from fabric.actor.core.util.id import ID
from fabric.message_bus.messages.auth_avro import AuthAvro
from fabric.message_bus.messages.claim_resources_avro import ClaimResourcesAvro
from fabric.message_bus.messages.reservation_mng import ReservationMng
from fabric.message_bus.messages.result_avro import ResultAvro

if TYPE_CHECKING:
    from fabric.message_bus.producer import AvroProducerApi


class KafkaBroker(KafkaServerActor, IMgmtBroker):
    def __init__(self, guid: ID, kafka_topic: str, auth: AuthAvro, logger,
                 message_processor: KafkaMgmtMessageProcessor, producer: AvroProducerApi = None):
        super().__init__(guid, kafka_topic, auth, logger, message_processor, producer)

    def claim_resources_slice(self, broker: ID, slice_id: ID, rid: ID) -> ReservationMng:

        self.clear_last()
        status = ResultAvro()
        ret_val = None

        try:
            request = ClaimResourcesAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.broker_id = str(broker)
            request.reservation_id = str(rid)
            request.slice_id = str(slice_id)
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic

            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0 and message_wrapper.response.reservations is not None and len(
                            message_wrapper.response.reservations) > 0:
                        ret_val = message_wrapper.response.reservations.__iter__().__next__()
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

        return ret_val

    def claim_resources(self, broker: ID, rid: ID) -> ReservationMng:
        self.clear_last()
        status = ResultAvro()

        try:
            request = ClaimResourcesAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.broker_id = str(broker)
            request.reservation_id = str(rid)
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic

            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0 and message_wrapper.response.reservations is not None and len(
                            message_wrapper.response.reservations) > 0:
                        ret_val = message_wrapper.response.reservations.__iter__().__next__()
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

        return ret_val