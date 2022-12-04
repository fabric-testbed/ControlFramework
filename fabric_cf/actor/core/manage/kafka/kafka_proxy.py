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
from typing import List, Tuple, Any

from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.request_by_id_record import RequestByIdRecord
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.producer import AvroProducerApi

from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.error import Error
from fabric_cf.actor.core.apis.abc_component import ABCComponent
from fabric_cf.actor.core.manage.kafka.kafka_mgmt_message_processor import KafkaMgmtMessageProcessor
from fabric_cf.actor.core.manage.messages.protocol_proxy_mng import ProtocolProxyMng
from fabric_cf.actor.core.util.id import ID


class KafkaProxy(ABCComponent):
    def __init__(self, *, guid: ID, kafka_topic: str, auth: AuthAvro, logger,
                 message_processor: KafkaMgmtMessageProcessor, producer: AvroProducerApi = None):
        self.management_id = guid
        self.auth = auth
        self.logger = logger
        self.kafka_topic = kafka_topic
        self.producer = None
        self.last_status = None
        self.last_exception = None
        self.callback_topic = None
        if producer is None:
            self.setup_kafka_producer()
        else:
            self.producer = producer
        self.message_processor = message_processor

    def setup_kafka_producer(self):
        try:
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            self.producer = GlobalsSingleton.get().get_kafka_producer()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.eror("Exception occurred while loading schemas {}".format(e))
            raise e

    def get_kafka_topic(self):
        return self.kafka_topic

    def clear_last(self):
        self.last_exception = None
        self.last_status = ResultAvro()

    def get_last_error(self) -> Error:
        return Error(status=self.last_status, e=self.last_exception)

    def get_protocols(self) -> List[ProtocolProxyMng]:
        proto = ProtocolProxyMng()
        proto.set_protocol(protocol=Constants.PROTOCOL_KAFKA)
        proto.set_proxy_class(proxy_class=KafkaProxy.__class__.__name__)
        proto.set_proxy_module(proxy_module=KafkaProxy.__module__)
        result = [proto]
        return result

    def get_type_id(self) -> str:
        raise ManageException("Not implemented")

    def fill_request_by_id_message(self, request: RequestByIdRecord, id_token: str = None, email: str = None,
                                   slice_id: ID = None, slice_name: str = None, reservation_state: int = None,
                                   rid: ID = None, delegation_id: str = None, broker_id: ID = None, type: str = None,
                                   site: str = None):
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.callback_topic = self.callback_topic
        request.message_id = str(ID())
        request.id_token = id_token
        request.email = email
        request.reservation_state = reservation_state
        request.delegation_id = delegation_id
        request.site = site
        request.type = type
        if slice_id is not None:
            request.slice_id = str(slice_id)
        if rid is not None:
            request.reservation_id = str(rid)
        if broker_id is not None:
            request.broker_id = str(broker_id)
        request.slice_name = slice_name

        return request

    def send_request(self, request: AbcMessageAvro) -> Tuple[ResultAvro, Any]:
        self.clear_last()

        status = ResultAvro()
        rret_val = None

        try:
            ret_val = self.producer.produce(topic=self.kafka_topic, record=request)

            self.logger.debug(Constants.MANAGEMENT_INTER_ACTOR_OUTBOUND_MESSAGE.format(request.get_message_name(),
                                                                                       self.kafka_topic))

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
                        rret_val = message_wrapper.response

            else:
                self.logger.debug(Constants.MANAGEMENT_INTER_ACTOR_MESSAGE_FAILED.format(
                    request.get_message_name(), self.kafka_topic))
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.interpret()

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.interpret(exception=e)
            status.details = traceback.format_exc()

        self.last_status = status

        return status, rret_val
