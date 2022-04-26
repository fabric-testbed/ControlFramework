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
from typing import TYPE_CHECKING

from fabric_mb.message_bus.messages.failed_rpc_avro import FailedRpcAvro
from fabric_mb.message_bus.messages.query_avro import QueryAvro
from fabric_mb.message_bus.messages.query_result_avro import QueryResultAvro
from fabric_mb.message_bus.producer import AvroProducerApi

from fabric_cf.actor.core.common.exceptions import ProxyException
from fabric_cf.actor.core.proxies.kafka.translate import Translate
from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.rpc_request_state import RPCRequestState
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.proxies.proxy import Proxy

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.util.id import ID


class KafkaProxyRequestState(RPCRequestState):
    def __init__(self):
        super().__init__()
        self.callback_topic = None
        self.reservation = None
        self.udd = None
        self.query = None
        self.request_id = None
        self.failed_reservation_id = None
        self.failed_request_type = None
        self.error_detail = None


class KafkaProxy(Proxy, ABCCallbackProxy):
    TypeDefault = 0
    TypeReturn = 1
    TypeBroker = 2
    TypeSite = 3

    def __init__(self, *, kafka_topic: str, identity: AuthToken, logger):
        super().__init__(auth=identity)
        self.kafka_topic = kafka_topic
        self.logger = logger
        self.proxy_type = Constants.PROTOCOL_KAFKA
        self.type = self.TypeDefault

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None

    def execute(self, *, request: ABCRPCRequestState, producer: AvroProducerApi):
        avro_message = None
        if request.get_type() == RPCRequestType.Query:
            avro_message = QueryAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.properties = request.query
            avro_message.callback_topic = request.callback_topic
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        elif request.get_type() == RPCRequestType.QueryResult:
            avro_message = QueryResultAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.request_id = str(request.request_id)
            avro_message.properties = request.query
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        elif request.get_type() == RPCRequestType.FailedRPC:
            avro_message = FailedRpcAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.request_id = str(request.request_id)
            avro_message.request_type = request.failed_request_type.value
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

            if request.failed_reservation_id is not None:
                avro_message.reservation_id = request.failed_reservation_id
            else:
                avro_message.reservation_id = ""
            avro_message.error_details = request.error_details

        else:
            raise ProxyException("Unsupported RPC: type={}".format(request.get_type()))

        if producer is not None and producer.produce(topic=self.kafka_topic, record=avro_message):
            self.logger.debug("Message {} written to {}".format(avro_message.name, self.kafka_topic))
        else:
            self.logger.error("Failed to send message {} to {} via producer {}".format(avro_message.name,
                                                                                       self.kafka_topic, producer))

    def prepare_query(self, *, callback: ABCCallbackProxy, query: dict, caller: AuthToken):
        request = KafkaProxyRequestState()
        request.query = query
        request.callback_topic = callback.get_kafka_topic()
        request.caller = caller
        return request

    def prepare_query_result(self, *, request_id: str, response, caller: AuthToken) -> ABCRPCRequestState:
        request = KafkaProxyRequestState()
        request.query = response
        request.request_id = request_id
        request.caller = caller
        return request

    def prepare_failed_request(self, *, request_id: str, failed_request_type,
                               failed_reservation_id: ID, error: str, caller: AuthToken) -> ABCRPCRequestState:
        request = KafkaProxyRequestState()
        request.failed_request_type = failed_request_type
        request.failed_reservation_id = failed_reservation_id
        request.error_detail = error
        request.request_id = request_id
        request.caller = caller
        return request

    def get_kafka_topic(self):
        return self.kafka_topic
