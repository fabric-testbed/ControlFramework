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

from fabric_mb.message_bus.messages.result_avro import ResultAvro

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.error import Error
from fabric_cf.actor.core.apis.i_component import IComponent
from fabric_cf.actor.core.manage.kafka.kafka_mgmt_message_processor import KafkaMgmtMessageProcessor
from fabric_cf.actor.core.manage.messages.protocol_proxy_mng import ProtocolProxyMng

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.auth_avro import AuthAvro
    from fabric_mb.message_bus.producer import AvroProducerApi
    from fabric_cf.actor.core.util.id import ID


class KafkaProxy(IComponent):
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
            traceback.print_exc()
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
        proto.set_protocol(protocol=Constants.protocol_kafka)
        proto.set_proxy_class(proxy_class=KafkaProxy.__class__.__name__)
        proto.set_proxy_module(proxy_module=KafkaProxy.__module__)
        result = [proto]
        return result

    def get_type_id(self) -> str:
        raise ManageException("Not implemented")
