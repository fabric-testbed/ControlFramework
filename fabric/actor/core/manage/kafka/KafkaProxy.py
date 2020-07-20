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

from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.manage.Error import Error
from fabric.actor.core.apis.IComponent import IComponent
from fabric.actor.core.manage.kafka.KafkaMgmtMessageProcessor import KafkaMgmtMessageProcessor
from fabric.actor.core.manage.messages.ProtocolProxyMng import ProtocolProxyMng
from fabric.message_bus.messages.ResultAvro import ResultAvro

if TYPE_CHECKING:
    from fabric.actor.core.util.ID import ID
    from fabric.message_bus.messages.AuthAvro import AuthAvro


class KafkaProxy(IComponent):
    def __init__(self, guid: ID, kafka_topic: str, auth: AuthAvro, kafka_config: dict, logger,
                 message_processor: KafkaMgmtMessageProcessor):
        self.management_id = guid
        self.auth = auth
        self.logger = logger
        self.kafka_topic = kafka_topic
        self.producer = None
        self.last_status = None
        self.last_exception = None
        self.callback_topic = None
        self.setup_kafka_producer(kafka_config)
        self.message_processor = message_processor

    def setup_kafka_producer(self, kafka_config: dict):
        try:
            boot_strap_server = kafka_config.get(Constants.PropertyConfKafkaServer, None)
            schema_registry = kafka_config.get(Constants.PropertyConfKafkaSchemaRegistry, None)
            key_schema_location = kafka_config.get(Constants.PropertyConfKafkaKeySchema, None)
            value_schema_location = kafka_config.get(Constants.PropertyConfKafkaValueSchema, None)

            if boot_strap_server is None or schema_registry is None or key_schema_location is None or \
                    value_schema_location is None:
                raise Exception("Invalid Arguments")

            conf = {'bootstrap.servers': boot_strap_server,
                    'schema.registry.url': schema_registry}

            from confluent_kafka import avro
            file = open(key_schema_location, "r")
            key_bytes = file.read()
            file.close()
            key_schema = avro.loads(key_bytes)
            file = open(value_schema_location, "r")
            val_bytes = file.read()
            file.close()
            val_schema = avro.loads(val_bytes)
            from fabric.message_bus.producer import AvroProducerApi
            self.producer = AvroProducerApi(conf, key_schema, val_schema, self.logger)
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
        return Error(self.last_status, self.last_exception)

    def get_protocols(self) -> list:
        proto = ProtocolProxyMng()
        proto.set_protocol(Constants.ProtocolKafka)
        proto.set_proxy_class(KafkaProxy.__class__.__name__)
        proto.set_proxy_module(KafkaProxy.__module__)
        result = [proto]
        return result

    def get_type_id(self) -> str:
        raise Exception("Not implemented")