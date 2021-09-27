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
import logging
import threading
import traceback

from fabric_mb.message_bus.messages.auth_avro import AuthAvro

from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.manage.kafka.kafka_actor import KafkaActor
from fabric_cf.actor.core.manage.kafka.kafka_broker import KafkaBroker
from fabric_cf.actor.core.manage.kafka.kafka_mgmt_message_processor import KafkaMgmtMessageProcessor
from fabric_cf.actor.core.util.id import ID


class KafkaProcessor:
    def __init__(self):
        self.message_processor = None
        self.actor_cache = {}
        self.lock = threading.Lock()
        self.auth = AuthAvro()
        self.auth.name = "ait"
        self.auth.guid = "ait-guid"
        self.key_schema = None
        self.val_schema = None
        self.producer = None
        self.kafka_topic = "ait-topic"
        self.broker_name = "broker"
        self.broker_guid = "broker-guid"
        self.broker_topic = "broker-topic"
        self.orchestrator_name = "orchestrator"
        self.orchestrator_guid = "orchestrator-guid"
        self.orchestrator_topic = "orchestrator-topic"
        self.am_name = "site1-am"
        self.am_guid = "site1-am-guid"
        self.am_topic = "site1-am-topic"
        self.net_am_name = "net1-am"
        self.net_am_guid = "net1-am-guid"
        self.net_am_topic = "net1-am-topic"

        self.logger = None

    def set_logger(self, logger):
        self.logger = logger

    def setup_kafka(self):
        """
        Set up Kafka Producer and Consumer
        """
        producer_conf = {
                Constants.BOOTSTRAP_SERVERS: "152.54.15.56:29092",
                #Constants.BOOTSTRAP_SERVERS: "localhost:19092",
                Constants.SECURITY_PROTOCOL: "SSL",
                Constants.SSL_CA_LOCATION: "../../secrets/snakeoil-ca-1.crt",
                Constants.SSL_CERTIFICATE_LOCATION: "../../secrets/kafkacat1-ca1-signed.pem",
                Constants.SSL_KEY_LOCATION: "../../secrets/kafkacat1.client.key",
                Constants.SSL_KEY_PASSWORD: "fabric",
                #Constants.SCHEMA_REGISTRY_URL: "http://localhost:8081"
                Constants.SCHEMA_REGISTRY_URL: "http://152.54.15.56:8081"
        }

        self.key_schema = "../actor/test/schema/key.avsc"
        self.val_schema = "../actor/test/schema/message.avsc"

        from fabric_mb.message_bus.producer import AvroProducerApi
        self.producer = AvroProducerApi(producer_conf=producer_conf, key_schema_location=self.key_schema,
                                        value_schema_location=self.val_schema, logger=self.logger)

        consumer_conf = producer_conf
        consumer_conf['auto.offset.reset'] = 'earliest'
        consumer_conf[Constants.GROUP_ID] = 'ait'
        topics = [self.kafka_topic]

        self.message_processor = KafkaMgmtMessageProcessor(consumer_conf=consumer_conf,
                                                           key_schema_location=self.key_schema,
                                                           value_schema_location=self.val_schema, topics=topics,
                                                           logger=self.logger)

    def initialize(self):
        """
        Initialize the Kafka Processor
        """
        self.setup_kafka()

        self.load_actor_cache()

    def load_actor_cache(self):
        """
        Load the Actor Cache
        """
        try:
            self.lock.acquire()
            mgmt_actor = KafkaBroker(guid=ID(uid=self.broker_guid), kafka_topic=self.broker_topic, auth=self.auth,
                                     logger=self.logger, message_processor=self.message_processor, producer=self.producer)

            self.actor_cache[self.broker_name] = mgmt_actor

            mgmt_actor = KafkaActor(guid=ID(uid=self.am_guid), kafka_topic=self.am_topic, auth=self.auth,
                                    logger=self.logger, message_processor=self.message_processor, producer=self.producer)

            self.actor_cache[self.am_name] = mgmt_actor

            mgmt_actor = KafkaActor(guid=ID(uid=self.orchestrator_guid), kafka_topic=self.orchestrator_topic,
                                    auth=self.auth, logger=self.logger, message_processor=self.message_processor,
                                    producer=self.producer)

            self.actor_cache[self.orchestrator_name] = mgmt_actor
        finally:
            self.lock.release()

    def get_mgmt_actor(self, *, name: str) -> KafkaActor:
        """
        Get Management Actor from Cache
        @param name actor name
        @return Management Actor
        """

        try:
            self.lock.acquire()
            return self.actor_cache.get(name, None)
        finally:
            self.lock.release()

    def get_callback_topic(self) -> str:
        """
        Get Callback Topic
        @return callback topic
        """
        return self.kafka_topic

    def start(self):
        """
        Start Synchronous Kafka Processor
        """
        try:
            self.initialize()
            self.message_processor.start()
        except Exception as e:
            self.logger.debug(f"Failed to start Management Shell: {e}")
            self.logger.error(traceback.format_exc())
            raise e

    def stop(self):
        """
        Stop the Synchronous Kafka Processor
        """
        try:
            self.message_processor.stop()
        except Exception as e:
            self.logger.debug(f"Failed to stop Management Shell: {e}")
            self.logger.error(traceback.format_exc())


class KafkaProcessorSingleton:
    """
    Kafka Processor Singleton
    """
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise Exception("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = KafkaProcessor()
        return self.__instance

    get = classmethod(get)


