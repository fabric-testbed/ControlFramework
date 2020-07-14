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

from fabric.actor.core import Constants
from fabric.actor.core import KafkaActor
from fabric.actor.core import KafkaBroker
from fabric.actor.core.manage.kafka.KafkaMgmtMessageProcessor import KafkaMgmtMessageProcessor
from fabric.actor.core.util import ID
from manage_cli.ConfigProcessor import ConfigProcessor
from message_bus.messages.GetReservationsResponse import GetReservationsResponseAvro
from message_bus.messages.GetSlicesResponseAvro import GetSlicesResponseAvro


class MainShell:
    PATH = "./config/manage-cli.yaml"

    def __init__(self):
        self.config_processor = ConfigProcessor(self.PATH)
        self.message_processor = None
        self.actor_cache = {}
        self.lock = threading.Lock()
        self.auth = None
        self.logger = None
        self.conf = None
        self.key_schema = None
        self.val_schema = None

    def set_up_kafka_conf_and_schema(self):
        try:
            self.conf = {'bootstrap.servers': self.config_processor.get_kafka_server(),
                         'schema.registry.url': self.config_processor.get_kafka_schema_registry()}

            from confluent_kafka import avro

            file = open(self.config_processor.get_kafka_key_schema(), "r")
            key_bytes = file.read()
            file.close()
            self.key_schema = avro.loads(key_bytes)
            file = open(self.config_processor.get_kafka_value_schema(), "r")
            val_bytes = file.read()
            file.close()
            self.val_schema = avro.loads(val_bytes)
        except Exception as e:
            traceback.print_exc()
            self.logger.error("Exception occurred while loading schemas {}".format(e))
            raise e

    def initialize(self):
        self.config_processor.process()

        self.logger = self.make_logger()

        self.set_up_kafka_conf_and_schema()

        self.setup_kafka_processor()

        self.load_actor_cache()

    def setup_kafka_processor(self):
        consumer_conf = self.conf
        consumer_conf['group.id'] = "fabric_cf"
        consumer_conf['auto.offset.reset'] = "earliest"
        topics = [self.config_processor.get_kafka_topic()]

        self.message_processor = KafkaMgmtMessageProcessor(consumer_conf, self.key_schema, self.val_schema, topics,
                                                           logger=self.logger)

    def load_actor_cache(self):
        peers = self.config_processor.get_peers()
        if peers is not None:
            for p in peers:
                # TODO Actor Live Check
                mgmt_actor= None
                if p.get_type() == Constants.BROKER:
                    mgmt_actor = KafkaBroker(ID(p.get_guid()), p.get_kafka_topic(), self.config_processor.get_auth(),
                                            self.config_processor.get_kafka_config(), self.logger,
                                            self.message_processor)
                else:
                    mgmt_actor = KafkaActor(ID(p.get_guid()), p.get_kafka_topic(), self.config_processor.get_auth(),
                                        self.config_processor.get_kafka_config(), self.logger, self.message_processor)
                try:
                    self.lock.acquire()
                    self.logger.debug("Added actor {} to cache".format(p.get_name()))
                    self.actor_cache[p.get_name()] = mgmt_actor
                finally:
                    self.lock.release()
        else:
            self.logger.debug("No peers available")

    def get_mgmt_actor(self, name: str) -> KafkaActor:

        try:
            self.lock.acquire()
            return self.actor_cache.get(name, None)
        finally:
            self.lock.release()

    def make_logger(self):
        """
        Detects the path and level for the log file from the actor config and sets
        up a logger. Instead of detecting the path and/or level from the
        config, a custom path and/or level for the log file can be passed as
        optional arguments.

        :param log_path: Path to custom log file
        :param log_level: Custom log level
        :return: logging.Logger object
        """

        # Get the log path
        if self.config_processor is None:
            raise RuntimeError('No config information available')

        log_path = self.config_processor.get_log_dir() + '/' + self.config_processor.get_log_file()

        if log_path is None:
            raise RuntimeError('The log file path must be specified in config or passed as an argument')

        # Get the log level
        log_level = self.config_processor.get_log_level()
        if log_level is None:
            log_level = logging.INFO

        # Set up the root logger
        log = logging.getLogger(self.config_processor.get_log_name())
        log.setLevel(log_level)
        log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'

        logging.basicConfig(format=log_format, filename=log_path)

        return log

    def start(self):
        self.initialize()
        self.message_processor.start()

    def stop(self):
        self.message_processor.stop()

    def get_slices(self, actor_name: str) -> GetSlicesResponseAvro:
        actor = self.get_mgmt_actor(actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(self.config_processor.get_kafka_topic())
            return actor.get_slices()
        except Exception as e:
            traceback.print_exc()

    def get_slice(self, actor_name: str, slice_id: str) -> GetSlicesResponseAvro:
        actor = self.get_mgmt_actor(actor_name)

        if actor is None or slice_id is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(self.config_processor.get_kafka_topic())
            return actor.get_slice(ID(slice_id))
        except Exception as e:
            traceback.print_exc()
            print(e)

    def get_reservations(self, actor_name: str) -> GetReservationsResponseAvro:
        actor = self.get_mgmt_actor(actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(self.config_processor.get_kafka_topic())
            return actor.get_reservations()
        except Exception as e:
            traceback.print_exc()

    def claim_resources(self, broker: str, am_guid: str, rid: str):
        actor = self.get_mgmt_actor(broker)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(broker))
        try:
            actor.prepare(self.config_processor.get_kafka_topic())
            return actor.claim_resources(ID(am_guid), ID(rid))
        except Exception as e:
            traceback.print_exc()


class MainShellSingleton:
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise Exception("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = MainShell()
        return self.__instance

    get = classmethod(get)


if __name__ == '__main__':

    MainShellSingleton.get().start()

    broker_slice_id_list = []

    result = MainShellSingleton.get().get_slices("fabric-vm-am")
    print("Get Slices Response Status: {}".format(result.status))
    if result.status.get_code() == 0 and result.slices is not None:
        for s in result.slices:
            s.print()
            if s.get_slice_name() == "fabric-broker":
                broker_slice_id_list.append(s.get_slice_id())

    claim_rid_list = {}
    result = MainShellSingleton.get().get_reservations("fabric-vm-am")
    print("Get Reservations Response Status: {}".format(result.status))
    if result.status.get_code() == 0 and result.reservations is not None:
        for r in result.reservations:
            if r.get_slice_id() in broker_slice_id_list:
                claim_rid_list[r.get_reservation_id()] = r.get_resource_type()
                r.print()

    print("List of reservations to be claimed: {}".format(claim_rid_list))

    for k,v in claim_rid_list.items():
        print("Claiming Reservation# {} for resource_type: {}".format(k, v))
        result = MainShellSingleton.get().claim_resources("fabric-broker", "fabric-vm-am-guid", k)
        print("Claim Response: {}".format(result))

    MainShellSingleton.get().stop()
