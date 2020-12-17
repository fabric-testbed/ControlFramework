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

import sched
import sys
import threading
import traceback
from logging.handlers import RotatingFileHandler
from typing import TYPE_CHECKING

import logging
import os

from fabric_cf.actor.core.common.exceptions import InitializationException
from fabric_cf.actor.core.container.event_manager import EventManager
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.container.container import Container

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_actor_container import IActorContainer
    from fabric_cf.actor.boot.configuration import Configuration


class Globals:
    config_file = Constants.configuration_file

    def __init__(self):
        self.config = None
        self.log = None
        self.initialized = False
        self.started = False
        self.start_completed = False
        self.event_manager = EventManager()
        self.container = None
        self.properties = None
        self.timer_scheduler = sched.scheduler()
        self.timer_thread = None
        self.timer_condition = threading.Condition()
        self.lock = threading.Lock()

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
        if self.config is None:
            raise RuntimeError('No config information available')
        log_config = self.config.get_global_config().get_logging()
        if log_config is None:
            raise RuntimeError('No logging  config information available')

        log_path = None
        if Constants.property_conf_log_directory in log_config and Constants.property_conf_log_file in log_config:
            log_path = log_config[Constants.property_conf_log_directory] + '/' + log_config[Constants.property_conf_log_file]

        if log_path is None:
            raise RuntimeError('The log file path must be specified in config or passed as an argument')

        # Get the log level
        log_level = None
        if Constants.property_conf_log_level in log_config:
            log_level = log_config.get(Constants.property_conf_log_level, None)

        if log_level is None:
            log_level = logging.INFO

        # Set up the root logger
        log = logging.getLogger(log_config.get(Constants.property_conf_logger, None))
        log.setLevel(log_level)
        log_format = \
            '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'

        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        backup_count = log_config.get(Constants.property_conf_log_retain, None)
        max_log_size = log_config.get(Constants.property_conf_log_size, None)

        file_handler = RotatingFileHandler(log_path, backupCount=int(backup_count), maxBytes=int(max_log_size))

        logging.basicConfig(handlers=[file_handler], format=log_format)

        return log

    @staticmethod
    def delete_super_block():
        """
        Delete Super block file
        """
        if os.path.isfile(Constants.superblock_location):
            os.remove(Constants.superblock_location)

    def fail(self, *, e: Exception):
        """
        Fail the Actor
        @param e exception
        """
        self.log.error("Critical error: Actor failed to initialize {}".format(e))
        sys.exit(-1)

    def initialize(self):
        """
        Initialize the container and actor
        """
        try:
            self.lock.acquire()
            if not self.initialized:
                self.load_config()
                self.log = self.make_logger()
                self.log.info("Checking if connection to Kafka broker can be established")
                admin_kafka_client = self.get_kafka_admin_client()
                admin_kafka_client.list_topics()
                self.log.info("Connection to Kafka broker established successfully")
                self.log.info("Main initialization complete.")
                self.initialized = True
        finally:
            self.lock.release()

    def load_config(self):
        """
        Load the configuration
        """
        try:
            from fabric_cf.actor.boot.configuration_loader import ConfigurationLoader
            loader = ConfigurationLoader(path=self.config_file)
            self.config = loader.read_configuration()
        except Exception as e:
            raise RuntimeError("Unable to parse configuration file {}".format(e))

    def get_container(self) -> IActorContainer:
        """
        Get the container
        @return container
        """
        if not self.initialized:
            raise InitializationException(Constants.uninitialized_state)
        return self.container

    def get_config(self) -> Configuration:
        """
        Get the configuration
        @return config
        """
        if not self.initialized:
            raise InitializationException(Constants.uninitialized_state)
        return self.config

    def get_kafka_config_admin_client(self) -> dict:
        """
        Get Kafka Config Admin Client
        @retun admin client config
        """
        if self.config is None or self.config.get_runtime_config() is None:
            return None
        bootstrap_server = self.config.get_runtime_config().get(Constants.property_conf_kafka_server, None)
        security_protocol = self.config.get_runtime_config().get(Constants.property_conf_kafka_security_protocol, None)
        group_id = self.config.get_runtime_config().get(Constants.property_conf_kafka_group_id, None)
        ssl_ca_location = self.config.get_runtime_config().get(Constants.property_conf_kafka_s_sl_ca_location, None)
        ssl_certificate_location = self.config.get_runtime_config().get(
            Constants.property_conf_kafka_ssl_certificate_location, None)
        ssl_key_location = self.config.get_runtime_config().get(Constants.property_conf_kafka_ssl_key_location, None)
        ssl_key_password = self.config.get_runtime_config().get(Constants.property_conf_kafka_ssl_key_password, None)
        sasl_username = self.config.get_runtime_config().get(Constants.property_conf_kafka_sasl_producer_username, None)
        sasl_password = self.config.get_runtime_config().get(Constants.property_conf_kafka_sasl_producer_password, None)
        sasl_mechanism = self.config.get_runtime_config().get(Constants.property_conf_kafka_sasl_mechanism, None)

        conf = {Constants.bootstrap_servers: bootstrap_server,
                Constants.security_protocol: security_protocol,
                Constants.group_id: group_id,
                Constants.ssl_ca_location: ssl_ca_location,
                Constants.ssl_certificate_location: ssl_certificate_location,
                Constants.ssl_key_location: ssl_key_location,
                Constants.ssl_key_password: ssl_key_password}

        if sasl_username is not None and sasl_username != '' and sasl_password is not None and sasl_password != '':
            conf[Constants.sasl_username] = sasl_username
            conf[Constants.sasl_password] = sasl_password
            conf[Constants.sasl_mechanism] = sasl_mechanism

        return conf

    def get_kafka_config_producer(self) -> dict:
        """
        Get Producer Config
        @return producer config
        """
        if self.config is None or self.config.get_runtime_config() is None:
            return None
        bootstrap_server = self.config.get_runtime_config().get(Constants.property_conf_kafka_server, None)
        schema_registry = self.config.get_runtime_config().get(Constants.property_conf_kafka_schema_registry, None)
        security_protocol = self.config.get_runtime_config().get(Constants.property_conf_kafka_security_protocol, None)
        group_id = self.config.get_runtime_config().get(Constants.property_conf_kafka_group_id, None)
        ssl_ca_location = self.config.get_runtime_config().get(Constants.property_conf_kafka_s_sl_ca_location, None)
        ssl_certificate_location = self.config.get_runtime_config().get(
            Constants.property_conf_kafka_ssl_certificate_location, None)
        ssl_key_location = self.config.get_runtime_config().get(Constants.property_conf_kafka_ssl_key_location, None)
        ssl_key_password = self.config.get_runtime_config().get(Constants.property_conf_kafka_ssl_key_password, None)

        sasl_username = self.config.get_runtime_config().get(Constants.property_conf_kafka_sasl_producer_username, None)
        sasl_password = self.config.get_runtime_config().get(Constants.property_conf_kafka_sasl_producer_password, None)
        sasl_mechanism = self.config.get_runtime_config().get(Constants.property_conf_kafka_sasl_mechanism, None)

        conf = {Constants.bootstrap_servers: bootstrap_server,
                Constants.security_protocol: security_protocol,
                Constants.group_id: group_id,
                Constants.ssl_ca_location: ssl_ca_location,
                Constants.ssl_certificate_location: ssl_certificate_location,
                Constants.ssl_key_location: ssl_key_location,
                Constants.ssl_key_password: ssl_key_password,
                Constants.schema_registry_url: schema_registry}

        if sasl_username is not None and sasl_username != '' and sasl_password is not None and sasl_password != '':
            conf[Constants.sasl_username] = sasl_username
            conf[Constants.sasl_password] = sasl_password
            conf[Constants.sasl_mechanism] = sasl_mechanism

        return conf

    def get_kafka_config_consumer(self) -> dict:
        """
        Get Consumer config
        @return consumer config
        """
        if self.config is None or self.config.get_runtime_config() is None:
            return None
        conf = self.get_kafka_config_producer()

        conf['auto.offset.reset'] = 'earliest'

        sasl_username = self.config.get_runtime_config().get(Constants.property_conf_kafka_sasl_consumer_username, None)
        sasl_password = self.config.get_runtime_config().get(Constants.property_conf_kafka_sasl_consumer_password, None)

        if sasl_username is not None and sasl_username != '' and sasl_password is not None and sasl_password != '':
            conf[Constants.sasl_username] = sasl_username
            conf[Constants.sasl_password] = sasl_password
        return conf

    def get_kafka_schemas(self):
        """
        Get Avro schema
        @return key and value schema
        """
        key_schema_file = self.config.get_runtime_config().get(Constants.property_conf_kafka_key_schema, None)
        value_schema_file = self.config.get_runtime_config().get(Constants.property_conf_kafka_value_schema, None)

        from confluent_kafka import avro
        file = open(key_schema_file, "r")
        kbytes = file.read()
        file.close()
        key_schema = avro.loads(kbytes)
        file = open(value_schema_file, "r")
        vbytes = file.read()
        file.close()
        val_schema = avro.loads(vbytes)

        return key_schema, val_schema

    def get_kafka_producer(self):
        """
        Create and return a kafka producer
        @return producer
        """
        conf = self.get_kafka_config_producer()
        key_schema, val_schema = self.get_kafka_schemas()

        from fabric_mb.message_bus.producer import AvroProducerApi
        producer = AvroProducerApi(conf=conf, key_schema=key_schema, record_schema=val_schema, logger=self.get_logger())
        return producer

    def get_kafka_admin_client(self):
        """
        Create and return a kafka admin client
        @return admin client
        """
        from fabric_mb.message_bus.admin import AdminApi
        conf = self.get_kafka_config_admin_client()
        admin = AdminApi(conf=conf)
        return admin

    def get_logger(self):
        """
        Get logger
        @return logger
        """
        if not self.initialized:
            raise InitializationException(Constants.uninitialized_state)

        if self.log is None:
            self.log = self.make_logger()
        return self.log

    def start(self, *, force_fresh: bool):
        """
        Start CF Actor
        @param force_fresh true if clean restart, false if stateful restart
        """
        try:
            try:
                self.lock.acquire()
                if self.started:
                    return
                self.started = True
                self.start_completed = False
                if force_fresh:
                    self.delete_super_block()
            finally:
                self.lock.release()

            self.initialize()
            self.start_timer_thread()
            try:
                self.lock.acquire()
                self.container = Container()
                self.log.info("Successfully instantiated the container implementation.")
                self.log.info("Initializing container")
                self.container.initialize(config=self.config)
                self.log.info("Successfully initialized the container")
                self.start_completed = True
            finally:
                self.lock.release()
        except Exception as e:
            self.fail(e=e)

    def stop(self):
        """
        Stop the Actor
        """
        try:
            self.lock.acquire()
            if not self.started:
                return
            self.log.info("Stopping Actor")
            self.started = False
            self.stop_timer_thread()
            self.get_container().shutdown()
        except Exception as e:
            self.log.error("Error while shutting down: {}".format(e))
        finally:
            self.lock.release()

    def start_timer_thread(self):
        """
        Start the timer thread
        """
        if self.timer_thread is not None:
            raise RuntimeError("This actor has already been started")

        self.timer_thread = threading.Thread(target=self.timer_loop)
        self.timer_thread.setName('GlobalTimer')
        self.timer_thread.setDaemon(True)
        self.timer_thread.start()

    def stop_timer_thread(self):
        """
        Stop timer thread
        """
        temp = self.timer_thread
        self.timer_thread = None
        if temp is not None:
            self.log.warning("It seems that the timer thread is running. Interrupting it")
            try:
                with self.timer_condition:
                    self.timer_condition.notify_all()
                temp.join()
            except Exception as e:
                self.log.error("Could not join timer thread {}".format(e))

    def timer_loop(self):
        """
        Timer thread run function
        """
        self.log.debug("Timer thread started")
        while True:
            with self.timer_condition:
                while self.timer_scheduler.empty() and self.started:
                    try:
                        #self.log.debug("Waiting for condition")
                        self.timer_condition.wait()
                    except InterruptedError as e:
                        self.log.error(traceback.format_exc())
                        self.log.error("Timer thread interrupted. Exiting {}".format(e))
                        return

                    if not self.started:
                        self.log.info("Timer thread exiting")
                        return

                    self.timer_condition.notify_all()

                if not self.timer_scheduler.empty():
                    #self.log.debug("Executing Scheduled items")
                    self.timer_scheduler.run(blocking=False)


class GlobalsSingleton:
    """
    Global Singleton class
    """
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise InitializationException("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = Globals()
        return self.__instance

    get = classmethod(get)
