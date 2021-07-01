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
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import TYPE_CHECKING

import logging
import os

from fss_utils.jwt_validate import JWTValidator

from fabric_cf.actor.core.common.exceptions import InitializationException
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.container.container import Container

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_container import ABCActorContainer
    from fabric_cf.actor.boot.configuration import Configuration

logging.TRACE = 5
logging.addLevelName(logging.TRACE, "TRACE")
logging.Logger.trace = lambda inst, msg, *args, **kwargs: inst.log(logging.TRACE, msg, *args, **kwargs)
logging.trace = lambda msg, *args, **kwargs: logging.log(logging.TRACE, msg, *args, **kwargs)


class Globals:
    config_file = Constants.CONFIGURATION_FILE

    def __init__(self):
        self.config = None
        self.log = None
        self.initialized = False
        self.started = False
        self.start_completed = False
        self.container = None
        self.properties = None
        self.timer_scheduler = sched.scheduler()
        self.timer_thread = None
        self.timer_condition = threading.Condition()
        self.lock = threading.Lock()
        self.jwt_validator = None

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
        if Constants.PROPERTY_CONF_LOG_DIRECTORY in log_config and Constants.PROPERTY_CONF_LOG_FILE in log_config:
            log_path = log_config[Constants.PROPERTY_CONF_LOG_DIRECTORY] + '/' + \
                       log_config[Constants.PROPERTY_CONF_LOG_FILE]

        if log_path is None:
            raise RuntimeError('The log file path must be specified in config or passed as an argument')

        # Get the log level
        log_level = None
        if Constants.PROPERTY_CONF_LOG_LEVEL in log_config:
            log_level = log_config.get(Constants.PROPERTY_CONF_LOG_LEVEL, None)

        if log_level is None:
            log_level = logging.INFO

        # Set up the root logger
        log = logging.getLogger(log_config.get(Constants.PROPERTY_CONF_LOGGER, None))
        log.setLevel(log_level)
        log_format = \
            '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'

        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        backup_count = log_config.get(Constants.PROPERTY_CONF_LOG_RETAIN, None)
        max_log_size = log_config.get(Constants.PROPERTY_CONF_LOG_SIZE, None)

        file_handler = RotatingFileHandler(log_path, backupCount=int(backup_count), maxBytes=int(max_log_size))

        logging.basicConfig(handlers=[file_handler], format=log_format)

        return log

    @staticmethod
    def delete_super_block():
        """
        Delete Super block file
        """
        if os.path.isfile(Constants.SUPERBLOCK_LOCATION):
            os.remove(Constants.SUPERBLOCK_LOCATION)

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
                self.load_jwt_validator()
                self.log.info("Main initialization complete.")
                self.initialized = True
        finally:
            self.lock.release()

    def load_jwt_validator(self):
        oauth_config = self.config.get_oauth_config()
        CREDMGR_CERTS = oauth_config.get(Constants.PROPERTY_CONF_O_AUTH_JWKS_URL, None)
        CREDMGR_KEY_REFRESH = oauth_config.get(Constants.PROPERTY_CONF_O_AUTH_KEY_REFRESH, None)
        self.log.info(f'Initializing JWT Validator to use {CREDMGR_CERTS} endpoint, '
                      f'refreshing keys every {CREDMGR_KEY_REFRESH} HH:MM:SS')
        t = datetime.strptime(CREDMGR_KEY_REFRESH, "%H:%M:%S")
        self.jwt_validator = JWTValidator(url=CREDMGR_CERTS,
                                          refresh_period=timedelta(hours=t.hour, minutes=t.minute, seconds=t.second))

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

    def get_jwt_validator(self):
        return self.jwt_validator

    def get_container(self) -> ABCActorContainer:
        """
        Get the container
        @return container
        """
        if not self.initialized:
            raise InitializationException(Constants.UNINITIALIZED_STATE)
        return self.container

    def get_config(self) -> Configuration:
        """
        Get the configuration
        @return config
        """
        if not self.initialized:
            raise InitializationException(Constants.UNINITIALIZED_STATE)
        return self.config

    def get_kafka_config_admin_client(self) -> dict:
        """
        Get Kafka Config Admin Client
        @retun admin client config
        """
        if self.config is None or self.config.get_runtime_config() is None:
            return None
        bootstrap_server = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SERVER, None)
        security_protocol = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SECURITY_PROTOCOL, None)
        ssl_ca_location = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_S_SL_CA_LOCATION, None)
        ssl_certificate_location = self.config.get_runtime_config().get(
            Constants.PROPERTY_CONF_KAFKA_SSL_CERTIFICATE_LOCATION, None)
        ssl_key_location = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SSL_KEY_LOCATION, None)
        ssl_key_password = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SSL_KEY_PASSWORD, None)
        sasl_username = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SASL_PRODUCER_USERNAME, None)
        sasl_password = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SASL_PRODUCER_PASSWORD, None)
        sasl_mechanism = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SASL_MECHANISM, None)

        conf = {Constants.BOOTSTRAP_SERVERS: bootstrap_server,
                Constants.SECURITY_PROTOCOL: security_protocol,
                Constants.SSL_CA_LOCATION: ssl_ca_location,
                Constants.SSL_CERTIFICATE_LOCATION: ssl_certificate_location,
                Constants.SSL_KEY_LOCATION: ssl_key_location,
                Constants.SSL_KEY_PASSWORD: ssl_key_password}

        if sasl_username is not None and sasl_username != '' and sasl_password is not None and sasl_password != '':
            conf[Constants.SASL_USERNAME] = sasl_username
            conf[Constants.SASL_PASSWORD] = sasl_password
            conf[Constants.SASL_MECHANISM] = sasl_mechanism

        return conf

    def get_kafka_config_producer(self) -> dict:
        """
        Get Producer Config
        @return producer config
        """
        if self.config is None or self.config.get_runtime_config() is None:
            return None
        bootstrap_server = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SERVER, None)
        schema_registry = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SCHEMA_REGISTRY, None)
        security_protocol = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SECURITY_PROTOCOL, None)
        ssl_ca_location = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_S_SL_CA_LOCATION, None)
        ssl_certificate_location = self.config.get_runtime_config().get(
            Constants.PROPERTY_CONF_KAFKA_SSL_CERTIFICATE_LOCATION, None)
        ssl_key_location = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SSL_KEY_LOCATION, None)
        ssl_key_password = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SSL_KEY_PASSWORD, None)

        sasl_username = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SASL_PRODUCER_USERNAME, None)
        sasl_password = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SASL_PRODUCER_PASSWORD, None)
        sasl_mechanism = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SASL_MECHANISM, None)

        conf = {Constants.BOOTSTRAP_SERVERS: bootstrap_server,
                Constants.SECURITY_PROTOCOL: security_protocol,
                Constants.SSL_CA_LOCATION: ssl_ca_location,
                Constants.SSL_CERTIFICATE_LOCATION: ssl_certificate_location,
                Constants.SSL_KEY_LOCATION: ssl_key_location,
                Constants.SSL_KEY_PASSWORD: ssl_key_password,
                Constants.SCHEMA_REGISTRY_URL: schema_registry}

        if sasl_username is not None and sasl_username != '' and sasl_password is not None and sasl_password != '':
            conf[Constants.SASL_USERNAME] = sasl_username
            conf[Constants.SASL_PASSWORD] = sasl_password
            conf[Constants.SASL_MECHANISM] = sasl_mechanism

        return conf

    def get_kafka_config_consumer(self) -> dict:
        """
        Get Consumer config
        @return consumer config
        """
        if self.config is None or self.config.get_runtime_config() is None:
            return None
        conf = self.get_kafka_config_producer()

        group_id = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_GROUP_ID, None)

        conf['auto.offset.reset'] = 'earliest'

        sasl_username = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SASL_CONSUMER_USERNAME, None)
        sasl_password = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_SASL_CONSUMER_PASSWORD, None)

        if sasl_username is not None and sasl_username != '' and sasl_password is not None and sasl_password != '':
            conf[Constants.SASL_USERNAME] = sasl_username
            conf[Constants.SASL_PASSWORD] = sasl_password
        conf[Constants.GROUP_ID] = group_id
        return conf

    def get_kafka_schemas(self):
        """
        Get Avro schema
        @return key and value schema
        """
        key_schema_file = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_KEY_SCHEMA, None)
        value_schema_file = self.config.get_runtime_config().get(Constants.PROPERTY_CONF_KAFKA_VALUE_SCHEMA, None)

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

    def get_simple_kafka_producer(self):
        """
        Create and return a kafka producer
        @return producer
        """
        conf = self.get_kafka_config_producer()
        if conf is not None:
            conf.pop(Constants.SCHEMA_REGISTRY_URL)

        bqm_config = self.config.get_global_config().get_bqm_config()
        if bqm_config is not None:
            prod_user_name = bqm_config.get(Constants.PROPERTY_CONF_KAFKA_SASL_PRODUCER_USERNAME, None)
            prod_password = bqm_config.get(Constants.PROPERTY_CONF_KAFKA_SASL_PRODUCER_PASSWORD, None)
            if prod_user_name is not None and prod_password is not None:
                conf[Constants.SASL_USERNAME] = prod_user_name
                conf[Constants.SASL_PASSWORD] = prod_password

        from confluent_kafka import Producer
        producer = Producer(conf)
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
            raise InitializationException(Constants.UNINITIALIZED_STATE)

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
            raise RuntimeError("This timer thread has already been started")

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
