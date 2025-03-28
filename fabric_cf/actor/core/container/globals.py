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
from typing import TYPE_CHECKING

import logging
import os

from fabric_cf.actor.core.util.quota_mgr import QuotaMgr
from fim.graph.neo4j_property_graph import Neo4jGraphImporter
from fim.graph.resources.abc_arm import ABCARMPropertyGraph
from fss_utils.jwt_validate import JWTValidator

from fabric_cf.actor.core.common.event_logger import EventLoggerSingleton
from fabric_cf.actor.core.common.exceptions import InitializationException
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.container.container import Container
from fabric_cf.actor.core.util.log_helper import LogHelper
from fabric_cf.actor.security.token_validator import TokenValidator

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_container import ABCActorContainer
    from fabric_cf.actor.boot.configuration import Configuration

logging.TRACE = 5
logging.addLevelName(logging.TRACE, "TRACE")
logging.Logger.trace = lambda inst, msg, *args, **kwargs: inst.log(logging.TRACE, msg, *args, **kwargs)
logging.trace = lambda msg, *args, **kwargs: logging.log(logging.TRACE, msg, *args, **kwargs)


class Globals:
    config_file = Constants.CONFIGURATION_FILE
    RPC_TIMEOUT = 0

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
        self.token_validator = None
        self.quota_mgr = None

    def make_logger(self):
        """
        Detects the path and level for the log file from the actor config and sets
        up a logger. Instead of detecting the path and/or level from the
        config, a custom path and/or level for the log file can be passed as
        optional arguments.

       :return: logging.Logger object
        """
        log_config = self.config.get_global_config().get_logging()
        if log_config is None:
            raise RuntimeError('No logging  config information available')

        EventLoggerSingleton.get().make_logger(log_config=log_config)

        log_dir = log_config.get(Constants.PROPERTY_CONF_LOG_DIRECTORY, ".")
        log_file = log_config.get(Constants.PROPERTY_CONF_LOG_FILE, "actor.log")
        log_level = log_config.get(Constants.PROPERTY_CONF_LOG_LEVEL, None)
        log_retain = int(log_config.get(Constants.PROPERTY_CONF_LOG_RETAIN, 50))
        log_size = int(log_config.get(Constants.PROPERTY_CONF_LOG_SIZE, 5000000))
        logger = log_config.get(Constants.PROPERTY_CONF_LOGGER, "actor")

        return LogHelper.make_logger(log_dir=log_dir, log_file=log_file, log_level=log_level, log_retain=log_retain,
                                     log_size=log_size, logger=logger)

    @staticmethod
    def delete_super_block():
        """
        Delete Super block file
        """
        if os.path.isfile(Constants.SUPERBLOCK_LOCATION):
            os.remove(Constants.SUPERBLOCK_LOCATION)

    @staticmethod
    def can_reload_model() -> bool:
        if os.path.isfile(Constants.MODEL_RELOAD_LOCATION):
            return True
        return False

    @staticmethod
    def delete_reload_model_state_file():
        """
        Delete reload model state file
        """
        if os.path.isfile(Constants.MODEL_RELOAD_LOCATION):
            os.remove(Constants.MODEL_RELOAD_LOCATION)

    def cleanup_neo4j(self):
        """
        Cleanup Neo4j on clean restart
        """
        self.log.debug("Cleanup Neo4j database started")
        config = self.get_config().get_neo4j_config()
        neo4j_graph_importer = Neo4jGraphImporter(url=config["url"], user=config["user"],
                                                  pswd=config["pass"],
                                                  import_host_dir=config["import_host_dir"],
                                                  import_dir=config["import_dir"])
        neo4j_graph_importer.delete_all_graphs()
        self.log.debug("Cleanup Neo4j database completed")

    def check_and_reload_model(self, *, graph_id) -> ABCARMPropertyGraph or None:
        """
            Reload Neo4j on model restart
        """
        if not self.can_reload_model():
            return None
        self.cleanup_neo4j()
        self.log.debug(f"Reload Neo4j database started {graph_id}")
        from fabric_cf.actor.fim.fim_helper import FimHelper
        arm_graph = FimHelper.get_arm_graph_from_file(filename=self.get_config().get_actor_config().get_substrate_file(),
                                                      graph_id=graph_id)
        self.log.debug(f"Reload Neo4j database completed {graph_id}")
        return arm_graph

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
                self.load_validators()
                self.log.info("Main initialization complete.")
                self.initialized = True
        finally:
            self.lock.release()

    def load_validators(self):
        oauth_config = self.config.get_oauth_config()
        CREDMGR_CERTS = oauth_config.get(Constants.PROPERTY_CONF_O_AUTH_JWKS_URL, None)
        CREDMGR_KEY_REFRESH = oauth_config.get(Constants.PROPERTY_CONF_O_AUTH_KEY_REFRESH, None)
        CREDMGR_TRL_REFRESH = oauth_config.get(Constants.PROPERTY_CONF_O_AUTH_TRL_REFRESH, '00:01:00')
        self.log.info(f'Initializing JWT Validator to use {CREDMGR_CERTS} endpoint, '
                      f'refreshing keys every {CREDMGR_KEY_REFRESH} HH:MM:SS refreshing '
                      f'token revoke list every {CREDMGR_TRL_REFRESH} HH:MM:SS')
        t = datetime.strptime(CREDMGR_KEY_REFRESH, "%H:%M:%S")
        self.jwt_validator = JWTValidator(url=CREDMGR_CERTS,
                                          refresh_period=timedelta(hours=t.hour, minutes=t.minute, seconds=t.second))
        from urllib.parse import urlparse
        t = datetime.strptime(CREDMGR_KEY_REFRESH, "%H:%M:%S")
        self.token_validator = TokenValidator(credmgr_host=str(urlparse(CREDMGR_CERTS).hostname),
                                              refresh_period=timedelta(hours=t.hour, minutes=t.minute, seconds=t.second),
                                              jwt_validator=self.jwt_validator)

        core_api = self.config.get_core_api_config()
        if core_api.get("enable", False):
            self.quota_mgr = QuotaMgr(core_api_host=core_api.get(Constants.PROPERTY_CONF_HOST),
                                      token=core_api.get(Constants.TOKEN, ""),
                                      logger=self.log)

    def load_config(self):
        """
        Load the configuration
        """
        try:
            from fabric_cf.actor.boot.configuration_loader import ConfigurationLoader
            loader = ConfigurationLoader(path=self.config_file)
            self.config = loader.read_configuration()
            self.RPC_TIMEOUT = self.config.get_rpc_request_timeout_seconds()
        except Exception as e:
            raise RuntimeError("Unable to parse configuration file {}".format(e))

    def get_jwt_validator(self) -> JWTValidator:
        return self.jwt_validator

    def get_token_validator(self) -> TokenValidator:
        return self.token_validator

    def get_quota_mgr(self) -> QuotaMgr:
        return self.quota_mgr

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

    def get_log_config(self) -> dict:
        """
        Get the Log configuration
        @return dict
        """
        return self.get_config().get_log_config()

    def get_kafka_config_admin_client(self) -> dict:
        """
        Get Kafka Config Admin Client
        @retun admin client config
        """
        if self.config is None or self.config.get_runtime_config() is None:
            return None

        sasl_username = self.config.get_kafka_prod_user_name()
        sasl_password = self.config.get_kafka_prod_user_pwd()
        sasl_mechanism = self.config.get_kafka_sasl_mechanism()

        conf = {Constants.BOOTSTRAP_SERVERS: self.config.get_kafka_server(),
                Constants.SECURITY_PROTOCOL: self.config.get_kafka_security_protocol(),
                Constants.SSL_CA_LOCATION: self.config.get_kafka_ssl_ca_location(),
                Constants.SSL_CERTIFICATE_LOCATION: self.config.get_kafka_ssl_cert_location(),
                Constants.SSL_KEY_LOCATION: self.config.get_kafka_ssl_key_location(),
                Constants.SSL_KEY_PASSWORD: self.config.get_kafka_ssl_key_password()}

        if sasl_username is not None and sasl_username != '' and sasl_password is not None and sasl_password != '':
            conf[Constants.SASL_USERNAME] = sasl_username
            conf[Constants.SASL_PASSWORD] = sasl_password
            conf[Constants.SASL_MECHANISM] = sasl_mechanism

        return conf

    def get_kafka_config_schema_registry_client(self) -> dict:
        """
        Get Producer Config
        @return producer config
        """
        if self.config and self.config.get_runtime_config():
            return {"url": self.config.get_kafka_schema_registry() }

    def get_kafka_config_producer(self) -> dict:
        """
        Get Producer Config
        @return producer config
        """
        if self.config is None or self.config.get_runtime_config() is None:
            return None

        sasl_username = self.config.get_kafka_prod_user_name()
        sasl_password = self.config.get_kafka_prod_user_pwd()
        sasl_mechanism = self.config.get_kafka_sasl_mechanism()

        conf = {Constants.BOOTSTRAP_SERVERS: self.config.get_kafka_server(),
                Constants.SECURITY_PROTOCOL: self.config.get_kafka_security_protocol(),
                Constants.SSL_CA_LOCATION: self.config.get_kafka_ssl_ca_location(),
                Constants.SSL_CERTIFICATE_LOCATION: self.config.get_kafka_ssl_cert_location(),
                Constants.SSL_KEY_LOCATION: self.config.get_kafka_ssl_key_location(),
                Constants.SSL_KEY_PASSWORD: self.config.get_kafka_ssl_key_password(),
                Constants.SCHEMA_REGISTRY_URL: self.config.get_kafka_schema_registry(),
                Constants.PROPERTY_CONF_KAFKA_REQUEST_TIMEOUT_MS: self.config.get_kafka_request_timeout_ms(),
                Constants.PROPERTY_CONF_KAFKA_MAX_MESSAGE_SIZE: self.config.get_kafka_max_message_size()}

        if sasl_username is not None and sasl_username != '' and sasl_password is not None and sasl_password != '':
            conf[Constants.SASL_USERNAME] = sasl_username
            conf[Constants.SASL_PASSWORD] = sasl_password
            conf[Constants.SASL_MECHANISM] = sasl_mechanism

        return conf

    def get_kafka_consumer_poll_timeout(self):
        if self.get_config():
            return self.get_config().get_kafka_consumer_poll_timeout()

    def get_kafka_consumer_commit_batch_size(self):
        if self.get_config():
            return self.get_config().get_kafka_consumer_commit_batch_size()

    def get_kafka_consumer_auto_commit_interval(self):
        if self.get_config():
            return self.get_config().get_kafka_consumer_auto_commit_interval()

    def get_kafka_consumer_enable_auto_commit(self):
        if self.get_config():
            return self.get_config().get_kafka_consumer_enable_auto_commit()

    def get_kafka_key_schema_location(self):
        if self.get_config():
            return self.get_config().get_kafka_key_schema_location()

    def get_kafka_value_schema_location(self):
        if self.get_config():
            return self.get_config().get_kafka_value_schema_location()

    def get_kafka_config_consumer(self) -> dict:
        """
        Get Consumer config
        @return consumer config
        """
        if self.config and self.config.get_runtime_config():
            conf = self.get_kafka_config_producer()

            group_id = self.get_config().get_kafka_cons_group_id()

            conf['auto.offset.reset'] = 'earliest'
            enable_auto_commit = self.get_kafka_consumer_enable_auto_commit()
            if enable_auto_commit:
                conf[Constants.PROPERTY_CONF_KAFKA_AUTO_COMMIT_INTERVAL] = self.get_kafka_consumer_auto_commit_interval()
            else:
                conf[Constants.PROPERTY_CONF_KAFKA_ENABLE_AUTO_COMMIT] = enable_auto_commit

            sasl_username = self.get_config().get_kafka_cons_user_name()
            sasl_password = self.get_config().get_kafka_cons_user_pwd()

            if sasl_username is not None and sasl_username != '' and sasl_password is not None and sasl_password != '':
                conf[Constants.SASL_USERNAME] = sasl_username
                conf[Constants.SASL_PASSWORD] = sasl_password
            conf[Constants.GROUP_ID] = group_id
            conf[Constants.PROPERTY_CONF_KAFKA_FETCH_MAX_MESSAGE_SIZE] = self.get_config().get_kafka_max_message_size()
            return conf

    def get_kafka_producer(self):
        """
        Create and return a kafka producer
        @return producer
        """
        conf = self.get_kafka_config_producer()
        key_schema_file = self.config.get_kafka_key_schema_location()
        value_schema_file = self.config.get_kafka_value_schema_location()

        from fabric_mb.message_bus.producer import AvroProducerApi
        producer = AvroProducerApi(producer_conf=conf,
                                   key_schema_location=key_schema_file,
                                   #schema_registry_conf=self.get_kafka_config_schema_registry_client(),
                                   value_schema_location=value_schema_file, logger=self.get_logger())
        return producer

    def get_kafka_producer_with_poller(self, *, actor):
        """
        Create and return a kafka producer
        @return producer
        """
        conf = self.get_kafka_config_producer()
        key_schema_file = self.config.get_kafka_key_schema_location()
        value_schema_file = self.config.get_kafka_value_schema_location()

        from fabric_cf.actor.core.container.rpc_producer import RPCProducer
        producer = RPCProducer(producer_conf=conf, key_schema_location=key_schema_file,
                               value_schema_location=value_schema_file, logger=self.get_logger(),
                               actor=actor, retries=self.config.get_rpc_retries())
        # Uncomment for 1.7
        #producer = RPCProducer(producer_conf=conf, schema_registry_conf=self.get_kafka_config_schema_registry_client(),
        #                       value_schema_location=value_schema_file, logger=self.get_logger(),
        #                       actor=actor, retries=self.config.get_rpc_retries())
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
            self.log.error(traceback.format_exc())
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
        self.log.debug(f"Timer thread started")
        while True:
            with self.timer_condition:
                while self.timer_scheduler.empty() and self.started:
                    try:
                        #self.log.debug("Waiting for condition")
                        self.timer_condition.wait()
                    except InterruptedError as e:
                        self.log.error(traceback.format_exc())
                        self.log.error(f" Timer thread interrupted. Exiting {e}")
                        return

                    if not self.started:
                        self.log.info(f"Timer thread exiting")
                        return

                    self.timer_condition.notify_all()

                if not self.timer_scheduler.empty():
                    #self.log.debug(f"Executing Scheduled items")
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
