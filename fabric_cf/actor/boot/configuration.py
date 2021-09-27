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
from typing import List, Dict

from fabric_cf.actor.core.common.constants import Constants


class GlobalConfig:
    """
    Represent Global configuration
    """
    def __init__(self, *, config: dict):
        self.runtime = {}
        if Constants.CONFIG_SECTION_RUNTIME in config:
            self.runtime = config.get(Constants.CONFIG_SECTION_RUNTIME)

        self.logging = {}
        if Constants.CONFIG_LOGGING_SECTION in config:
            self.logging = config.get(Constants.CONFIG_LOGGING_SECTION)

        self.oauth = {}
        if Constants.CONFIG_SECTION_O_AUTH in config:
            self.oauth = config.get(Constants.CONFIG_SECTION_O_AUTH)

        self.database = {}
        if Constants.CONFIG_SECTION_DATABASE in config:
            self.database = config.get(Constants.CONFIG_SECTION_DATABASE)

        self.container = {}
        if Constants.CONFIG_SECTION_CONTAINER in config:
            self.container = config.get(Constants.CONFIG_SECTION_CONTAINER)

        self.time = {}
        if Constants.CONFIG_SECTION_TIME in config:
            self.time = config.get(Constants.CONFIG_SECTION_TIME)

        self.neo4j = {}
        if Constants.CONFIG_SECTION_NEO4J in config:
            self.neo4j = config[Constants.CONFIG_SECTION_NEO4J]

        self.pdp = {}
        if Constants.CONFIG_SECTION_PDP in config:
            self.pdp = config[Constants.CONFIG_SECTION_PDP]

        self.bqm = {}
        if Constants.CONFIG_SECTION_BQM in config:
            self.bqm = config[Constants.CONFIG_SECTION_BQM]

    def get_runtime(self) -> dict:
        """
        Return runtime config
        """
        return self.runtime

    def get_logging(self) -> dict:
        """
        Return logging config
        """
        return self.logging

    def get_oauth(self) -> dict:
        """
        Return oauth config
        """
        return self.oauth

    def get_database(self) -> dict:
        """
        Return database config
        """
        return self.database

    def get_container(self) -> dict:
        """
        Return container config
        """
        return self.container

    def get_time(self) -> dict:
        """
        Return time config
        """
        return self.time

    def get_neo4j_config(self) -> dict:
        """
        Return neo4j config
        """
        return self.neo4j

    def get_pdp_config(self) -> dict:
        """
        Return PDP config
        """
        return self.pdp

    def get_bqm_config(self) -> dict:
        """
        Return BQM config
        """
        return self.bqm


class HandlerConfig:
    """
    Represents Handler Config
    """
    def __init__(self, *, config: dict):
        self.module_name = config.get(Constants.PROPERTY_CONF_MODULE_NAME, None)
        self.class_name = config.get(Constants.PROPERTY_CONF_CLASS_NAME, None)
        self.properties = {}
        if Constants.PROPERTY_CONF_PROPERTIES_NAME in config:
            self.properties = config.get(Constants.PROPERTY_CONF_PROPERTIES_NAME)

    def get_module_name(self) -> str:
        """
        Return Module Name for Handler
        """
        return self.module_name

    def get_class_name(self) -> str:
        """
        Return Class Name for Handler
        """
        return self.class_name

    def get_properties(self) -> dict:
        """
        Return Handler Properties
        """
        return self.properties


class ControlConfig:
    """
    Represents Control or Inventory config
    """
    def __init__(self, *, config: dict):
        self.type = []
        self.module_name = config.get(Constants.PROPERTY_CONF_MODULE_NAME, None)
        self.class_name = config.get(Constants.PROPERTY_CONF_CLASS_NAME, None)
        if Constants.TYPE in config:
            value = config.get(Constants.TYPE)
            values = value.split(',')
            for v in values:
                self.type.append(v.strip())

    def get_type(self) -> List[str]:
        """
        Return type
        """
        return self.type

    def get_module_name(self) -> str:
        """
        Return Module name
        """
        return self.module_name

    def get_class_name(self) -> str:
        """
        Return Class Name
        """
        return self.class_name


class ResourceConfig:
    """
    Represents resource config
    """
    def __init__(self, *, config: dict):
        self.description = config.get(Constants.DESCRIPTION, None)
        self.type = []
        if Constants.TYPE in config:
            value = config.get(Constants.TYPE)
            values = value.split(',')
            for v in values:
                self.type.append(v.strip())
        self.label = config.get(Constants.LABEL, None)
        self.handler = HandlerConfig(config=config.get(Constants.HANDLER, None))

    def get_type(self) -> List[str]:
        """
        Return type
        """
        return self.type

    def get_label(self) -> str:
        """
        Return label
        """
        return self.label

    def get_description(self) -> str:
        """
        Return description
        """
        return self.description

    def get_handler(self) -> HandlerConfig:
        """
        Return handler config
        """
        return self.handler


class PolicyConfig:
    """
    Represents Policy config
    """
    def __init__(self, *, config: dict):
        self.module_name = config.get(Constants.PROPERTY_CONF_MODULE_NAME, None)
        self.class_name = config.get(Constants.PROPERTY_CONF_CLASS_NAME, None)
        self.properties = {}
        if Constants.PROPERTY_CONF_PROPERTIES_NAME in config:
            self.properties = config.get(Constants.PROPERTY_CONF_PROPERTIES_NAME)

    def get_module_name(self) -> str:
        """
        Return module name
        """
        return self.module_name

    def get_class_name(self) -> str:
        """
        Return class name
        """
        return self.class_name

    def get_properties(self) -> dict:
        """
        Return properties
        """
        return self.properties


class ActorConfig:
    """
    Represents Actor Config
    """
    def __init__(self, *, config: dict):
        self.policy = PolicyConfig(config=config.get('policy', None))
        self.description = config.get(Constants.DESCRIPTION, None)
        self.resources = []
        self.controls = []
        self.type = config.get(Constants.TYPE, None)
        self.name = config.get(Constants.NAME, None)
        self.guid = config.get(Constants.GUID, None)
        self.kafka_topic = config.get(Constants.KAFKA_TOPIC, None)
        self.substrate_file = config.get(Constants.PROPERTY_SUBSTRATE_FILE)
        res = config.get('resources', None)
        if res is not None:
            for r in res:
                resource_config = ResourceConfig(config=r['resource'])
                self.resources.append(resource_config)

        ctrls = config.get('controls', None)
        if ctrls is not None:
            for c in ctrls:
                control_config = ControlConfig(config=c['control'])
                self.controls.append(control_config)

    def get_type(self) -> str:
        """
        Return Actor Type
        """
        return self.type

    def get_name(self) -> str:
        """
        Return Actor Name
        """
        return self.name

    def get_guid(self) -> str:
        """
        Return Actor Guid
        """
        return self.guid

    def get_description(self) -> str:
        """
        Return Actor Description
        """
        return self.description

    def get_kafka_topic(self) -> str:
        """
        Return Kafka topic
        """
        return self.kafka_topic

    def get_controls(self) -> List[ControlConfig]:
        """
        Return Controls
        """
        return self.controls

    def get_policy(self) -> PolicyConfig:
        """
        Return Policy
        """
        return self.policy

    def get_resources(self) -> List[ResourceConfig]:
        """
        Return resources
        """
        return self.resources

    def get_substrate_file(self) -> str:
        """
        Return Substrate file
        """
        return self.substrate_file


class Peer:
    """
    Represent Peer Config
    """
    def __init__(self, *, config: dict):
        self.name = config.get(Constants.NAME, None)
        self.type = config.get(Constants.TYPE, None)
        self.guid = config.get(Constants.GUID, None)
        self.kafka_topic = config.get(Constants.KAFKA_TOPIC, None)
        self.delegation = config.get(Constants.DELEGATION, None)

    def get_name(self) -> str:
        """
        Return Name
        """
        return self.name

    def get_type(self) -> str:
        """
        Return Type
        """
        return self.type

    def get_guid(self) -> str:
        """
        Return Guid
        """
        return self.guid

    def get_kafka_topic(self) -> str:
        """
        Return Kafka Topic
        """
        return self.kafka_topic

    def get_delegation(self) -> str:
        """
        Return delegation
        """
        return self.delegation


class Configuration:
    """
    Represent Configuration read from file
    """
    def __init__(self, *, config: dict):
        self.global_config = GlobalConfig(config=config)
        self.actor = ActorConfig(config=config['actor'])
        self.peers = []
        self.topic_peer_map = {}
        if 'peers' in config:
            for e in config['peers']:
                p = Peer(config=e['peer'])
                self.peers.append(p)
                self.topic_peer_map[p.get_kafka_topic()] = p

    def get_global_config(self) -> GlobalConfig:
        """
        Return Global Config
        """
        return self.global_config

    def get_log_config(self) -> dict:
        """
        Return Log config
        """
        return self.global_config.get_logging()

    def get_runtime_config(self) -> dict:
        """
        Return Runtime Config
        """
        if self.global_config is not None:
            return self.global_config.get_runtime()
        return None

    def get_oauth_config(self) -> dict:
        """
        Return OAuth Config
        """
        if self.global_config is not None:
            return self.global_config.get_oauth()
        return None

    def get_actor(self) -> ActorConfig:
        """
        Return Actor Config
        """
        return self.actor

    def get_peers(self) -> List[Peer]:
        """
        Return Peer Config
        """
        return self.peers

    def get_topic_peer_map(self) -> Dict[str, Peer]:
        return self.topic_peer_map

    def get_neo4j_config(self) -> dict:
        """
        Return Neo4j config
        """
        if self.global_config is not None:
            return self.global_config.get_neo4j_config()

        return None

    def get_kafka_key_schema_location(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_KEY_SCHEMA, None)

    def get_kafka_value_schema_location(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_VALUE_SCHEMA, None)

    def get_kafka_server(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_SERVER, None)

    def get_kafka_schema_registry(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_SCHEMA_REGISTRY, None)

    def get_kafka_security_protocol(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_SECURITY_PROTOCOL, None)

    def get_kafka_ssl_ca_location(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_S_SL_CA_LOCATION, None)

    def get_kafka_ssl_cert_location(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_SSL_CERTIFICATE_LOCATION, None)

    def get_kafka_ssl_key_location(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_SSL_KEY_LOCATION, None)

    def get_kafka_ssl_key_password(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_SSL_KEY_PASSWORD, None)

    def get_kafka_prod_user_name(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_SASL_PRODUCER_USERNAME, None)

    def get_kafka_prod_user_pwd(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_SASL_PRODUCER_PASSWORD, None)

    def get_kafka_sasl_mechanism(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_SASL_MECHANISM, None)

    def get_kafka_cons_group_id(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_GROUP_ID, None)

    def get_kafka_cons_user_name(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_SASL_CONSUMER_USERNAME, None)

    def get_kafka_cons_user_pwd(self) -> str or None:
        return self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_SASL_CONSUMER_PASSWORD, None)

    def get_kafka_request_timeout_ms(self) -> int:
        value = self.global_config.runtime.get(Constants.PROPERTY_CONF_KAFKA_REQUEST_TIMEOUT_MS, 120000)
        return int(value)

    def get_rpc_request_timeout_seconds(self) -> int:
        value = self.global_config.runtime.get(Constants.PROPERTY_CONF_RPC_REQUEST_TIMEOUT_SECONDS, 900)
        return int(value)