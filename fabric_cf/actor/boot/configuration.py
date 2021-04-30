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
from datetime import datetime
from typing import List

from fabric_cf.actor.core.common.constants import Constants


class GlobalConfig:
    """
    Represent Global configuration
    """
    def __init__(self, *, config: dict):
        self.runtime = {}
        if Constants.CONFIG_SECTION_RUNTIME in config:
            for prop in config[Constants.CONFIG_SECTION_RUNTIME]:
                for key, value in prop.items():
                    self.runtime[key] = value
        self.logging = {}
        if Constants.CONFIG_LOGGING_SECTION in config:
            for prop in config[Constants.CONFIG_LOGGING_SECTION]:
                for key, value in prop.items():
                    self.logging[key] = value
        self.oauth = {}
        if Constants.CONFIG_SECTION_O_AUTH in config:
            for prop in config[Constants.CONFIG_SECTION_O_AUTH]:
                for key, value in prop.items():
                    self.oauth[key] = value
        self.database = {}
        if Constants.CONFIG_SECTION_DATABASE in config:
            for prop in config[Constants.CONFIG_SECTION_DATABASE]:
                for key, value in prop.items():
                    self.database[key] = value
        self.container = {}
        if Constants.CONFIG_SECTION_CONTAINER in config:
            for prop in config[Constants.CONFIG_SECTION_CONTAINER]:
                for key, value in prop.items():
                    self.container[key] = value
        self.time = {}
        if Constants.CONFIG_SECTION_TIME in config:
            for prop in config[Constants.CONFIG_SECTION_TIME]:
                for key, value in prop.items():
                    self.time[key] = value

        self.neo4j = {}
        if Constants.CONFIG_SECTION_NEO4J in config:
            self.neo4j = config[Constants.CONFIG_SECTION_NEO4J]

        self.pdp = {}
        if Constants.CONFIG_SECTION_PDP in config:
            self.pdp = config[Constants.CONFIG_SECTION_PDP]

        self.bqm = {}
        if Constants.BROKER_QUERY_MODEL in config:
            self.bqm = config[Constants.BROKER_QUERY_MODEL]

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
    def __init__(self, *, config: list):
        self.module_name = None
        self.class_name = None
        self.properties = {}
        for prop in config:
            for key, value in prop.items():
                if key == 'module':
                    self.module_name = value
                elif key == 'class':
                    self.class_name = value
                elif key == 'properties':
                    for p in value:
                        for k, v in p.items():
                            self.properties[k] = str(v)

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
    def __init__(self, *, config: list):
        self.type = []
        self.module_name = None
        self.class_name = None
        for prop in config:
            for key, value in prop.items():
                if key == 'type':
                    values = value.split(',')
                    for v in values:
                        self.type.append(v.strip())
                elif key == 'module':
                    self.module_name = value
                elif key == 'class':
                    self.class_name = value

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
    def __init__(self, *, config: list):
        self.description = None
        self.type = None
        self.label = None
        self.properties = {}
        self.handler = None
        for prop in config:
            for key, value in prop.items():
                if key == 'type':
                    self.type = value
                elif key == 'label':
                    self.label = value
                elif key == 'description':
                    self.description = value
                elif key == 'properties':
                    for p in value:
                        for k, v in p.items():
                            self.properties[k] = str(v)
                elif key == 'handler':
                    self.handler = HandlerConfig(config=value)

    def get_type(self) -> str:
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

    def get_properties(self) -> dict:
        """
        Return properties
        """
        return self.properties


class PolicyConfig:
    """
    Represents Policy config
    """
    def __init__(self, *, config: list):
        self.module_name = None
        self.class_name = None
        self.properties = {}
        for prop in config:
            for key, value in prop.items():
                if key == 'module':
                    self.module_name = value
                elif key == 'class':
                    self.class_name = value
                elif key == 'properties':
                    for p in value:
                        for k, v in p.items():
                            self.properties[k] = str(v)

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
    def __init__(self, *, config: list):
        self.policy = None
        self.description = None
        self.resources = []
        self.controls = []
        self.type = None
        self.name = None
        self.guid = None
        self.kafka_topic = None
        self.policy = None
        self.substrate_file = None
        for prop in config:
            for key, value in prop.items():
                if key == 'type':
                    self.type = value
                elif key == 'name':
                    self.name = value
                elif key == 'guid':
                    self.guid = value
                elif key == 'description':
                    self.description = value
                elif key == 'kafka-topic':
                    self.kafka_topic = value
                elif key == 'substrate.file':
                    self.substrate_file = value
                elif key == 'controls':
                    for c in value:
                        self.controls.append(ControlConfig(config=c['control']))
                elif key == 'policy':
                    self.policy = PolicyConfig(config=value)
                elif key == 'resources':
                    for p in value:
                        self.resources.append(ResourceConfig(config=p['resource']))

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
    def __init__(self, *, config: list):
        self.name = None
        self.type = None
        self.guid = None
        self.kafka_topic = None
        self.delegation = None
        for prop in config:
            for key, value in prop.items():
                if key == 'name':
                    self.name = value
                elif key == 'type':
                    self.type = value
                elif key == 'guid':
                    self.guid = value
                elif key == 'kafka-topic':
                    self.kafka_topic = value
                elif key == 'delegation':
                    self.delegation = value

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
        if 'peers' in config:
            for e in config['peers']:
                self.peers.append(Peer(config=e['peer']))

    def get_global_config(self) -> GlobalConfig:
        """
        Return Global Config
        """
        return self.global_config

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

    def get_neo4j_config(self) -> dict:
        """
        Return Neo4j config
        """
        if self.global_config is not None:
            return self.global_config.get_neo4j_config()

        return None
