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
from fabric.actor.core.common.constants import Constants


class RuntimeConfig:

    def __init__(self, *, config: list):
        self.kafka_config = {}

        for prop in config:
            for key, value in prop.items():
                self.kafka_config[key] = value

    def get_kafka_config(self):
        return self.kafka_config

    def get_kafka_server(self) -> str:
        return self.kafka_config.get(Constants.PropertyConfKafkaServer, None)

    def get_kafka_schema_registry(self) -> str:
        return self.kafka_config.get(Constants.PropertyConfKafkaSchemaRegistry, None)

    def get_kafka_key_schema(self) -> str:
        return self.kafka_config.get(Constants.PropertyConfKafkaKeySchema, None)

    def get_kafka_value_schema(self) -> str:
        return self.kafka_config.get(Constants.PropertyConfKafkaValueSchema, None)

    def get_kafka_topic(self) -> str:
        return self.kafka_config.get(Constants.KafkaTopic, None)

    def get_security_protocol(self) -> str:
        return self.kafka_config.get(Constants.PropertyConfKafkaSecurityProtocol, None)

    def get_group_id(self) -> str:
        return self.kafka_config.get(Constants.PropertyConfKafkaGroupId, None)

    def get_ca_location(self) -> str:
        return self.kafka_config.get(Constants.PropertyConfKafkaSSlCaLocation, None)

    def get_cert_location(self) -> str:
        return self.kafka_config.get(Constants.PropertyConfKafkaSslCertificateLocation, None)

    def get_key_location(self) -> str:
        return self.kafka_config.get(Constants.PropertyConfKafkaSslKeyLocation, None)

    def get_key_password(self) -> str:
        return self.kafka_config.get(Constants.PropertyConfKafkaSslKeyPassword, None)


class LogConfig:
    def __init__(self, *, config: list):
        self.log_dir = None
        self.log_file = None
        self.log_level = None
        self.log_retain = None
        self.log_size = None
        self.log_name = None

        for prop in config:
            for key, value in prop.items():
                if key.lower() == Constants.PropertyConfLogDirectory:
                    self.log_dir = value
                if key.lower() == Constants.PropertyConfLogFile:
                    self.log_file = value
                if key.lower() == Constants.PropertyConfLogLevel:
                    self.log_level = value
                if key.lower() == Constants.PropertyConfLogRetain:
                    self.log_retain = value
                if key.lower() == Constants.PropertyConfLogSize:
                    self.log_size = value
                if key.lower() == Constants.PropertyConfLogger:
                    self.log_name = value

    def get_log_dir(self) -> str:
        return self.log_dir

    def get_log_file(self) -> str:
        return self.log_file

    def get_log_level(self):
        return self.log_level

    def get_log_retain(self) -> int:
        return self.log_retain

    def get_log_size(self) -> int:
        return self.log_size

    def get_log_name(self) -> int:
        return self.log_name


class AuthConfig:
    def __init__(self, *, config: list):
        self.name = None
        self.guid = None

        for prop in config:
            for key, value in prop.items():
                if key.lower() == Constants.Name:
                    self.name = value
                if key.lower() == Constants.Guid:
                    self.guid = value

    def get_name(self) -> str:
        return self.name

    def get_guid(self) -> str:
        return self.guid


class Peer:
    def __init__(self, *, config: list):
        self.name = None
        self.type = None
        self.guid = None
        self.kafka_topic = None
        for prop in config:
            for key, value in prop.items():
                if key == Constants.Name:
                    self.name = value
                elif key == Constants.Type:
                    self.type = value
                elif key == Constants.Guid:
                    self.guid = value
                elif key == Constants.KafkaTopic:
                    self.kafka_topic = value

    def get_name(self) -> str:
        return self.name

    def get_type(self) -> str:
        return self.type

    def get_guid(self) -> str:
        return self.guid

    def get_kafka_topic(self) -> str:
        return self.kafka_topic


class Configuration:
    def __init__(self, config: dict):
        self.runtime = RuntimeConfig(config=config[Constants.ConfigSectionRuntime])
        self.logging = LogConfig(config=config[Constants.ConfigLoggingSection])
        self.auth = AuthConfig(config=config['auth'])
        self.peers = []
        if 'peers' in config:
            for e in config['peers']:
                self.peers.append(Peer(config=e['peer']))

    def get_runtime_config(self) -> RuntimeConfig:
        return self.runtime

    def get_peers(self) -> list:
        return self.peers

    def get_auth(self) -> AuthConfig:
        return self.auth

    def get_logging(self) -> LogConfig:
        return self.logging
