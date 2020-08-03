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

import yaml

from fabric.managecli.Configuration import Configuration
from fabric.message_bus.messages.auth_avro import AuthAvro


class ConfigProcessor:
    def __init__(self, path: str = None):
        self.path = path
        self.config = None

    def process(self, config: Configuration = None):
        if config is None:
            self.read_configuration()
        else:
            self.config = config
        if self.config.get_auth() is not None:
            self.auth = AuthAvro()
            self.auth.name = self.config.get_auth().get_name()
            self.auth.guid = self.config.get_auth().get_guid()

    def read_configuration(self) -> Configuration:
        if self.path is None:
            raise Exception("No data source has been specified")
        config_dict = None
        with open(self.path) as f:
            config_dict = yaml.safe_load(f)
        self.config = Configuration(config_dict)
        return self.config

    def get_auth(self) -> AuthAvro:
        return self.auth

    def get_kafka_server(self) -> str:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_kafka_server()
        else:
            return None

    def get_kafka_schema_registry(self) -> str:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_kafka_schema_registry()
        else:
            return None

    def get_kafka_key_schema(self) -> str:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_kafka_key_schema()
        else:
            return None

    def get_kafka_value_schema(self) -> str:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_kafka_value_schema()
        else:
            return None

    def get_kafka_topic(self) -> str:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_kafka_topic()
        else:
            return None

    def get_security_protocol(self) -> str:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_security_protocol()
        return None

    def get_group_id(self) -> str:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_group_id()
        return None

    def get_ca_location(self) -> str:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_ca_location()
        return None

    def get_cert_location(self) -> str:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_cert_location()
        return None

    def get_key_location(self) -> str:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_key_location()
        return None

    def get_key_password(self) -> str:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_key_password()
        return None

    def get_peers(self) -> list:
        return self.config.get_peers()

    def get_log_dir(self) -> str:
        if self.config is not None and self.config.get_logging() is not None:
            return self.config.get_logging().get_log_dir()
        return None

    def get_log_file(self) -> str:
        if self.config is not None and self.config.get_logging() is not None:
            return self.config.get_logging().get_log_file()
        return None

    def get_log_level(self):
        if self.config is not None and self.config.get_logging() is not None:
            return self.config.get_logging().get_log_level()
        return None

    def get_log_retain(self) -> int:
        if self.config is not None and self.config.get_logging() is not None:
            return int(self.config.get_logging().get_log_retain())
        return None

    def get_log_size(self) -> int:
        if self.config is not None and self.config.get_logging() is not None:
            return int(self.config.get_logging().get_log_size())
        return None

    def get_log_name(self) -> str:
        if self.config is not None and self.config.get_logging() is not None:
            return self.config.get_logging().get_log_name()
        return None

    def get_kafka_config(self) -> dict:
        if self.config is not None and self.config.get_runtime_config() is not None:
            return self.config.get_runtime_config().get_kafka_config()
        return None

    def get_kafka_config_producer(self) -> dict:
        if self.config is None or self.config.get_runtime_config() is None:
            return None

        bootstrap_server = self.get_kafka_server()
        schema_registry = self.get_kafka_schema_registry()
        security_protocol = self.get_security_protocol()
        group_id = self.get_group_id()
        ssl_ca_location = self.get_ca_location()
        ssl_certificate_location = self.get_cert_location()
        ssl_key_location = self.get_key_location()
        ssl_key_password = self.get_key_password()

        conf = {'bootstrap.servers': bootstrap_server,
                'security.protocol': security_protocol,
                'group.id': group_id,
                'ssl.ca.location': ssl_ca_location,
                'ssl.certificate.location': ssl_certificate_location,
                'ssl.key.location': ssl_key_location,
                'ssl.key.password': ssl_key_password,
                'schema.registry.url': schema_registry}

        return conf

    def get_kafka_config_consumer(self) -> dict:
        if self.config is None or self.config.get_runtime_config() is None:
            return None

        conf = self.get_kafka_config_producer()
        conf['auto.offset.reset'] = 'earliest'
        return conf

    def get_kafka_schemas(self):
        key_schema_file = self.get_kafka_key_schema()
        value_schema_file = self.get_kafka_value_schema()

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