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

from manage_cli.Configuration import Configuration
from message_bus.messages.AuthAvro import AuthAvro


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
        print("Reading config file: {}".format(self.path))
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