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
from abc import ABC, abstractmethod
from typing import Tuple

import yaml

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken


class ConfigurationException(Exception):
    pass


class HandlerBase(ABC):
    def __init__(self, logger: logging.Logger, properties: dict, process_lock: threading.Lock):
        self.properties = properties
        self.logger = logger
        self.config = None
        self.process_lock = process_lock

    def get_config(self) -> dict:
        if self.config is None:
            config_properties_file = self.properties.get(Constants.CONFIG_PROPERTIES_FILE, None)
            if config_properties_file is None:
                raise ConfigurationException("No data source has been specified")
            self.get_logger().debug(f"Reading config file: {config_properties_file}")
            with open(config_properties_file) as f:
                self.config = yaml.safe_load(f)
        return self.config

    def get_logger(self) -> logging.Logger:
        return self.logger

    @abstractmethod
    def create(self, unit: ConfigToken) -> Tuple[dict, ConfigToken]:
        """
        Create
        """

    @abstractmethod
    def modify(self, unit: ConfigToken) -> Tuple[dict, ConfigToken]:
        """
        Create
        """

    @abstractmethod
    def delete(self, unit: ConfigToken) -> Tuple[dict, ConfigToken]:
        """
        Create
        """
