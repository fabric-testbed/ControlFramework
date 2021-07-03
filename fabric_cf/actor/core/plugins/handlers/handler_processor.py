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

import threading
from typing import TYPE_CHECKING

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import PluginException

if TYPE_CHECKING:
    from fabric_cf.actor.core.plugins.base_plugin import BasePlugin
    from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
    from fabric_cf.actor.core.plugins.handlers.configuration_mapping import ConfigurationMapping


class HandlerProcessor:
    def __init__(self):
        self.plugin = None
        self.logger = None
        self.initialized = False
        self.config_mappings = {}
        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['plugin']
        del state['initialized']
        del state['lock']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.plugin = None
        self.initialized = False
        self.lock = threading.Lock()

    def initialize(self):
        if not self.initialized:
            if self.plugin is None:
                raise PluginException(Constants.NOT_SPECIFIED_PREFIX.format("plugin"))
            self.logger = self.plugin.get_logger()
            self.load_config_mappings()
            self.initialized = True

    def load_config_mappings(self):
        try:
            self.lock.acquire()
            mappings = self.plugin.get_database().get_config_mappings()
            for m in mappings:
                self.config_mappings[m.get_key()] = m
        finally:
            self.lock.release()

    def add_config_mapping(self, *, mapping: ConfigurationMapping):
        try:
            self.lock.acquire()
            self.config_mappings[mapping.get_key()] = mapping
            self.plugin.get_database().add_config_mapping(key=mapping.get_key(), config_mapping=mapping)
        finally:
            self.lock.release()

    def create(self, unit: ConfigToken):
        self.logger.info("Executing Create")

        result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_CREATE,
                  Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_OK,
                  Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}

        self.plugin.configuration_complete(token=unit, properties=result)
        self.logger.info("Executing Create completed")

    def delete(self, unit: ConfigToken):
        self.logger.info("Executing Delete")

        result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_DELETE,
                  Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_OK,
                  Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}

        self.plugin.configuration_complete(token=unit, properties=result)
        self.logger.info("Executing Delete completed")

    def modify(self, unit: ConfigToken):
        self.logger.info("Executing Modify")

        result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_MODIFY,
                  Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_OK,
                  Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}

        self.plugin.configuration_complete(token=unit, properties=result)
        self.logger.info("Executing Modify completed")

    def set_logger(self, *, logger):
        self.logger = logger

    def set_plugin(self, *, plugin: BasePlugin):
        self.plugin = plugin

    @staticmethod
    def get_action_sequence_number(*, properties: dict):
        if properties is None:
            raise PluginException(Constants.INVALID_ARGUMENT)

        if Constants.PROPERTY_ACTION_SEQUENCE_NUMBER in properties:
            return int(properties[Constants.PROPERTY_ACTION_SEQUENCE_NUMBER])

        raise PluginException("Action Sequence Number not found")

    @staticmethod
    def get_result_code(*, properties: dict):
        if properties is None:
            raise PluginException(Constants.INVALID_ARGUMENT)

        if Constants.PROPERTY_TARGET_RESULT_CODE in properties:
            return int(properties[Constants.PROPERTY_TARGET_RESULT_CODE])

        raise PluginException("Target Result code not found")

    @staticmethod
    def get_result_code_message(*, properties: dict):
        if properties is None:
            raise PluginException(Constants.INVALID_ARGUMENT)

        if Constants.PROPERTY_TARGET_RESULT_CODE_MESSAGE in properties:
            return properties[Constants.PROPERTY_TARGET_RESULT_CODE_MESSAGE]

        return None

    @staticmethod
    def get_exception_message(*, properties: dict):
        if properties is None:
            raise PluginException(Constants.INVALID_ARGUMENT)

        if Constants.PROPERTY_EXCEPTION_MESSAGE in properties:
            return properties[Constants.PROPERTY_EXCEPTION_MESSAGE]

        return None

    def shutdown(self):
        return

    def start(self):
        return
