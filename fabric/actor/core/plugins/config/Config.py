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

if TYPE_CHECKING:
    from fabric.actor.core.plugins.base_plugin import BasePlugin
    from fabric.actor.core.plugins.config.config_token import ConfigToken
    from fabric.actor.core.plugins.config.configuration_mapping import ConfigurationMapping


class Config:
    # Specifies a configuration file or action to execute.
    PropertyConfig = "config.action"
    PropertyExceptionMessage = "exception.message"
    PropertyExceptionStack = "exception.stack"
    PropertyTargetName = "target.name"
    PropertyTargetResultCode = "target.code"
    PropertyTargetResultCodeMessage = "target.code.message"
    PropertyConfigurationProperties = "config"
    PropertyActionSequenceNumber = "action.sequence"

    ResultCodeException = -1
    ResultCodeOK = 0
    TargetJoin = "join"
    TargetLeave = "leave"
    TargetModify = "modify"
    PropertyResourceType = "unit.resourceType"
    PropertyUnitAll = "unit.all"

    def __init__(self):
        self.is_sync = False
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
                raise Exception("Missing plugin")
            self.logger = self.plugin.get_logger()
            self.initialized = True

    def add_config_mapping(self, mapping: ConfigurationMapping):
        try:
            self.lock.acquire()
            self.config_mappings[mapping.get_key()] = mapping
        finally:
            self.lock.release()

    def join(self, token: ConfigToken, properties: dict):
        self.logger.info("Executing Join")

        # TODO
        result = {self.PropertyTargetName: self.TargetJoin,
                  self.PropertyTargetResultCode: self.ResultCodeOK,
                  self.PropertyActionSequenceNumber: 0 }

        self.plugin.configuration_complete(token, result)
        self.logger.info("Executing Join completed")

    def leave(self, token: ConfigToken, properties: dict):
        self.logger.info("Executing Leave")

        # TODO
        result = {self.PropertyTargetName: self.TargetLeave,
                  self.PropertyTargetResultCode: self.ResultCodeOK,
                  self.PropertyActionSequenceNumber: 0}

        self.plugin.configuration_complete(token, result)
        self.logger.info("Executing Leave completed")

    def modify(self, token: ConfigToken, properties: dict):
        self.logger.info("Executing Modify")

        # TODO
        result = {self.PropertyTargetName: self.TargetModify,
                  self.PropertyTargetResultCode: self.ResultCodeOK,
                  self.PropertyActionSequenceNumber: 0}

        self.plugin.configuration_complete(token, result)
        self.logger.info("Executing Modify completed")

    def set_logger(self, logger):
        self.logger = logger

    def set_slices_plugin(self, plugin: BasePlugin):
        self.plugin = plugin

    @staticmethod
    def get_action_sequence_number(properties: dict):
        if properties is None:
            raise Exception("Invalid Argument")

        if Config.PropertyActionSequenceNumber in properties:
            return int(properties[Config.PropertyActionSequenceNumber])

        raise Exception("Action Sequence Number not found")

    @staticmethod
    def get_result_code(properties: dict):
        if properties is None:
            raise Exception("Invalid Argument")

        if Config.PropertyTargetResultCode in properties:
            return int(properties[Config.PropertyTargetResultCode])

        raise Exception("Target Result code not found")

    @staticmethod
    def get_result_code_message(properties: dict):
        if properties is None:
            raise Exception("Invalid Argument")

        if Config.PropertyTargetResultCodeMessage in properties:
            return properties[Config.PropertyTargetResultCodeMessage]

        return None

    @staticmethod
    def get_exception_message(properties: dict):
        if properties is None:
            raise Exception("Invalid Argument")

        if Config.PropertyExceptionMessage in properties:
            return properties[Config.PropertyExceptionMessage]

        return None