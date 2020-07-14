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

from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.container.Container import Container

if TYPE_CHECKING:
    from fabric.actor.core.apis.IActorContainer import IActorContainer
    from fabric.actor.boot.Configuration import Configuration

import logging
import os
from fabric.actor.core.container.EventManager import EventManager


class Globals:
    LogDir = '.'
    LogFile = 'actor.log'
    LogLevel = 'DEBUG'
    LogRetain = '5'
    LogFileSize = '5000000'
    Logger = 'fabric.actor'
    OauthJwksUri = 'https://cilogon.org/oauth2/certs'

    ConfigFile = Constants.ConfigurationFile

    def __init__(self):
        self.config = None
        self.log = None
        self.initialized = False
        self.started = False
        self.start_completed = False
        self.event_manager = EventManager()
        self.container = None
        self.properties = None
        self.lock = threading.Lock()

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
        if Constants.PropertyConfLogDirectory in log_config and Constants.PropertyConfLogFile in log_config:
            log_path = log_config[Constants.PropertyConfLogDirectory] + '/' + log_config[Constants.PropertyConfLogFile]

        if log_path is None:
            raise RuntimeError('The log file path must be specified in config or passed as an argument')

        # Get the log level
        log_level = None
        if Constants.PropertyConfLogLevel in log_config :
           log_level = log_config[Constants.PropertyConfLogLevel]

        if log_level is None:
            log_level = logging.INFO

        # Set up the root logger
        log = logging.getLogger(log_config[Constants.PropertyConfLogger])
        log.setLevel(log_level)
        log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'

        logging.basicConfig(format=log_format, filename=log_path)

        return log

    def delete_super_block(self):
        if os.path.isfile(Constants.SuperblockLocation):
            os.remove(Constants.SuperblockLocation)

    def fail(self, e: Exception):
        self.log.error("Critical error: Actor failed to initialize {}".format(e))
        exit(-1)

    def initialize(self):
        try:
            self.lock.acquire()
            if not self.initialized:
                self.load_config()
                self.log = self.make_logger()
                self.log.info("Main container initialization complete.")
                self.initialized = True
        finally:
            self.lock.release()

    def load_config(self):
        try:
            from fabric.actor.boot.ConfigurationLoader import ConfigurationLoader
            loader = ConfigurationLoader(self.ConfigFile)
            self.config = loader.read_configuration()
        except Exception as e:
            raise RuntimeError("Unable to parse configuration file {}".format(e))

    def get_container(self) -> IActorContainer:
        if not self.initialized:
            raise Exception("Invalid state")
        return self.container

    def get_config(self) -> Configuration:
        if not self.initialized:
            raise Exception("Invalid state")
        return self.config

    def get_logger(self):
        if not self.initialized:
            raise Exception("Invalid state")

        if self.log is None:
            self.log = self.make_logger()
        return self.log

    def start(self, force_fresh: bool):
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
            try:
                self.lock.acquire()
                self.container = Container()
                self.log.info("Successfully instantiated the container implementation.")
                self.log.info("Initializing container")
                self.container.initialize(self.config)
                self.log.info("Successfully initialized the container")
                self.start_completed = True
            finally:
                self.lock.release()
        except Exception as e:
            # TODO
            raise e
            self.fail(e)

    def stop(self):
        try:
            self.lock.acquire()
            if not self.started:
                return
            self.log.info("Stopping Actor")
            self.started = False
            self.get_container().shutdown()
        except Exception as e:
            self.log.error("Error while shutting down: {}".format(e))
        finally:
            self.lock.release()


class GlobalsSingleton:
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise Exception("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = Globals()
        return self.__instance

    get = classmethod(get)