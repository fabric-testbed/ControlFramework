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
import os
from logging.handlers import RotatingFileHandler

from fabric_cf.actor.core.common.constants import Constants


class LogHelper:
    @staticmethod
    def make_logger(*, log_config: dict = None):
        """
        Detects the path and level for the log file from the actor config and sets
        up a logger. Instead of detecting the path and/or level from the
        config, a custom path and/or level for the log file can be passed as
        optional arguments.

       :param log_config: Log config
       :return: logging.Logger object
        """
        if log_config is None:
            raise RuntimeError('No config information available')

        log_path = None
        if Constants.PROPERTY_CONF_LOG_DIRECTORY in log_config and Constants.PROPERTY_CONF_LOG_FILE in log_config:
            log_path = log_config[Constants.PROPERTY_CONF_LOG_DIRECTORY] + '/' + \
                       log_config[Constants.PROPERTY_CONF_LOG_FILE]

        if log_path is None:
            raise RuntimeError('The log file path must be specified in config or passed as an argument')

        # Get the log level
        log_level = None
        if Constants.PROPERTY_CONF_LOG_LEVEL in log_config:
            log_level = log_config.get(Constants.PROPERTY_CONF_LOG_LEVEL, None)

        if log_level is None:
            log_level = logging.INFO

        # Set up the root logger
        log = logging.getLogger(log_config.get(Constants.PROPERTY_CONF_LOGGER, None))
        log.setLevel(log_level)
        log_format = \
            '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'

        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        backup_count = log_config.get(Constants.PROPERTY_CONF_LOG_RETAIN, None)
        max_log_size = log_config.get(Constants.PROPERTY_CONF_LOG_SIZE, None)

        file_handler = RotatingFileHandler(log_path, backupCount=int(backup_count), maxBytes=int(max_log_size))

        logging.basicConfig(handlers=[file_handler], format=log_format)

        return log