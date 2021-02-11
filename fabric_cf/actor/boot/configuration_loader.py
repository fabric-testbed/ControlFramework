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

from fabric_cf.actor.boot.configuration import Configuration
from fabric_cf.actor.boot.configuration_exception import ConfigurationException
from fabric_cf.actor.boot.configuration_processor import ConfigurationProcessor


class ConfigurationLoader:
    """
    Loads handlers read from a file
    """
    def __init__(self, *, path: str = None):
        self.path = path
        self.config = None

    def process(self, *, config: Configuration = None):
        """
        Read handlers and parse it
        @param config handlers
        """
        if config is None:
            self.read_configuration()
        else:
            self.config = config
        init = ConfigurationProcessor(config=self.config)
        init.process()

    def read_configuration(self) -> Configuration:
        """
        Read handlers file
        """
        if self.path is None:
            raise ConfigurationException("No data source has been specified")
        print("Reading handlers file: {}".format(self.path))
        config_dict = None
        with open(self.path) as f:
            config_dict = yaml.safe_load(f)
        self.config = Configuration(config=config_dict)
        return self.config
