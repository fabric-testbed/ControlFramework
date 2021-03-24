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
from fabric_cf.actor.core.common.exceptions import RegistryException
from fabric_cf.actor.core.proxies.proxy import Proxy

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin


class PeerRegistry:
    def __init__(self):
        # A hash table of proxies to brokers. Persisted to the data store.
        self.brokers = {}
        self.default_broker = None

        # Cache of objects per actor name
        self.cache = {}
        self.plugin = None
        self.logger = None
        self.initialized = False
        self.lock = threading.Lock()

    def actor_added(self):
        self.load_from_db()

    def add_broker(self, *, broker: ABCBrokerProxy):
        try:
            self.lock.acquire()
            self.brokers[broker.get_identity().get_guid()] = broker

            if self.default_broker is None:
                self.default_broker = broker
        finally:
            self.lock.release()

        try:
            self.plugin.get_database().add_broker(broker=broker)
            self.logger.info("Added {} as broker".format(broker.get_name()))
        except Exception as e:
            self.plugin.get_logger().error("Error while adding broker {}".format(e))

    def get_broker(self, *, guid: ID) -> ABCBrokerProxy:
        ret_val = None
        try:
            self.lock.acquire()
            ret_val = self.brokers.get(guid, None)
        finally:
            self.lock.release()
        return ret_val

    def get_brokers(self) -> list:
        ret_val = []
        try:
            self.lock.acquire()
            for b in self.brokers.values():
                ret_val.append(b)
        finally:
            self.lock.release()
        return ret_val

    def get_default_broker(self) -> ABCBrokerProxy:
        try:
            self.lock.acquire()
            return self.default_broker
        finally:
            self.lock.release()

    def initialize(self):
        if not self.initialized:
            if self.plugin is None:
                raise RegistryException(Constants.NOT_SPECIFIED_PREFIX.format("plugin"))

            self.logger = self.plugin.get_logger()

    def load_from_db(self):
        brokers = self.plugin.get_database().get_brokers()
        count = 0
        if brokers is None:
            return
        for b in brokers:
            agent = Proxy.get_proxy(proxy_reload_from_db=b)
            self.brokers[agent.get_identity().get_guid()] = agent
            if count == 0:
                self.default_broker = agent
            count += 1

    def remove_broker(self, *, broker: ABCBrokerProxy):
        try:
            self.lock.acquire()
            if broker.get_identity() in self.brokers:
                self.brokers.pop(broker.get_identity)
        finally:
            self.lock.release()

    def set_slices_plugin(self, *, plugin: ABCBasePlugin):
        self.plugin = plugin

