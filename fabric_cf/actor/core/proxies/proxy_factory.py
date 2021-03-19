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
from typing import TYPE_CHECKING

from fabric_cf.actor.core.common.exceptions import ProxyException
from fabric_cf.actor.core.proxies.kafka.kafka_proxy_factory import KafkaProxyFactory
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.proxies.local.local_proxy_factory import LocalProxyFactory

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_identity import ABCActorIdentity
    from fabric_cf.actor.core.proxies.actor_location import ActorLocation


class ProxyFactory:
    def __init__(self):
        self.factories = {}
        self.load_factories()

    def load_factories(self):
        self.factories[Constants.PROTOCOL_LOCAL] = LocalProxyFactory()
        self.factories[Constants.PROTOCOL_KAFKA] = KafkaProxyFactory()

    def new_callback(self, *, protocol: str, identity: ABCActorIdentity, location: ActorLocation):
        if protocol in self.factories:
            factory = self.factories[protocol]
            return factory.new_callback(identity=identity, location=location)
        return None

    def new_proxy(self, *, protocol: str, identity: ABCActorIdentity, location: ActorLocation, proxy_type: str = None):
        if protocol in self.factories:
            factory = self.factories[protocol]
            return factory.new_proxy(identity=identity, location=location, proxy_type=proxy_type)
        return None


class ProxyFactorySingleton:
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise ProxyException("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = ProxyFactory()
        return self.__instance

    get = classmethod(get)

