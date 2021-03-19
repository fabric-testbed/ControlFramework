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
import threading

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
from fabric_cf.actor.core.apis.abc_proxy import ABCProxy
from fabric_cf.actor.core.common.exceptions import RegistryException
from fabric_cf.actor.core.registry.callback_registry import CallbackRegistry
from fabric_cf.actor.core.registry.proxy_registry import ProxyRegistry


class ActorRegistry:
    """
    Maintains a collection of uniquely named actors residing in a given docker container or
    known to the docker container (singleton), indexed by type and by owner.
    getActor(String) now looks up EITHER by name or guid - tries one, then the other
    """
    class ActorRegistryEntry:
        def __init__(self, *, actor: ABCActorMixin):
            self.actor = actor
            self.kafka_topics = {}

        def get_actor(self) -> ABCActorMixin:
            return self.actor

        def get_kafka_topic(self, *, protocol: str) -> str:
            return self.kafka_topics[protocol]

        def register_kafka_topic(self, *, protocol: str, kafka_topic: str):
            self.kafka_topics[protocol] = kafka_topic

    def __init__(self):
        self.actors = {}
        self.actors_by_guid = {}
        self.proxies = ProxyRegistry()
        self.callbacks = CallbackRegistry()
        self.lock = threading.Lock()

    def clear(self):
        try:
            self.lock.acquire()
            self.actors.clear()
            self.actors_by_guid.clear()
            self.proxies.clear()
            self.callbacks.clear()
        finally:
            self.lock.release()

    def get_actor(self, *, actor_name_or_guid: str) -> ABCActorMixin:
        result = None
        entry = None
        try:
            self.lock.acquire()
            if actor_name_or_guid in self.actors:
                entry = self.actors[actor_name_or_guid]

            if actor_name_or_guid in self.actors_by_guid:
                entry = self.actors_by_guid[actor_name_or_guid]

            if entry is not None:
                result = entry.actor
        finally:
            self.lock.release()
        return result

    def get_actors(self) -> list:
        result = []
        try:
            self.lock.acquire()
            for actor in self.actors.values():
                result.append(actor.get_actor())
        finally:
            self.lock.release()
        return result

    def get_broker_proxies(self, *, protocol: str) -> list:
        result = None
        try:
            self.lock.acquire()
            result = self.proxies.get_broker_proxies(protocol=protocol)
        finally:
            self.lock.release()
        return result

    def get_callback(self, *, protocol: str, actor_name: str) -> ABCCallbackProxy:
        result = None
        try:
            self.lock.acquire()
            result = self.callbacks.get_callback(protocol=protocol, actor_name=actor_name)
        finally:
            self.lock.release()
        return result

    def get_kafka_topic(self, *, protocol: str, actor_name: str):
        result = None
        try:
            self.lock.acquire()
            if actor_name in self.actors:
                result = self.actors[actor_name].get_kafka_topic(protocol)
        finally:
            self.lock.release()
        return result

    def get_proxies(self, *, protocol: str) -> list:
        result = None
        try:
            self.lock.acquire()
            result = self.proxies.get_proxies(protocol=protocol)
        finally:
            self.lock.release()
        return result

    def get_proxy(self, *, protocol: str, actor_name: str) -> ABCProxy:
        result = None
        try:
            self.lock.acquire()
            result = self.proxies.get_proxy(protocol=protocol, actor_name=actor_name)
        finally:
            self.lock.release()
        return result

    def get_site_proxies(self, *, protocol: str) -> list:
        result = None
        try:
            self.lock.acquire()
            result = self.proxies.get_site_proxies(protocol=protocol)
        finally:
            self.lock.release()
        return result

    def register_actor(self, *, actor: ABCActorMixin):
        actor_name = actor.get_name()
        guid = actor.get_guid()

        try:
            self.lock.acquire()
            if actor_name in self.actors or guid in self.actors_by_guid:
                raise RegistryException("An actor name {} already exists".format(actor_name))

            entry = self.ActorRegistryEntry(actor=actor)
            self.actors_by_guid[guid] = entry
            self.actors[actor_name] = entry
        finally:
            self.lock.release()

    def register_callback(self, *, callback: ABCCallbackProxy):
        try:
            self.lock.acquire()
            self.callbacks.register_callback(callback=callback)
        finally:
            self.lock.release()

    def register_kafka_topic(self, *, actor_name: str, protocol: str, kafka_topic: str):
        try:
            self.lock.acquire()
            if actor_name not in self.actors:
                raise RegistryException("Actor name {} not found".format(actor_name))

            entry = self.actors[actor_name]
            entry.register_kafka_topic(protocol, kafka_topic)
        finally:
            self.lock.release()

    def register_proxy(self, *, proxy: ABCProxy):
        try:
            self.lock.acquire()
            self.proxies.register_proxy(proxy=proxy)
        finally:
            self.lock.release()

    def unregister(self, *, actor: ABCActorMixin):
        try:
            self.lock.acquire()
            actor_name = actor.get_name()
            guid = actor.get_guid()

            if actor_name in self.actors:
                self.actors.pop(actor_name)
            if guid in self.actors_by_guid:
                self.actors_by_guid.pop(guid)
            self.callbacks.unregister(actor_name=actor_name)
            self.proxies.unregister(actor_name=actor_name)
        finally:
            self.lock.release()


class ActorRegistrySingleton:
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise RegistryException("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = ActorRegistry()
        return self.__instance

    get = classmethod(get)
