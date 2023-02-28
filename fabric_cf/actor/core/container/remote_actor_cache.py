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
import traceback
from typing import TYPE_CHECKING

from fabric_cf.actor.core.apis.abc_mgmt_client_actor import ABCMgmtClientActor
from fabric_cf.actor.core.apis.abc_mgmt_server_actor import ABCMgmtServerActor
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.actor_identity import ActorIdentity
from fabric_cf.actor.core.manage.messages.client_mng import ClientMng
from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_mgmt_actor import ABCMgmtActor
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin


class RemoteActorCacheException(Exception):
    """
    Exception raised by RemoteActorCache
    """


class RemoteActorCache:
    """
    Maintains Remote Actors to which Actor is connected
    """
    actor_name = "NAME"
    actor_guid = "GUID"
    actor_location = "LOCATION"
    actor_protocol = "PROTOCOL"
    actor_type = "TYPE"

    def __init__(self):
        self.cache = {}
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.lock = threading.Lock()
        self.local_actor_guids = set()

    def known_guids(self) -> set:
        """
        Returns list of know GUIDs
        """
        try:
            self.lock.acquire()
            result = set()
            for k in self.cache.keys():
                result.add(k)
            return result
        finally:
            self.lock.release()

    def check_to_remove_entry(self, *, guid: ID):
        """
        Check if actor entry can be removed
        @param guid actor guid
        """
        if guid is None:
            return

        try:
            self.lock.acquire()
            if guid in self.cache:
                value = self.cache[guid]
                if self.actor_location not in value:
                    self.cache.pop(guid)
        finally:
            self.lock.release()

    def add_cache_entry(self, *, guid: ID, entry: dict):
        """
        Add actor entry to Cache
        @param guid actor guid
        @param entry actor entry
        """
        if guid is None or entry is None:
            return

        try:
            self.lock.acquire()
            self.non_mt_cache_merge(guid=guid, entry=entry)
            self.local_actor_guids.add(guid)
        finally:
            self.lock.release()

    def add_partial_cache_entry(self, *, guid: ID, entry: dict):
        """
        Add partial actor entry to Cache
        @param guid actor guid
        @param entry actor entry
        """
        if guid is None or entry is None:
            return
        try:
            self.lock.acquire()
            self.non_mt_cache_merge(guid=guid, entry=entry)
        finally:
            self.lock.release()

    def non_mt_cache_merge(self, *, guid: ID, entry: dict):
        """
        Merge to an existing entry if present; remove otherwise
        @param guid actor guid
        @param entry actor entry
        """
        if guid not in self.cache:
            self.logger.debug("Inserting new entry for {}".format(guid))
            self.cache[guid] = entry
            return
        current = self.cache[guid]
        for k, v in entry.items():
            current[k] = v

        self.cache[guid] = current

    def get_cache_entry_copy(self, *, guid: ID) -> dict:
        """
        Get a copy of cacher entry
        @param guid actor guid
        """
        try:
            self.lock.acquire()
            if guid in self.cache:
                ret_val = self.cache[guid]
                return ret_val
        finally:
            self.lock.release()

    def check_peer(self, *, mgmt_actor: ABCMgmtActor, peer_guid: ID, peer_type: ActorType):
        """
        Check if a peer is already connected
        @param mgmt_actor mgmt_actor
        @param peer_guid peer_guid
        @param peer_type peer_type
        """
        self.logger.debug(f"Check if Peer {peer_guid}/{peer_type} already exists!")
        try:
            # For Broker - all AMs are added as proxies
            # For Orchestrator - all peers will be added as Proxies
            if isinstance(mgmt_actor, ABCMgmtClientActor) and peer_type in [ActorType.Authority, ActorType.Broker]:
                self.logger.debug(f"Checking brokers")
                brokers = mgmt_actor.get_brokers(broker=peer_guid)
                self.logger.debug(f"brokers -- {brokers}")
                if brokers is not None:
                    self.logger.debug(f"Edge between {mgmt_actor.get_guid()} and {peer_guid} exists (broker)")
                    return True

            # For AM - all peers will be added as clients
            # For Broker - orchestrator as client
            elif isinstance(mgmt_actor, ABCMgmtServerActor) and peer_type in [ActorType.Orchestrator, ActorType.Broker]:
                self.logger.debug(f"Checking clients")
                clients = mgmt_actor.get_clients(guid=peer_guid)
                self.logger.debug(f"clients -- {clients} {mgmt_actor.get_last_error()}")
                if clients is not None:
                    self.logger.debug(f"Edge between {mgmt_actor.get_guid()} and {peer_guid} exists (client)")
                    return True
        except Exception as e:
            raise RemoteActorCacheException(f"Unable to cast actor {mgmt_actor.get_guid()} or {peer_guid} e={e}")

        self.logger.debug(f"Edge between {mgmt_actor.get_guid()} and {peer_guid} does not exist")
        return False

    def establish_peer_private(self, *, mgmt_actor: ABCMgmtActor, peer_guid: ID, peer_type: ActorType,
                               update: bool = False) -> ClientMng:
        """
        Establish connection i.e. create either proxies or clients between peer
        @param mgmt_actor mgmt_actor
        @param peer_guid peer_guid
        @param peer_type peer_type
        @param update update
        """
        self.logger.debug("establish_peer_private IN")
        client = None
        cache_entry = self.get_cache_entry_copy(guid=peer_guid)
        if cache_entry is None:
            raise RemoteActorCacheException(f"Actor {peer_guid} does not have a registry cache entry")

        protocol = cache_entry.get(self.actor_protocol)
        kafka_topic = cache_entry.get(self.actor_location)
        identity = ActorIdentity(name=cache_entry.get(self.actor_name), guid=peer_guid)

        if kafka_topic is None:
            raise RemoteActorCacheException(f"Actor {peer_guid} does not have a kafka topic")

        if isinstance(mgmt_actor, ABCMgmtClientActor) and peer_type in [ActorType.Authority, ActorType.Broker]:
            proxy = ProxyAvro()
            proxy.set_protocol(protocol)
            proxy.set_guid(str(identity.get_guid()))
            proxy.set_name(identity.get_name())
            proxy.set_type(cache_entry.get(self.actor_type))
            proxy.set_kafka_topic(kafka_topic)

            try:
                if not update:
                    self.logger.debug(f"Creating new proxy: {proxy}")
                    if not mgmt_actor.add_broker(broker=proxy):
                        raise RemoteActorCacheException(f"Could not register broker {peer_guid} "
                                                        f"error: {mgmt_actor.get_last_error()}")
                else:
                    self.logger.debug(f"Updating existing proxy: {proxy}")
                    if not mgmt_actor.update_broker(broker=proxy):
                        raise RemoteActorCacheException(f"Could not update broker {peer_guid} "
                                                        f"error: {mgmt_actor.get_last_error()}")
            except Exception as e:
                self.logger.error(e)
                self.logger.error(traceback.format_exc())
        elif isinstance(mgmt_actor, ABCMgmtServerActor) and peer_type in [ActorType.Orchestrator, ActorType.Broker]:
            client = ClientMng()
            client.set_name(name=cache_entry.get(self.actor_name))
            client.set_guid(guid=str(peer_guid))
            try:
                if not update:
                    self.logger.debug(f"Creating new client: {client}")
                    mgmt_actor.register_client(client=client, kafka_topic=kafka_topic)
                else:
                    self.logger.debug(f"Updating existing client: {client}")
                    mgmt_actor.update_client(client=client, kafka_topic=kafka_topic)
            except Exception as e:
                raise RemoteActorCacheException(f"Could not register actor: {peer_guid} as a client of "
                                                f"actor: {mgmt_actor} e= {e}")

        self.logger.debug("establish_peer_private OUT {}".format(client))
        return client

    def establish_peer(self, *, mgmt_actor: ABCMgmtActor, peer_guid: ID, peer_type: ActorType) -> ClientMng:
        """
        Check if peer exists in cache and if not Establish connection i.e. create either proxies or clients between peer
        @param mgmt_actor mgmt_actor
        @param peer_guid peer_guid
        @param peer_type peer_type
        """
        self.logger.debug("establish_peer IN")
        client = None
        if mgmt_actor is None or peer_guid is None:
            self.logger.error("Cannot establish peer when either guid is not known")
            raise RemoteActorCacheException("Cannot establish peer when either guid is not known")
        try:
            update = self.check_peer(mgmt_actor=mgmt_actor, peer_guid=peer_guid, peer_type=peer_type)

            client = self.establish_peer_private(mgmt_actor=mgmt_actor, peer_guid=peer_guid, peer_type=peer_type,
                                                 update=update)

            self.check_to_remove_entry(guid=peer_guid)

            self.logger.debug(f"Peer established from {mgmt_actor} to {peer_guid}")

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Peer could not be established from {mgmt_actor} to {peer_guid} e:={e}")
        self.logger.debug("establish_peer OUT {}".format(client))
        return client

    def register_with_registry(self, *, actor: ABCActorMixin):
        """
        Register an actor with Registry
        @param actor actor
        """
        try:
            act_name = actor.get_name()
            act_type = actor.get_type()
            act_guid = actor.get_guid()

            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            kafka_topic = GlobalsSingleton.get().get_config().get_actor_config().get_kafka_topic()

            entry = {self.actor_name: act_name,
                     self.actor_guid: act_guid,
                     self.actor_type: act_type.name,
                     self.actor_protocol: Constants.PROTOCOL_KAFKA,
                     self.actor_location: kafka_topic}

            self.add_cache_entry(guid=act_guid, entry=entry)
            # TODO start liveness thread
        except Exception as e:
            self.logger.debug("Could not register actor {} with local registry e: {}".format(actor.get_name(), e))


class RemoteActorCacheSingleton:
    """
    Remote Actor Cache Singleton
    """
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise RemoteActorCacheException("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = RemoteActorCache()
        return self.__instance

    get = classmethod(get)
