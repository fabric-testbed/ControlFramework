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

    def check_peer(self, *, from_mgmt_actor: ABCMgmtActor, from_guid: ID, to_mgmt_actor: ABCMgmtActor, to_guid: ID):
        """
        Check if a peer is already connected
        @param from_mgmt_actor from actor
        @param from_guid guid of from actor
        @param to_mgmt_actor to actor
        @param to_guid guid of to actor
        """
        self.logger.debug("from_mgmt_actor={} from_guid={} to_mgmt_actor={} to_guid={}".format(type(from_mgmt_actor),
                                                                                               from_guid,
                                                                                               type(to_mgmt_actor),
                                                                                               to_guid))
        try:
            # For Broker/AM
            if to_mgmt_actor is not None and isinstance(to_mgmt_actor, ABCMgmtServerActor):
                clients = to_mgmt_actor.get_clients(guid=from_guid)
                if clients is not None:
                    self.logger.debug("Edge between {} and {} exists (client)".format(from_guid, to_guid))
                    return True

            # For Orchestrator/Broker
            elif from_mgmt_actor is not None and isinstance(from_mgmt_actor, ABCMgmtClientActor):
                brokers = from_mgmt_actor.get_brokers(broker=to_guid)
                if brokers is not None:
                    self.logger.debug("Edge between {} and {} exists (broker)".format(from_guid, to_guid))
                    return True
        except Exception as e:
            raise RemoteActorCacheException("Unable to cast actor {} or {} e={}".format(from_guid, to_guid, e))

        self.logger.debug("Edge between {} and {} does not exist".format(from_guid, to_guid))
        return False

    def establish_peer_private(self, *, from_mgmt_actor: ABCMgmtActor, from_guid: ID, to_mgmt_actor: ABCMgmtActor,
                               to_guid: ID) -> ClientMng:
        """
        Establish connection i.e. create either proxies or clients between peer
        @param from_mgmt_actor from actor
        @param from_guid guid of from actor
        @param to_mgmt_actor to actor
        @param to_guid guid of to actor
        """
        self.logger.debug("establish_peer_private IN")
        client = None
        from_map = self.get_cache_entry_copy(guid=from_guid)
        to_map = self.get_cache_entry_copy(guid=to_guid)

        if from_map is None:
            raise RemoteActorCacheException("Actor {} does not have a registry cache entry".format(from_guid))

        if to_map is None:
            raise RemoteActorCacheException("Actor {} does not have a registry cache entry".format(to_guid))

        if from_mgmt_actor is not None:
            self.logger.debug("From actor {} is local".format(from_mgmt_actor.get_name()))

            protocol = Constants.PROTOCOL_LOCAL
            kafka_topic = None

            if self.actor_location in to_map:
                if self.actor_protocol not in to_map:
                    raise RemoteActorCacheException("Actor {} does not specify communications protocol (local/kafka)".
                                                    format(to_map[self.actor_name]))

                protocol = to_map.get(self.actor_protocol, None)
                kafka_topic = to_map[self.actor_location]
                self.logger.debug("Added To actor location (non-local) {}".format(to_map[self.actor_location]))

            identity = ActorIdentity(name=to_map[self.actor_name], guid=to_guid)

            if kafka_topic is not None and isinstance(from_mgmt_actor, ABCMgmtClientActor):
                self.logger.debug("Kafka Topic is available, registering broker proxy")
                proxy = ProxyAvro()
                proxy.set_protocol(protocol)
                proxy.set_guid(str(identity.get_guid()))
                proxy.set_name(identity.get_name())
                proxy.set_type(to_map[self.actor_type])
                proxy.set_kafka_topic(kafka_topic)

                try:
                    if not from_mgmt_actor.add_broker(broker=proxy):
                        raise RemoteActorCacheException("Could not register broker {}".
                                                        format(from_mgmt_actor.get_last_error()))
                except Exception as e:
                    self.logger.error(e)
                    self.logger.error(traceback.format_exc())
            else:
                self.logger.debug("Not adding broker to actor at this time because the remote actor actor "
                                  "kafka topic is not available")

            if to_mgmt_actor is not None and isinstance(to_mgmt_actor, ABCMgmtServerActor):
                self.logger.debug("Creating a client for local to actor")
                client = ClientMng()
                client.set_name(name=from_mgmt_actor.get_name())
                client.set_guid(guid=str(from_mgmt_actor.get_guid()))
                try:
                    to_mgmt_actor.register_client(client=client, kafka_topic=kafka_topic)
                except Exception as e:
                    raise RemoteActorCacheException("Could not register actor: {} as a client of actor: {} e= {}".
                                                    format(client.get_name(), to_mgmt_actor.get_name(), e))
        else:
            # fromActor is remote: toActor must be local
            # no-need to create any proxies
            # we only need to register clients
            if to_mgmt_actor is None:
                raise RemoteActorCacheException("Both peer endpoints are non local actors: {} {}".format(
                    from_map[self.actor_name], to_map[self.actor_name]))

            if self.actor_guid not in from_map:
                raise RemoteActorCacheException("Missing guid for remote actor: {}".format(from_map[self.actor_name]))

            self.logger.debug("From actor was remote, to actor {} is local".format(to_mgmt_actor.get_name()))
            if self.actor_location in from_map and isinstance(to_mgmt_actor, ABCMgmtServerActor):
                kafka_topic = from_map[self.actor_location]
                self.logger.debug("From actor has kafka topic")
                self.logger.debug("Creating client for from actor {}".format(from_map[self.actor_name]))
                client = ClientMng()
                client.set_name(name=from_map[self.actor_name])
                client.set_guid(guid=str(from_map[self.actor_guid]))
                try:
                    to_mgmt_actor.register_client(client=client, kafka_topic=kafka_topic)
                except Exception as e:
                    raise RemoteActorCacheException(
                        "Could not register actor: {} as a client of actor: {} e= {}".format(
                            client.get_name(), to_mgmt_actor.get_name(), e))
            else:
                self.logger.debug("Not adding client to actor at this time - remote actor topic not available")
        self.logger.debug("establish_peer_private OUT {}".format(client))
        return client

    def establish_peer(self, *, from_guid: ID, from_mgmt_actor: ABCMgmtActor, to_guid: ID,
                       to_mgmt_actor: ABCMgmtActor) -> ClientMng:
        """
        Check if peer exists in cache and if not Establish connection i.e. create either proxies or clients between peer
        @param from_mgmt_actor from actor
        @param from_guid guid of from actor
        @param to_mgmt_actor to actor
        @param to_guid guid of to actor
        """
        self.logger.debug("establish_peer IN")
        client = None
        if from_guid is None or to_guid is None:
            self.logger.error("Cannot establish peer when either guid is not known")
            raise RemoteActorCacheException("Cannot establish peer when either guid is not known")
        try:
            if not self.check_peer(from_mgmt_actor=from_mgmt_actor, from_guid=from_guid,
                                   to_mgmt_actor=to_mgmt_actor, to_guid=to_guid):

                client = self.establish_peer_private(from_mgmt_actor=from_mgmt_actor, from_guid=from_guid,
                                                     to_mgmt_actor=to_mgmt_actor, to_guid=to_guid)

                self.check_to_remove_entry(guid=from_guid)
                self.check_to_remove_entry(guid=to_guid)

                self.logger.debug("Peer established from {} to {}".format(from_guid, to_guid))

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Peer could not be established from {} to {} e:={}".format(from_guid, to_guid, e))
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
