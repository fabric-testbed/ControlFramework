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

import pickle
import threading
import traceback
from typing import TYPE_CHECKING

import os
from enum import Enum

from yapsy.PluginManager import PluginManagerSingleton

from fabric.actor.core.container.remote_actor_cache import RemoteActorCacheSingleton
from fabric.actor.core.container.db.container_database import ContainerDatabase
from fabric.actor.core.proxies.actor_location import ActorLocation
from fabric.actor.core.proxies.proxy_factory import ProxyFactorySingleton
from fabric.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric.actor.core.time.actor_clock import ActorClock
from fabric.actor.core.util.reflection_utils import ReflectionUtils
from fabric.actor.core.apis.i_actor_container import IActorContainer
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.container.protocol_descriptor import ProtocolDescriptor
from fabric.actor.core.extensions.plugin_manager import PluginManager
from fabric.actor.core.kernel.kernel_tick import KernelTick
from fabric.actor.core.manage.management_object_manager import ManagementObjectManager
from fim.graph.neo4j_property_graph import Neo4jGraphImporter
from fabric.actor.core.util.id import ID


if TYPE_CHECKING:
    from fabric.actor.core.apis.i_actor import IActor
    from fabric.actor.core.apis.i_tick import ITick
    from fabric.actor.core.apis.i_actor_identity import IActorIdentity
    from fabric.actor.core.apis.i_container_database import IContainerDatabase
    from fabric.actor.boot.configuration import Configuration


class ContainerState(Enum):
    """
    Container State enumeration
    """
    Unknown = 1
    Starting = 2
    Recovering = 3
    Started = 4
    Stopping = 5
    Stopped = 6
    Failed = 7


class Container(IActorContainer):
    """
    Container is the "heart" of Fabric Core system. The container manager is responsible for managing the core instance.
    This is a singleton class.
    """

    PropertyBeginningOfTime = "BeginningOfTime"
    PropertyCycleMillis = "CycleMillis"
    PropertyManualTicks = "ManualTicks"

    PropertyTime = "time"

    def __init__(self):
        self.state = ContainerState.Unknown
        self.guid = None
        self.config = None
        self.ticker = None
        self.protocols = {}
        self.fresh = True
        self.recovered = False
        self.plugin_manager = PluginManager()
        self.management_object_manager = ManagementObjectManager()
        from fabric.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.db = None
        self.container_lock = threading.Lock()
        self.actor = None

    def get_actor(self) -> IActor:
        """
        Return the actor running in the container
        @return actor
        """
        return self.actor

    def cleanup_neo4j(self):
        """
        Cleanup Neo4j on clean restart
        """
        self.logger.debug("Cleanup Neo4j database started")
        config = self.config.get_global_config().get_neo4j_config()
        neo4j_graph_importer = Neo4jGraphImporter(url=config["url"], user=config["user"],
                                                  pswd=config["pass"],
                                                  import_host_dir=config["import_host_dir"],
                                                  import_dir=config["import_dir"])
        neo4j_graph_importer.delete_all_graphs()
        self.logger.debug("Cleanup Neo4j database completed")

    def determine_boot_mode(self):
        """
        Determine boot mode is clean restart or stateful restart
        """
        filename = Constants.SuperblockLocation
        self.logger.debug("Checking if this container is recovering. Looking for: {}".format(filename))
        if os.path.isfile(filename):
            self.logger.debug("Found super block file. This container is recovering")
            self.fresh = False
        else:
            self.logger.debug("Super block file does not exist. This is a fresh container")
            self.fresh = True

    def create_super_block(self):
        """
        Create Super Block
        """
        self.logger.debug("Creating superblock")
        file = None
        try:
            file = open(Constants.SuperblockLocation, 'r')
        except IOError:
            file = open(Constants.SuperblockLocation, 'w')
        finally:
            if file is not None:
                file.close()

    def boot(self):
        """
        Startup a container
        """
        self.logger.debug("Booting")
        self.boot_common()
        if self.is_fresh():
            self.logger.debug("Booting a fresh container")
            self.boot_basic()
            self.finish_fresh_boot()
        else:
            self.logger.debug("Recovering an existing container")
            self.recover_basic()
            self.recover_actors()
            self.finish_recovery_boot()
        self.write_state_file()

    def create_database(self):
        """
        Create Database
        """
        user = self.config.get_global_config().get_database().get(Constants.PropertyConfDbUser, None)
        password = self.config.get_global_config().get_database().get(Constants.PropertyConfDbPassword, None)
        dbname = self.config.get_global_config().get_database().get(Constants.PropertyConfDbName, None)
        dbhost = self.config.get_global_config().get_database().get(Constants.PropertyConfDbHost, None)
        self.db = ContainerDatabase(user=user, password=password, database=dbname, db_host=dbhost, logger=self.logger)
        if self.is_fresh():
            self.db.set_reset_state(value=True)
        else:
            self.db.set_reset_state(value=False)
        self.db.initialize()

    def initialize(self, *, config: Configuration):
        """
        Initialize container and actor
        """
        if config is None:
            raise Exception("config cannot be null")

        with self.container_lock:
            if self.state != ContainerState.Unknown:
                raise Exception("Cannot initialize container in state: {}".format(self.state))

            self.state = ContainerState.Starting

        self.config = config
        failed = False

        try:
            self.determine_boot_mode()
            self.create_database()
            self.boot()

            if self.is_fresh():
                try:
                    self.cleanup_neo4j()
                    from fabric.actor.boot.configuration_loader import ConfigurationLoader
                    loader = ConfigurationLoader()
                    loader.process(config=self.config)
                except Exception as e:
                    traceback.print_exc()
                    self.logger.error("Failed to instantiate actors {}".format(e))
                    self.logger.error("This container may need to be restored to a clean state")
                    raise e
        except Exception as e:
            self.logger.error(e)
            failed = True
            raise e
        finally:
            with self.container_lock:
                if failed:
                    self.state = ContainerState.Failed
                else:
                    self.state = ContainerState.Started

    def boot_common(self):
        """
        Perform common boot actions
        """
        self.logger.debug("Performing common boot tasks")
        self.define_protocols()
        self.plugin_manager.initialize(db=self.db)
        self.management_object_manager.initialize(db=self.db)
        from fabric.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
        RPCManagerSingleton.get().start()

    def define_protocols(self):
        """
        Define Protocols i.e. Kafka and Local
        """
        self.logger.debug("Defining container protocols")
        desc = ProtocolDescriptor(protocol=Constants.ProtocolLocal, location=None)
        self.logger.debug("Registering protocol {}".format(Constants.ProtocolLocal))
        self.register_protocol(protocol=desc)

        kafka_topic_name = self.get_config().get_actor().get_kafka_topic()
        self.logger.debug("Kafka Topic {}".format(kafka_topic_name))
        if kafka_topic_name is not None:
            desc = ProtocolDescriptor(protocol=Constants.ProtocolKafka, location=kafka_topic_name)
            self.logger.debug("Registering protocol {}".format(Constants.ProtocolKafka))
            self.register_protocol(protocol=desc)

    def boot_basic(self):
        """
        Perform basic boot actions
        """
        self.guid = self.get_config().get_global_config().get_container().get(Constants.PropertyConfContainerGuid, None)
        self.logger.debug("Container guid is {}".format(self.guid))
        self.set_time()
        self.persist_basic()
        self.create_super_block()

    def persist_basic(self):
        """
        Save basic and time information
        """
        self.persist_container()
        self.persist_time()

    def persist_container(self):
        """
        Save container information in database
        """
        properties = {Constants.PropertyConfContainerGuid: self.guid}
        self.db.add_container_properties(properties=properties)

    def persist_time(self):
        """
        Save time in database
        """
        properties = {self.PropertyTime: self.PropertyTime,
                      self.PropertyBeginningOfTime: self.ticker.get_beginning_of_time(),
                      self.PropertyCycleMillis: self.ticker.get_cycle_millis(),
                      self.PropertyManualTicks: self.ticker.is_manual()}
        self.db.add_time(properties=properties)

    def set_time(self):
        """
        Set the Actor clock and time
        """
        start_time = int(self.config.get_global_config().get_time().get(Constants.PropertyConfTimeStartTime, None))
        if start_time == -1:
            start_time = ActorClock.get_current_milliseconds()

        cycle_millis = int(self.config.get_global_config().get_time().get(Constants.PropertyConfTimeCycleMillis, None))

        manual = False
        if self.config.get_global_config().get_time().get(Constants.PropertyConfTimeManual, None):
            manual = True

        self.create_and_start_tick(start_time=start_time, cycle_millis=cycle_millis, manual=manual)

    def create_and_start_tick(self, *, start_time: int, cycle_millis: int, manual: bool):
        """
        Create Actor clock and start the ticker
        """
        self.logger.debug("Creating container ticker")
        self.ticker = KernelTick()
        self.ticker.set_beginning_of_time(value=start_time)
        self.ticker.set_cycle_millis(cycle_millis=cycle_millis)
        self.ticker.set_manual(value=manual)
        self.ticker.initialize()
        self.ticker.start()

        if not self.ticker.is_manual():
            self.logger.info("Using automatic ticks. Tick length={} ms".format(self.ticker.get_cycle_millis()))
        else:
            self.logger.info("Using manual ticks. Tick length={} ms".format(self.ticker.get_cycle_millis()))

    def finish_fresh_boot(self):
        """
        Complete fresh boot
        """
        self.create_container_manager_object()
        self.load_extensions()

    def finish_recovery_boot(self):
        """
        Complete recovery boot
        """
        return

    def create_container_manager_object(self):
        """
        Create Container Manager Object
        """
        self.logger.info("Creating container manager object")
        from fabric.actor.core.manage.container_management_object import ContainerManagementObject
        management_object = ContainerManagementObject()
        self.management_object_manager.register_manager_object(manager=management_object)

    def load_extensions(self):
        """
        Loads all container-level extensions specified in the container configuration
        """
        self.logger.info("Instantiating extensions...")
        # Load the core from the plugin directory.
        plugin_dir = self.config.get_global_config().get_runtime().get(Constants.PropertyConfPluginDir, None)
        manager = PluginManagerSingleton().get()
        manager.setPluginPlaces([plugin_dir])
        manager.collectPlugins()

        # Loop round the core and print their names.
        for plugin in manager.getAllPlugins():
            plugin.plugin_object.print_name()

    def recover_basic(self):
        """
        Recover Basic entities such as Container GUID and time
        """
        if self.is_fresh():
            raise Exception("A fresh container cannot be recovered")
        try:
            self.container_lock.acquire()
            if self.state != ContainerState.Starting:
                raise Exception("Invalid state for recovery: {}".format(self.state))
            self.state = ContainerState.Recovering
        finally:
            self.container_lock.release()

        self.recover_guid()
        self.recover_time()

    def recover_guid(self):
        """
        Recover GUID from database
        """
        self.logger.debug("Recovering container GUID")
        properties = self.db.get_container_properties()
        stored_guid = properties.get(Constants.PropertyConfContainerGuid, None)
        if stored_guid is None:
            raise Exception("Could not obtain saved container GUID from database")
        self.guid = ID(id=stored_guid)
        self.logger.info("Recovered container guid: {}".format(self.guid))

    def recover_time(self):
        """
        Recover time from database
        """
        self.logger.debug("Recovering container time settings")
        time_obj = self.db.get_time()
        if time_obj is None:
            raise Exception("Could not obtain container saved state from database")
        properties = time_obj.get('properties', None)
        beginning_of_time = properties.get(self.PropertyBeginningOfTime, None)
        cycle_millis = properties.get(self.PropertyCycleMillis, None)
        manual = properties.get(self.PropertyManualTicks, None)

        self.create_and_start_tick(start_time=beginning_of_time, cycle_millis=cycle_millis, manual=manual)

    def recover_actors(self):
        """
        Recover actors from Database (there is only one actor)
        """
        self.logger.info("Recovering actors")
        actor_list = self.db.get_actors()
        for a in actor_list:
            try:
                self.recover_actor(properties=a)
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.error("Could not recover actor {}, exception: {}".format(a.get('act_name', None), e))

    def recover_actor(self, *, properties: dict):
        """
        Recover actor
        """
        actor_name = properties.get('act_name', None)
        if actor_name is None:
            raise Exception("Cannot recover actor: no name")
        self.logger.info("Recover actor: {}".format(actor_name))
        self.logger.debug("Restoring actor from saved state")
        pickled_actor = properties.get(Constants.PropertyPickleProperties, None)
        if pickled_actor is None:
            raise Exception("Pickled Actor read from Data is None")
        actor = pickle.loads(pickled_actor)
        self.logger.debug("Initializing the actor object")
        actor.initialize()
        # By now we have a valid actor object. We need to register it with
        # the container and call recovery for its reservations.
        self.register_recovered_actor(actor=actor)
        self.logger.debug("Starting recovery from database for actor %s", actor_name)
        actor.recover()
        self.register(tickable=actor)

        self.logger.info("Actor %s recovered successfully", actor_name)

    def register(self, *, tickable: ITick):
        """
        Register Actor with timer thread
        @param tickable actor
        """
        self.ticker.add_tickable(tickable=tickable)

    def unregister(self, *, tickable: ITick):
        """
        Un-Register Actor with timer thread
        @param tickable actor
        """
        self.ticker.remove_tickable(tickable=tickable)

    def register_actor(self, *, actor: IActor):
        """
        Registers a new actor: adds the actor to the database, registers actor proxies and callbacks. Must not
        register the actor with the clock! Clock registration is a separate phase.
        @param actor actor
        @raises Exception in case of error
        """
        self.db.add_actor(actor=actor)
        actor.actor_added()
        self.register_management_object(actor=actor)
        self.register_common(actor=actor)
        self.actor = actor
        actor.start()

    def unregister_actor(self, *, actor: IActor):
        """
        Unregisters the actor from the container.
        @param actor actor
        @raises Exception in case of error
        """
        self.management_object_manager.unload_actor_manager_objects(actor_name=actor.get_name())
        ActorRegistrySingleton.get().unregister(actor=actor)

    def register_common(self, *, actor: IActor):
        """
        Performs the common steps required to register an actor with the container.
        @param actor actor
        @raises Exception in case of error
        """
        ActorRegistrySingleton.get().register_actor(actor=actor)
        self.register_proxies(actor=actor)

        RemoteActorCacheSingleton.get().register_with_registry(actor=actor)

    def register_management_object(self, *, actor: IActor):
        """
        Register Actor Management Object
        """
        module_name = actor.get_management_object_module()
        class_name = actor.get_management_object_class()
        mo = ReflectionUtils.create_instance(module_name=module_name, class_name=class_name)
        mo.set_actor(actor=actor)
        mo.initialize()
        self.management_object_manager.register_manager_object(manager=mo)

    def remove_actor(self, *, actor_name: str):
        """
        Remove actor metadata
        @param actor_name actor name;
        @raises Exception in case of error
        """
        self.db.remove_actor(actor_name=actor_name)
        actor = ActorRegistrySingleton.get().get_actor(actor_name_or_guid=actor_name)
        if actor is not None:
            ActorRegistrySingleton.get().unregister(actor=actor)
        actor.actor_removed()

    def remove_actor_database(self, *, actor_name: str):
        """
        Remove actor database
        @param actor_name actor name;
        @raises Exception in case of error
        """
        self.db.remove_actor(actor_name=actor_name)

    def register_protocol(self, *, protocol: ProtocolDescriptor):
        self.protocols[protocol.get_protocol()] = protocol
        self.logger.debug("Registered container protocol: {}".format(protocol.get_protocol()))

    def register_proxies(self, *, actor: IActor):
        """
        Registers all proxies for the specified actor.
        @param actor actor
        @raises Exception in case of error
        """
        self.logger.debug("Registering proxies for actor: {}".format(actor.get_name()))

        for protocol in self.protocols.values():
            self.logger.debug("Processing protocol {}".format(protocol.get_protocol()))
            location = ActorLocation()
            location.set_descriptor(descriptor=protocol)

            proxy = ProxyFactorySingleton.get().new_proxy(protocol=protocol.get_protocol(),
                                                          identity=actor.get_identity(), location=location)
            if proxy is not None:
                self.logger.debug(
                    "Registering proxy {} for protocol {}".format(actor.get_name(), protocol.get_protocol()))
                ActorRegistrySingleton.get().register_proxy(proxy=proxy)

            callback = ProxyFactorySingleton.get().new_callback(protocol=protocol.get_protocol(),
                                                                identity=actor.get_identity(), location=location)
            if callback is not None:
                self.logger.debug(
                    "Registering callback {} for protocol {}".format(actor.get_name(), protocol.get_protocol()))
                ActorRegistrySingleton.get().register_callback(callback=callback)

    def register_recovered_actor(self, *, actor: IActor):
        """
        Registers a recovered actor.
        @param actor recovered actor
        @raises Exception in case of error
        """
        self.logger.debug("Registering a recovered actor")
        actor.actor_added()
        self.register_common(actor=actor)
        self.load_actor_management_objects(actor=actor)
        self.actor = actor
        actor.start()

    def load_actor_management_objects(self, *, actor: IActor):
        """
        Load Actor Management Objects
        @param actor actor
        """
        try:
            self.management_object_manager.load_actor_manager_objects(actor_name=actor.get_name())
        except Exception as e:
            self.logger.error("Error while loading manager objects for actor: {} {}".format(actor.get_name(), e))

    def stop(self):
        if self.ticker is not None:
            self.ticker.stop()
        else:
            raise Exception("The container does not have a clock")

    def tick(self):
        if self.ticker is not None:
            self.ticker.tick()
        else:
            raise Exception("The container does not have a clock")

    def shutdown(self):
        try:
            self.logger.info("Actor container shutting down")
            self.remove_state_file()
            self.stop()
            self.logger.info("Stopping RPC Manager")
            from fabric.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
            RPCManagerSingleton.get().stop()
            self.logger.info("Stopping actors")
            actors = ActorRegistrySingleton.get().get_actors()
            for actor in actors:
                self.logger.info("Stopping actor: {}".format(actor.get_name()))
                actor.stop()
                self.unregister_actor(actor=actor)

            ActorRegistrySingleton.get().clear()
            self.logger.info("Container is no longer active")
        except Exception as e:
            self.logger.error("Exception occurred while shutting down e: %s", e)
            traceback.print_exc()

    @staticmethod
    def write_state_file():
        try:
            file = open(Constants.StateFileLocation, 'r')
        except IOError:
            file = open(Constants.StateFileLocation, 'w')
        finally:
            file.close()

    @staticmethod
    def remove_state_file():
        os.remove(Constants.StateFileLocation)

    def get_actor_clock(self) -> ActorClock:
        if self.ticker is None:
            raise Exception("No tick")
        return ActorClock(beginning_of_time=self.ticker.get_beginning_of_time(),
                          cycle_millis=self.ticker.get_cycle_millis())

    def get_current_cycle(self):
        if self.ticker is None:
            return -1
        return self.ticker.get_current_cycle()

    def is_manual_clock(self) -> bool:
        result = False
        if self.ticker is not None:
            result = self.ticker.is_manual()
        return result

    def get_guid(self) -> ID:
        return self.guid

    def get_database(self) -> IContainerDatabase:
        return self.db

    def get_config(self) -> Configuration:
        return self.config

    def is_fresh(self) -> bool:
        return self.fresh

    def is_recovered(self):
        return self.recovered

    def get_management_object_manager(self) -> ManagementObjectManager:
        return self.management_object_manager

    def get_management_object(self, *, key: ID):
        if key is None:
            raise Exception("key cannot be null")
        return self.management_object_manager.get_management_object(key=key)

    def get_plugin_manager(self) -> PluginManager:
        return self.plugin_manager

    def get_protocol_descriptor(self, *, protocol: str) -> ProtocolDescriptor:
        return self.protocols.get(protocol, None)

    @staticmethod
    def get_proxy(protocol: str, identity: IActorIdentity, location: ActorLocation, type: str = None):
        try:
            proxy = ActorRegistrySingleton.get().get_proxy(protocol=protocol, actor_name=identity.get_name())
            if proxy is None:
                proxy = ProxyFactorySingleton.get().new_proxy(protocol=protocol, identity=identity, location=location,
                                                              type=type)
                ActorRegistrySingleton.get().register_proxy(proxy=proxy)
            return proxy
        except Exception as e:
            raise e
