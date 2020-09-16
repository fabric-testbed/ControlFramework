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

import os
from enum import Enum

from yapsy.PluginManager import PluginManagerSingleton

from fabric.actor.core.apis.i_mgmt_actor import IMgmtActor
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

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_actor import IActor
    from fabric.actor.core.apis.i_tick import ITick
    from fabric.actor.core.util.id import ID
    from fabric.actor.core.apis.i_actor_identity import IActorIdentity
    from fabric.actor.core.apis.i_container_database import IContainerDatabase
    from fabric.actor.boot.configuration import Configuration


class ContainerState(Enum):
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
        return self.actor

    def determine_boot_mode(self):
        filename = Constants.SuperblockLocation
        self.logger.debug("Checking if this container is recovering. Looking for: {}".format(filename))
        if os.path.isfile(filename):
            self.logger.debug("Found super block file. This container is recovering")
            self.fresh = False
        else:
            self.logger.debug("Super block file does not exist. This is a fresh container")
            self.fresh = True

    def create_super_block(self):
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
        self.logger.debug("Booting")
        self.boot_common()
        if self.is_fresh():
            self.logger.debug("Booting a fresh container")
            self.boot_basic()
            self.finish_fresh_boot()
        else:
            self.logger.debug("Recovering an existing container")
            # TODO
        self.write_state_file()

    def create_database(self):
        user = self.config.get_global_config().get_database()[Constants.PropertyConfDbUser]
        password = self.config.get_global_config().get_database()[Constants.PropertyConfDbPassword]
        dbname = self.config.get_global_config().get_database()[Constants.PropertyConfDbName]
        dbhost = self.config.get_global_config().get_database()[Constants.PropertyConfDbHost]
        self.db = ContainerDatabase(user, password, dbname, dbhost, self.logger)
        if self.is_fresh():
            self.db.set_reset_state(True)
        else:
            self.db.set_reset_state(False)
        self.db.initialize()

    def initialize(self, config: Configuration):
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
                    from fabric.actor.boot.configuration_loader import ConfigurationLoader
                    loader = ConfigurationLoader()
                    loader.process(self.config)
                except Exception as e:
                    traceback.print_exc()
                    self.logger.error("Failed to instantiate actors {}".format(e))
                    self.logger.error("This container may need to be restored to a clean state")
                    raise e
            else:
                # recovery
                self.logger.debug("TODO")

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
        self.logger.debug("Performing common boot tasks")
        self.define_protocols()
        self.plugin_manager.initialize(self.db)
        self.management_object_manager.initialize(self.db)
        from fabric.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
        RPCManagerSingleton.get().start()

    def define_protocols(self):
        self.logger.debug("Defining container protocols")
        desc = ProtocolDescriptor(Constants.ProtocolLocal, None)
        self.logger.debug("Registering protocol {}".format(Constants.ProtocolLocal))
        self.register_protocol(protocol=desc)

        kafka_topic_name = self.get_config().get_actor().get_kafka_topic()
        self.logger.debug("Kafka Topic {}".format(kafka_topic_name))
        if kafka_topic_name is not None:
            desc = ProtocolDescriptor(Constants.ProtocolKafka, kafka_topic_name)
            self.logger.debug("Registering protocol {}".format(Constants.ProtocolKafka))
            self.register_protocol(desc)

    def boot_basic(self):
        self.guid = self.get_config().get_global_config().get_container()[Constants.PropertyConfContainerGuid]
        self.logger.debug("Container guid is {}".format(self.guid))
        self.set_time()
        self.persist_basic()
        self.create_super_block()

    def persist_basic(self):
        self.persist_container()
        self.persist_time()

    def persist_container(self):
        # TODO
        return

    def persist_time(self):
        properties = {self.PropertyTime: self.PropertyTime,
                      self.PropertyBeginningOfTime: self.ticker.get_beginning_of_time(),
                      self.PropertyCycleMillis: self.ticker.get_cycle_millis(),
                      self.PropertyManualTicks: self.ticker.is_manual()}
        self.db.add_time(properties)

    def set_time(self):
        start_time = int(self.config.get_global_config().get_time()[Constants.PropertyConfTimeStartTime])
        if start_time == -1:
            start_time = ActorClock.get_current_milliseconds()

        cycle_millis = int(self.config.get_global_config().get_time()[Constants.PropertyConfTimeCycleMillis])

        manual = False
        if self.config.get_global_config().get_time()[Constants.PropertyConfTimeManual]:
            manual = True

        self.create_and_start_tick(start_time, cycle_millis, manual)

    def create_and_start_tick(self, start_time: int, cycle_millis: int, manual: bool):
        self.logger.debug("Creating container ticker")
        self.ticker = KernelTick()
        self.ticker.set_beginning_of_time(start_time)
        self.ticker.set_cycle_millis(cycle_millis)
        self.ticker.set_manual(manual)
        self.ticker.initialize()
        self.ticker.start()

        if not self.ticker.is_manual():
            self.logger.info("Using automatic ticks. Tick length={} ms".format(self.ticker.get_cycle_millis()))
        else:
            self.logger.info("Using manual ticks. Tick length={} ms".format(self.ticker.get_cycle_millis()))

    def finish_fresh_boot(self):
        self.create_container_manager_object()
        self.load_extensions()

    def finish_recovery_boot(self):
        return

    def create_container_manager_object(self):
        self.logger.info("Creating container manager object")
        from fabric.actor.core.manage.container_management_object import ContainerManagementObject
        management_object = ContainerManagementObject()
        self.management_object_manager.register_manager_object(management_object)

    def load_extensions(self):
        self.logger.info("Instantiating extensions...")
        # Load the core from the plugin directory.
        plugin_dir = self.config.get_global_config().get_runtime()[Constants.PropertyConfPluginDir]
        manager = PluginManagerSingleton().get()
        manager.setPluginPlaces([plugin_dir])
        manager.collectPlugins()

        # Loop round the core and print their names.
        for plugin in manager.getAllPlugins():
            plugin.plugin_object.print_name()

    def recover_basic(self):
        if self.is_fresh():
            raise Exception("A fresh container cannot be recovered")
        with self.container_lock:
            if self.state != ContainerState.Starting:
                raise Exception("Invalid state for recovery: {}".format(self.state))
            self.state = ContainerState.Recovering

        self.recover_guid()
        self.recover_time()

    def recover_guid(self):
        self.logger.debug("Recoverying container GUID")
        # TODO

    def recover_time(self):
        self.logger.debug("Recoverying container time settings")
        time_obj = self.db.get_time()
        if time_obj is None:
            raise Exception("Could not obtain container saved state from database")
        properties = time_obj['properties']
        beginning_of_time = properties[self.PropertyBeginningOfTime]
        cycle_millis = properties[self.PropertyCycleMillis]
        manual = properties[self.PropertyManualTicks]

        self.create_and_start_tick(beginning_of_time, cycle_millis, manual)

    def recover_actors(self):
        self.logger.info("Recovering actors")
        # TODO

    def recover_actor(self, properties: dict):
        self.logger.info("Recover actor")
        # TODO

    def register(self, tickable: ITick):
        self.ticker.add_tickable(tickable)

    def unregister(self, tickable: ITick):
        self.ticker.remove_tickable(tickable)

    def register_actor(self, actor: IActor):
        self.db.add_actor(actor)
        actor.actor_added()
        self.register_management_object(actor)
        self.register_common(actor)
        self.actor = actor
        actor.start()

    def unregister_actor(self, actor: IActor):
        self.management_object_manager.unload_actor_manager_objects(actor.get_name())
        ActorRegistrySingleton.get().unregister(actor)

    def register_common(self, actor: IActor):
        ActorRegistrySingleton.get().register_actor(actor)
        self.register_proxies(actor)

        RemoteActorCacheSingleton.get().register_with_registry(actor)

    def register_management_object(self, actor: IActor):
        module_name = actor.get_management_object_module()
        class_name = actor.get_management_object_class()
        mo = ReflectionUtils.create_instance(module_name, class_name)
        mo.set_actor(actor)
        mo.initialize()
        self.management_object_manager.register_manager_object(mo)

    def remove_actor(self, actor_name: str):
        self.db.remove_actor(actor_name)
        actor = ActorRegistrySingleton.get().get_actor(actor_name)
        if actor is not None:
            ActorRegistrySingleton.get().unregister(actor)
        actor.actor_removed()

    def remove_actor_database(self, actor_nam: str):
        self.db.remove_actor(actor_nam)

    def register_protocol(self, protocol: ProtocolDescriptor):
        self.protocols[protocol.get_protocol()] = protocol
        self.logger.debug("Registered container protocol: {}".format(protocol.get_protocol()))

    def register_proxies(self, actor: IActor):
        self.logger.debug("Registering proxies for actor: {}".format(actor.get_name()))

        for protocol in self.protocols.values():
            self.logger.debug("Processing protocol {}".format(protocol.get_protocol()))
            location = ActorLocation()
            location.set_descriptor(protocol)

            proxy = ProxyFactorySingleton.get().new_proxy(protocol.get_protocol(), actor.get_identity(), location)
            if proxy is not None:
                self.logger.debug(
                    "Registering proxy {} for protocol {}".format(actor.get_name(), protocol.get_protocol()))
                ActorRegistrySingleton.get().register_proxy(proxy)

            callback = ProxyFactorySingleton.get().new_callback(protocol.get_protocol(), actor.get_identity(), location)
            if callback is not None:
                self.logger.debug(
                    "Registering callback {} for protocol {}".format(actor.get_name(), protocol.get_protocol()))
                ActorRegistrySingleton.get().register_callback(callback)

    def register_recovered_actor(self, actor: IActor):
        self.logger.debug("Registering a recovered actor")
        actor.actor_added()
        self.register_common(actor)
        self.load_actor_management_objects(actor)
        actor.start()

    def load_actor_management_objects(self, actor: IActor):
        try:
            self.management_object_manager.load_actor_manager_objects(actor.get_name())
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
                self.unregister_actor(actor)

            ActorRegistrySingleton.get().clear()
            self.logger.info("Container is no longer active")
        except Exception as e:
            self.logger.error("Exception occurred while shutting down")
            traceback.print_exc()

    def write_state_file(self):
        try:
            file = open(Constants.StateFileLocation, 'r')
        except IOError:
            file = open(Constants.StateFileLocation, 'w')
        finally:
            file.close()

    def remove_state_file(self):
        os.remove(Constants.StateFileLocation)

    def get_actor_clock(self) -> ActorClock:
        if self.ticker is None:
            raise Exception("No tick")
        return ActorClock(self.ticker.get_beginning_of_time(), self.ticker.get_cycle_millis())

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

    def get_management_object(self, object_id: ID):
        if object_id is None:
            raise Exception("objectID cannot be null")
        self.management_object_manager.get_management_object(object_id)

    def get_plugin_manager(self) -> PluginManager:
        return self.plugin_manager

    def get_protocol_descriptor(self, protocol: str) -> ProtocolDescriptor:
        return self.protocols[protocol]

    @staticmethod
    def get_proxy(protocol: str, identity: IActorIdentity, location: ActorLocation, type: str = None):
        try:
            proxy = ActorRegistrySingleton.get().get_proxy(protocol, identity.get_name())
            if proxy is None:
                proxy = ProxyFactorySingleton.get().new_proxy(protocol, identity, location, type)
                ActorRegistrySingleton.get().register_proxy(proxy)
            return proxy
        except Exception as e:
            raise e

    def get_management_actor(self) -> IMgmtActor:
        return self.get_management_object(self.actor.get_guid())