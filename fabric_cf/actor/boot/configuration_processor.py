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

import traceback
from typing import TYPE_CHECKING, List, Dict

from fabric_cf.actor.boot.configuration_exception import ConfigurationException
from fabric_cf.actor.boot.inventory.aggregate_resource_model_creator import AggregateResourceModelCreator
from fabric_cf.actor.core.apis.abc_authority import ABCAuthority
from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.resource_config import ResourceConfig
from fabric_cf.actor.core.container.remote_actor_cache import RemoteActorCacheSingleton, RemoteActorCache
from fabric_cf.actor.core.core.authority import Authority
from fabric_cf.actor.core.core.broker import Broker
from fabric_cf.actor.core.core.controller import Controller
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.manage.management_utils import ManagementUtils
from fabric_cf.actor.core.manage.messages.client_mng import ClientMng
from fabric_cf.actor.core.plugins.base_plugin import BasePlugin
from fabric_cf.actor.core.plugins.handlers.ansible_handler_processor import AnsibleHandlerProcessor
from fabric_cf.actor.core.plugins.handlers.handler_processor import HandlerProcessor
from fabric_cf.actor.core.plugins.db.server_actor_database import ServerActorDatabase
from fabric_cf.actor.core.plugins.substrate.authority_substrate import AuthoritySubstrate
from fabric_cf.actor.core.plugins.substrate.substrate_mixin import SubstrateMixin
from fabric_cf.actor.core.plugins.substrate.db.substrate_actor_database import SubstrateActorDatabase
from fabric_cf.actor.core.policy.authority_calendar_policy import AuthorityCalendarPolicy
from fabric_cf.actor.core.policy.broker_simpler_units_policy import BrokerSimplerUnitsPolicy
from fabric_cf.actor.core.policy.controller_ticket_review_policy import ControllerTicketReviewPolicy
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reflection_utils import ReflectionUtils
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.core.core.actor import ActorMixin
from fabric_cf.actor.security.auth_token import AuthToken
from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType

if TYPE_CHECKING:
    from fabric_cf.actor.boot.configuration import Configuration, ActorConfig, PolicyConfig, Peer
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_mgmt_actor import ABCMgmtActor


class ExportAdvertisement:
    """
    Encapsulates information needed for the delegations to be exported which are later claimed by broker
    """
    def __init__(self, *, exporter: ABCMgmtActor, client: ClientMng, delegation: str, topic: str):
        self.exporter = exporter
        self.client = client
        self.delegation = delegation
        self.client_topic = topic
        self.exported = None


class ConfigurationProcessor:
    """
    Processing the Configuration loaded and brings up the required actors, peers and various threads
    """
    def __init__(self, *, config: Configuration):
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.config = config
        self.actor = None
        self.to_export = []
        self.to_advertise = []
        self.resources = {}
        self.controls = None
        self.aggregate_delegation_models = None

    def process(self):
        """
        Instantiates the configuration
        @raises ConfigurationException in case of error
        """
        try:
            self.create_actor()
            self.initialize_actor()
            self.logger.info(f"There are {ActorMixin.actor_count} actors")
            self.register_actor()
            self.create_default_slice()
            self.populate_inventory_neo4j()
            self.recover_actor()
            self.enable_ticking()
            self.process_topology()
            self.logger.info(f"Processing exports with actor count {ActorMixin.actor_count}")
            self.process_advertise()
            self.logger.info("Processing exports completed")
        except Exception as e:
            self.logger.error(traceback.format_exc())
            raise ConfigurationException(f"Unexpected error while processing configuration {e}")
        self.logger.info("Finished instantiating actors.")
        print("End process")

    def create_actor(self):
        """
        Instantiates all actors
        @raises ConfigurationException in case of error
        """
        try:
            if self.config.get_actor() is not None:
                self.logger.debug(f"Creating Actor: name={self.config.get_actor().get_name()}")
                self.actor = self.do_common(actor_config=self.config.get_actor())
                self.do_specific(actor=self.actor, config=self.config.get_actor())
        except Exception as e:
            raise ConfigurationException(f"Unexpected error while creating actor {e}")

    def do_common(self, *, actor_config: ActorConfig):
        """
        Perform the common operations for actor creation
        @param actor_config actor config
        @raises ConfigurationException in case of error
        """
        actor = self.make_actor_instance(actor_config=actor_config)
        actor.set_plugin(plugin=self.make_plugin_instance(actor=actor, actor_config=actor_config))
        actor.set_policy(policy=self.make_actor_policy(actor=actor, config=actor_config))
        return actor

    @staticmethod
    def make_actor_instance(*, actor_config: ActorConfig) -> ABCActorMixin:
        """
        Creates Actor instance
        @param actor_config actor config
        @raises ConfigurationException in case of error
        """
        actor_type = ActorType.get_actor_type_from_string(actor_type=actor_config.get_type())
        actor = None
        if actor_type == ActorType.Orchestrator:
            actor = Controller()
        elif actor_type == ActorType.Broker:
            actor = Broker()
        elif actor_type == ActorType.Authority:
            actor = Authority()
        else:
            raise ConfigurationException(f"Unsupported actor type: {actor_type}")

        actor_guid = ID()
        if actor_config.get_guid() is not None:
            actor_guid = ID(uid=actor_config.get_guid())
        auth_token = AuthToken(name=actor_config.get_name(), guid=actor_guid)
        actor.set_identity(token=auth_token)
        if actor_config.get_description() is not None:
            actor.set_description(description=actor_config.get_description())

        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        actor.set_actor_clock(clock=GlobalsSingleton.get().get_container().get_actor_clock())

        return actor

    def make_plugin_instance(self, *, actor: ABCActorMixin, actor_config: ActorConfig):
        """
        Creates Plugin instance for the Actor
        @param actor actor
        @param actor_config actor config
        @raises ConfigurationException in case of error
        """
        plugin = None
        if actor.get_plugin() is None:
            if actor.get_type() == ActorType.Authority:
                plugin = AuthoritySubstrate(actor=actor, db=None, handler_processor=AnsibleHandlerProcessor())

            elif actor.get_type() == ActorType.Orchestrator:
                plugin = SubstrateMixin(actor=actor, db=None, handler_processor=HandlerProcessor())

            elif actor.get_type() == ActorType.Broker:
                plugin = BasePlugin(actor=actor, db=None, handler_processor=HandlerProcessor())

        if plugin is None:
            raise ConfigurationException(f"Cannot instantiate plugin for actor: {actor_config.get_name()}")

        if plugin.get_database() is None:
            db = None
            user = self.config.get_global_config().get_database()[Constants.PROPERTY_CONF_DB_USER]
            password = self.config.get_global_config().get_database()[Constants.PROPERTY_CONF_DB_PASSWORD]
            db_host = self.config.get_global_config().get_database()[Constants.PROPERTY_CONF_DB_HOST]
            db_name = self.config.get_global_config().get_database()[Constants.PROPERTY_CONF_DB_NAME]
            if isinstance(plugin, SubstrateMixin):
                db = SubstrateActorDatabase(user=user, password=password, database=db_name, db_host=db_host,
                                            logger=self.logger)
            else:
                db = ServerActorDatabase(user=user, password=password, database=db_name, db_host=db_host,
                                         logger=self.logger)

            plugin.set_database(db=db)
        return plugin

    def make_actor_policy(self, *, actor: ABCActorMixin, config: ActorConfig):
        """
        Creates Actor Policy instance
        @param actor actor
        @param actor_config actor config
        @raises ConfigurationException in case of error
        """
        policy = None
        if actor.get_type() == ActorType.Authority:
            policy = self.make_site_policy(config=config)
        elif actor.get_type() == ActorType.Broker:
            policy = self.make_broker_policy(config=config)
        elif actor.get_type() == ActorType.Orchestrator:
            if config.get_policy() is not None:
                policy = self.make_policy(policy=config.get_policy())
            else:
                policy = ControllerTicketReviewPolicy()

        if policy is None:
            raise ConfigurationException(f"Could not instantiate policy for actor: {config.get_name()}")
        return policy

    def make_broker_policy(self, *, config: ActorConfig):
        """
        Creates AM Policy instance and set up controls
        @param config actor config
        @raises ConfigurationException in case of error
        """
        policy = None
        if config.get_policy() is not None:
            policy = self.make_policy(policy=config.get_policy())
            properties = config.get_policy().get_properties()
            policy.set_properties(properties=properties)
        else:
            policy = BrokerSimplerUnitsPolicy()

        for i in config.get_controls():
            try:
                if i.get_module_name() is None or i.get_class_name() is None:
                    raise ConfigurationException("Missing inventory class name")

                inventory = ReflectionUtils.create_instance(module_name=i.get_module_name(),
                                                            class_name=i.get_class_name())
                inventory.set_logger(logger=self.logger)

                if i.get_type() is None:
                    raise ConfigurationException("No type specified for control")

                for t in i.get_type():
                    policy.inventory.add_inventory_by_type(rtype=ResourceType(resource_type=t), inventory=inventory)
            except Exception as e:
                self.logger.error(traceback.format_exc())
                raise ConfigurationException("Could not create control {}".format(e))
        return policy

    def make_site_policy(self, *, config: ActorConfig):
        """
        Creates AM Policy instance and set up controls
        @param config actor config
        @raises ConfigurationException in case of error
        """
        policy = None
        if config.get_policy() is not None:
            policy = self.make_policy(policy=config.get_policy())
        else:
            policy = AuthorityCalendarPolicy()
        for c in config.get_controls():
            try:
                if c.get_module_name() is None or c.get_class_name() is None:
                    raise ConfigurationException("Missing control class name")

                control = ReflectionUtils.create_instance(module_name=c.get_module_name(),
                                                          class_name=c.get_class_name())
                control.set_actor(actor=self.actor)

                if c.get_type() is None:
                    raise ConfigurationException("No type specified for control")

                for t in c.get_type():
                    control.add_type(rtype=ResourceType(resource_type=t))
                policy.register_control(control=control)
            except Exception as e:
                self.logger.error(traceback.format_exc())
                raise ConfigurationException("Could not create control {}".format(e))
        return policy

    @staticmethod
    def make_policy(*, policy: PolicyConfig) -> ABCPolicy:
        """
        Creates Policy Instance
        @param policy policy config
        @raises ConfigurationException in case of error
        """
        if policy.get_class_name() is None or policy.get_module_name() is None:
            raise ConfigurationException("Policy is missing class name")

        return ReflectionUtils.create_instance(module_name=policy.get_module_name(),
                                               class_name=policy.get_class_name())

    def do_specific(self, *, actor: ABCActorMixin, config: ActorConfig):
        """
        Do Actor specify initialization
        @param actor actor
        @param config actor config
        @raises ConfigurationException in case of error
        """
        if isinstance(actor, ABCAuthority):
            self.resources = self.read_resource_config(config=config)

    @staticmethod
    def read_resource_config(*, config: ActorConfig) -> Dict[ResourceType, ResourceConfig]:
        """
        Read resource config and create ARM and inventory slices
        @param config actor config
        @raises ConfigurationException in case of error
        """
        result = {}
        resources = config.get_resources()
        if resources is None or len(resources) == 0:
            return result

        for r in resources:
            for resource_type in r.get_type():
                descriptor = ResourceConfig()
                descriptor.set_resource_type(rtype=ResourceType(resource_type=resource_type))
                descriptor.set_resource_type_label(rtype_label=r.get_label())

                handler = r.get_handler()
                if handler is not None:
                    descriptor.set_handler_class(handler_class=handler.get_class_name())
                    descriptor.set_handler_module(module=handler.get_module_name())
                    descriptor.set_handler_properties(properties=handler.get_properties())

                result[descriptor.get_resource_type()] = descriptor
        return result

    def initialize_actor(self):
        """
        Initialize actor
        @raises ConfigurationException in case of error
        """
        try:
            ActorMixin.actor_count += 1
            self.actor.initialize()
        except Exception as e:
            raise ConfigurationException(f"Actor failed to initialize: {self.actor.get_name()} {e}")

    def register_actor(self):
        """
        Register actor
        @raises ConfigurationException in case of error
        """
        try:
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            GlobalsSingleton.get().get_container().register_actor(actor=self.actor)
        except Exception as e:
            raise ConfigurationException(f"Could not register actor: {self.actor.get_name()} {e}")

    def create_default_slice(self):
        """
        Create default slice
        @raises ConfigurationException in case of error
        """
        if self.actor.get_type() != ActorType.Authority:
            slice_obj = SliceFactory.create(slice_id=ID(), name=self.actor.get_name())
            slice_obj.set_inventory(value=True)
            try:
                self.actor.register_slice(slice_object=slice_obj)
            except Exception as e:
                self.logger.error(traceback.format_exc())
                raise ConfigurationException(f"Could not create default slice for actor: {self.actor.get_name()} {e}")

    def populate_inventory_neo4j(self):
        """
        Load Aggregate Resource model
        @raises ConfigurationException in case of error
        """
        if isinstance(self.actor, ABCAuthority) and isinstance(self.actor.get_plugin(), AuthoritySubstrate):
            creator = AggregateResourceModelCreator(substrate=self.actor.get_plugin(), resources=self.resources,
                                                    neo4j_config=self.config.get_global_config().get_neo4j_config())
            self.aggregate_delegation_models = creator.process_neo4j(actor_name=self.actor.get_name(),
                                                                     substrate_file=self.config.get_actor().
                                                                     get_substrate_file())
            self.actor.set_aggregate_resource_model(aggregate_resource_model=creator.get_arm_graph())

    def recover_actor(self):
        """
        Recover an actor on stateful restart
        @raises ConfigurationException in case of error
        """
        try:
            self.actor.recover()
        except Exception as e:
            raise ConfigurationException(f"Recovery failed for actor: {self.actor.get_name()} e: {e}")

    def enable_ticking(self):
        """
        Enable the Actor tick
        """
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        GlobalsSingleton.get().get_container().register(tickable=self.actor)

    def process_topology(self):
        """
        Set up Peer registry with identity and kafka topic information
        @raises ConfigurationException in case of error
        """
        if self.config.get_peers() is None or len(self.config.get_peers()) == 0:
            self.logger.debug("No peers specified")
        else:
            self.create_proxies(peers=self.config.get_peers())

    def process_advertise(self):
        """
        Set up Aggregate Delegation models for the peers
        @raises ConfigurationException in case of error
        """
        if self.aggregate_delegation_models is None and len(self.to_advertise) > 0:
            raise ConfigurationException("No delegations found in Aggregate Resource Model")

        for ei in self.to_advertise:
            self.advertise(info=ei)

    def create_proxies(self, *, peers: List[Peer]):
        """
        Set up proxies
        @param peers List of the Peer config
        @raises ConfigurationException in case of error
        """
        for e in peers:
            self.process_peer(peer=e)

    def vertex_to_registry_cache(self, *, peer: Peer):
        """
        Add peer to Registry Cache
        @param peer peer config
        @raises ConfigurationException in case of error
        """
        self.logger.debug("Adding vertex for {}".format(peer.get_name()))

        if peer.get_name() is None:
            raise ConfigurationException("Actor must specify a name")

        if peer.get_guid() is None:
            raise ConfigurationException("Actor must specify a guid")

        protocol = Constants.PROTOCOL_LOCAL
        kafka_topic = None
        if peer.get_kafka_topic() is not None:
            protocol = Constants.PROTOCOL_KAFKA
            kafka_topic = peer.get_kafka_topic()

        actor_type = peer.get_type().lower()

        entry = {
            RemoteActorCache.actor_name: peer.get_name(),
            RemoteActorCache.actor_guid: ID(uid=peer.get_guid()),
            RemoteActorCache.actor_type: actor_type,
            RemoteActorCache.actor_protocol: protocol
        }
        if kafka_topic is not None:
            entry[RemoteActorCache.actor_location] = kafka_topic

        RemoteActorCacheSingleton.get().add_partial_cache_entry(guid=ID(uid=peer.get_guid()), entry=entry)

    def process_peer(self, *, peer: Peer):
        """
        Process a peer
        @param peer peer
        @raises ConfigurationException in case of error
        """
        from_guid = ID(uid=peer.get_guid())
        from_type = ActorType.get_actor_type_from_string(actor_type=peer.get_type())
        to_guid = self.actor.get_guid()
        to_type = self.actor.get_type()

        # We only like peers broker->site and orchestrator->broker
        # Reverse the peer if it connects site->broker or broker->orchestrator

        if from_type == ActorType.Authority and to_type == ActorType.Broker:
            from_guid, to_guid = to_guid, from_guid
            from_type, to_type = to_type, from_type

        if from_type == ActorType.Broker and to_type == ActorType.Orchestrator:
            from_guid, to_guid = to_guid, from_guid
            from_type, to_type = to_type, from_type

        if from_type == ActorType.Authority and to_type == ActorType.Orchestrator:
            from_guid, to_guid = to_guid, from_guid
            from_type, to_type = to_type, from_type

        # peers between actors of same type aren't allowed unless the actors are both brokers
        if from_type == to_type and from_type != ActorType.Broker:
            raise ConfigurationException(
                "Invalid peer type: broker can only talk to broker, orchestrator or site authority")

        container = ManagementUtils.connect(caller=self.actor.get_identity())
        to_mgmt_actor = container.get_actor(guid=to_guid)
        self.logger.debug(f"to_mgmt_actor={to_mgmt_actor} to_guid={to_guid}")
        if to_mgmt_actor is None and container.get_last_error() is not None:
            self.logger.error(container.get_last_error())
        from_mgmt_actor = container.get_actor(guid=from_guid)
        self.logger.debug(f"from_mgmt_actor={from_mgmt_actor} from_guid={from_guid}")
        if from_mgmt_actor is None and container.get_last_error() is not None:
                self.logger.error(container.get_last_error())

        self.vertex_to_registry_cache(peer=peer)

        try:
            client = RemoteActorCacheSingleton.get().establish_peer(from_guid=from_guid,
                                                                    from_mgmt_actor=from_mgmt_actor,
                                                                    to_guid=to_guid, to_mgmt_actor=to_mgmt_actor)
            self.logger.debug(f"Client returned {client}")
            if client is not None:
                self.parse_exports(peer=peer, client=client, mgmt_actor=to_mgmt_actor)
        except Exception as e:
            raise ConfigurationException(f"Could not process exports from: {peer.get_guid()} to "
                                         f"{self.actor.get_guid()}. e= {e}")

    def parse_exports(self, *, peer: Peer, client: ClientMng, mgmt_actor: ABCMgmtActor):
        """
        Identify all the delegations to be advertised
        @param peer peer config
        @param client client
        @param mgmt_actor management actor
        """
        if peer.get_delegation() is not None:
            info = ExportAdvertisement(exporter=mgmt_actor, client=client,
                                       delegation=peer.get_delegation(),
                                       topic=peer.get_kafka_topic())
            self.to_advertise.append(info)

    def advertise(self, *, info: ExportAdvertisement):
        """
        Advertise a delegation
        @param info advertisement information
        @raises ConfigurationException in case of error
        """
        self.logger.debug(f"Using Server Actor {info.exporter.__class__.__name__} to export resources")

        delegation = self.aggregate_delegation_models.get(info.delegation, None)

        if delegation is None:
            raise ConfigurationException(f"Aggregate Delegation Model not found for delegation: {info.delegation}")

        client = AuthToken(name=info.client.get_name(),
                           guid=ID(uid=info.client.get_guid()))
        info.exported = info.exporter.advertise_resources(delegation=delegation, delegation_name=info.delegation,
                                                          client=client)

        if info.exported is None:
            raise ConfigurationException(f"Could not export resources from actor: {info.exporter.get_name()} to actor:"
                                         f" {info.client.get_name()} Error = {info.exporter.get_last_error()}")
