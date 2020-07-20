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
from typing import TYPE_CHECKING

from fabric.actor.boot.inventory.PoolCreator import PoolCreator
from fabric.actor.core.apis.IAuthority import IAuthority
from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.common.ResourcePoolAttributeDescriptor import ResourcePoolAttributeDescriptor, \
    ResourcePoolAttributeType
from fabric.actor.core.common.ResourcePoolDescriptor import ResourcePoolDescriptor
from fabric.actor.core.container.RemoteActorCache import RemoteActorCacheSingleton, RemoteActorCache
from fabric.actor.core.core.Authority import Authority
from fabric.actor.core.core.Broker import Broker
from fabric.actor.core.core.Controller import Controller
from fabric.actor.core.delegation.SimpleResourceTicketFactory import SimpleResourceTicketFactory
from fabric.actor.core.kernel.SliceFactory import SliceFactory
from fabric.actor.core.manage.ManagementUtils import ManagementUtils
from fabric.actor.core.manage.messages.ClientMng import ClientMng
from fabric.actor.core.plugins.BasePlugin import BasePlugin
from fabric.actor.core.plugins.config.Config import Config
from fabric.actor.core.plugins.db.ServerActorDatabase import ServerActorDatabase
from fabric.actor.core.plugins.substrate.AuthoritySubstrate import AuthoritySubstrate
from fabric.actor.core.plugins.substrate.Substrate import Substrate
from fabric.actor.core.plugins.substrate.db.SubstrateActorDatabase import SubstrateActorDatabase
from fabric.actor.core.policy.AuthorityCalendarPolicy import AuthorityCalendarPolicy
from fabric.actor.core.policy.BrokerSimplerUnitsPolicy import BrokerSimplerUnitsPolicy
from fabric.actor.core.policy.ControllerSimplePolicy import ControllerSimplePolicy
from fabric.actor.core.proxies.kafka.Translate import Translate
from fabric.actor.core.util.ID import ID
from fabric.actor.core.util.ReflectionUtils import ReflectionUtils
from fabric.actor.core.util.ResourceType import ResourceType
from fabric.actor.core.core.Actor import Actor
from fabric.actor.security.AuthToken import AuthToken
from fabric.message_bus.messages.ClaimResourcesAvro import ClaimResourcesAvro
from fabric.message_bus.producer import AvroProducerApi

if TYPE_CHECKING:
    from fabric.actor.boot.Configuration import Configuration, ActorConfig, PolicyConfig, Peer
    from fabric.actor.core.apis.IActor import IActor
    from fabric.actor.core.apis.IMgmtActor import IMgmtActor


class ExportInfo:
    def __init__(self, exporter: IMgmtActor, client: ClientMng, units: int, rtype: ResourceType, topic: str):
        self.exporter = exporter
        self.client = client
        self.units = units
        self.rtype = rtype
        self.client_topic = topic

        self.exported = None
        self.start = None
        self.end = None


class ConfigurationProcessor:
    def __init__(self, config: Configuration):
        from fabric.actor.core.container.Globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.config = config
        self.actor = None
        self.to_export = []
        self.pools = {}

    def process(self):
        try:
            self.create_actor()
            self.initialize_actor()
            self.logger.info("There are {} actors".format(Actor.actor_count))
            self.register_actor()
            self.create_default_slice()
            self.populate_inventory()
            self.recover_actor()
            self.enable_ticking()
            self.process_topology()
            self.logger.info("Processing exports with actor count {}".format(Actor.actor_count))
            self.process_exports()
            self.logger.info("Processing exports completed")
            self.logger.info("Processing claims with actor count {}".format(Actor.actor_count))
            self.process_claims()
            self.logger.info("Processing claims completed")
        except Exception as e:
            raise Exception("Unexpected error while processing configuration {}".format(e))
        self.logger.info("Finished instantiating actors.")

    def create_actor(self):
        try:
            if self.config.get_actor() is not None:
                self.logger.debug("Creating Actor: name={}".format(self.config.get_actor().get_name()))
                self.actor = self.do_common(self.config.get_actor())
                self.do_specific(self.actor, self.config.get_actor())
        except Exception as e:
            raise Exception("Unexpected error while creating actor {}".format(e))

    def do_common(self, actor_config: ActorConfig):
        actor = self.make_actor_instance(actor_config)
        actor.set_plugin(self.make_plugin_instance(actor, actor_config))
        actor.set_policy(self.make_actor_policy(actor, actor_config))
        return actor

    def make_actor_instance(self, actor_config: ActorConfig) -> IActor:
        actor_type = actor_config.get_type()
        actor = None
        if actor_type == Constants.CONTROLLER:
            actor = Controller()
        elif actor_type == Constants.BROKER:
            actor = Broker()
        elif actor_type == Constants.AUTHORITY:
            actor = Authority()
        else:
            raise Exception("Unsupported actor type: {}".format(actor_type))

        if actor is not None:
            actor_guid = ID()
            if actor_config.get_guid() is not None:
                actor_guid = ID(actor_config.get_guid())
            auth_token = AuthToken(actor_config.get_name(), actor_guid)
            actor.set_identity(auth_token)
            if actor_config.get_description() is not None:
                actor.set_description(actor_config.get_description())

            from fabric.actor.core.container.Globals import GlobalsSingleton
            actor.set_actor_clock(GlobalsSingleton.get().get_container().get_actor_clock())

        return actor

    def make_plugin_instance(self, actor: IActor, actor_config: ActorConfig):
        plugin = None
        if actor.get_plugin() is None:
            if actor.get_type() == Constants.ActorTypeSiteAuthority:
                # TODO replacement of ANT CONFIG
                plugin = AuthoritySubstrate(actor, None, Config())

            elif actor.get_type() == Constants.ActorTypeController:
                # TODO replacement of ANT CONFIG
                plugin = Substrate(actor, None, Config())

            elif actor.get_type() == Constants.ActorTypeBroker:
                plugin = BasePlugin(actor, None, Config())

        if plugin is None:
            raise Exception("Cannot instantiate shirako plugin for actor: {}".format(actor_config.get_name()))

        if plugin.get_database() is None:
            db = None
            user = self.config.get_global_config().get_database()[Constants.PropertyConfDbUser]
            password = self.config.get_global_config().get_database()[Constants.PropertyConfDbPassword]
            db_host = self.config.get_global_config().get_database()[Constants.PropertyConfDbHost]
            db_name = self.config.get_global_config().get_database()[Constants.PropertyConfDbName]
            if isinstance(plugin, Substrate):
                db = SubstrateActorDatabase(user, password, db_name, db_host, self.logger)
            else:
                db = ServerActorDatabase(user, password, db_name, db_host, self.logger)

            plugin.set_database(db)

        ticket_factory = SimpleResourceTicketFactory()
        ticket_factory.set_actor(actor)
        plugin.set_ticket_factory(ticket_factory)
        return plugin

    def make_actor_policy(self, actor: IActor, config: ActorConfig):
        policy = None
        if config.get_policy() is None:
            if actor.get_type() == Constants.ActorTypeSiteAuthority:
                policy = self.make_site_policy(config)
            elif actor.get_type() == Constants.ActorTypeBroker:
                policy = BrokerSimplerUnitsPolicy()
            elif actor.get_type() == Constants.ActorTypeController:
                policy = ControllerSimplePolicy()
        else:
            policy = self.make_policy(config.get_policy())

        if policy is None:
            raise Exception("Could not instantiate policy for actor: {}".format(config.get_name()))
        return policy

    def make_site_policy(self, config: ActorConfig):
        if config.get_controls() is None or len(config.get_controls()) == 0:
            raise Exception("Missing authority policy but no control has been specified")

        policy = AuthorityCalendarPolicy()
        for c in config.get_controls():
            try:
                if c.get_module_name() is None or c.get_class_name() is None:
                    raise Exception("Missing control class name")

                control = ReflectionUtils.create_instance(c.get_module_name(), c.get_class_name())

                if c.get_type() is None:
                    raise Exception("No type specified for control")

                control.add_type(ResourceType(c.get_type()))
                policy.register_control(control)
            except Exception as e:
                raise Exception("Could not create control {}".format(e))
        return policy

    def make_policy(self, policy: PolicyConfig):
        if policy.get_class_name() is None or policy.get_module_name() is None:
            raise Exception("Policy is missing class name")

        return ReflectionUtils.create_instance(policy.get_module_name(), policy.get_class_name())

    def do_specific(self, actor: IActor, config: ActorConfig):
        if isinstance(actor, IAuthority):
            rd = self.read_resource_pools(config)
            self.pools[actor.get_guid()] = rd

    def read_resource_pools(self, config: ActorConfig) -> dict:
        result = {}
        pools = config.get_pools()
        if pools is None or len(pools) == 0:
            return result

        for p in pools:
            descriptor = ResourcePoolDescriptor()
            descriptor.set_resource_type(ResourceType(p.get_type()))
            descriptor.set_resource_type_label(p.get_label())
            descriptor.set_units(p.get_units())
            descriptor.set_start(p.get_start())
            descriptor.set_end(p.get_end())
            descriptor.set_pool_factory_class(p.get_factory_class())
            descriptor.set_pool_factory_module(p.get_factory_module())
            handler = p.get_handler()
            if handler is not None:
                descriptor.set_handler_class(handler.get_class_name())
                descriptor.set_handler_module(handler.get_module_name())
                descriptor.set_handler_properties(handler.get_properties())

            descriptor.pool_properties = p.get_properties()

            for attr in p.get_attributes():
                attribute = ResourcePoolAttributeDescriptor()
                attribute.set_key(attr.get_key())
                attribute.set_value(attr.get_value())
                if attr.get_type().lower() == "integer":
                    attribute.set_type(ResourcePoolAttributeType.INTEGER)
                elif attr.get_type().lower() == "string":
                    attribute.set_type(ResourcePoolAttributeType.STRING)
                elif attr.get_type().lower() == "neo4j":
                    attribute.set_type(ResourcePoolAttributeType.NEO4J)
                elif attr.get_type().lower() == "class":
                    attribute.set_type(ResourcePoolAttributeType.CLASS)
                else:
                    raise Exception("Unsupported attribute type: {}".format(attr.get_type()))
                descriptor.add_attribute(attribute)

            result[descriptor.get_resource_type()] = descriptor
        return result

    def initialize_actor(self):
        try:
            Actor.actor_count += 1
            self.actor.initialize()
        except Exception as e:
            raise Exception("Actor failed to initialize: {} {}".format(self.actor.get_name(), e))

    def register_actor(self):
        try:
            from fabric.actor.core.container.Globals import GlobalsSingleton
            GlobalsSingleton.get().get_container().register_actor(self.actor)
        except Exception as e:
            raise Exception("Could not register actor: {} {}".format(self.actor.get_name(), e))

    def create_default_slice(self):
        if self.actor.get_type() != Constants.ActorTypeSiteAuthority:
            slice_obj = SliceFactory.create(slice_id=ID(), name=self.actor.get_name())
            slice_obj.set_inventory(True)
            try:
                self.actor.register_slice(slice_obj)
            except Exception as e:
                Exception("Could not create default slice for actor: {} {}".format(self.actor.get_name(), e))

    def populate_inventory(self):
        if isinstance(self.actor, IAuthority):
            if isinstance(self.actor.get_plugin(), AuthoritySubstrate):
                descriptor = self.pools[self.actor.get_guid()]
                creator = PoolCreator(self.actor.get_plugin(), descriptor)
                creator.process()

    def recover_actor(self):
        try:
            self.actor.recover()
        except Exception as e:
            raise Exception("Recovery failed for actor: {}".format(self.actor.get_name()))

    def enable_ticking(self):
        from fabric.actor.core.container.Globals import GlobalsSingleton
        GlobalsSingleton.get().get_container().register(self.actor)

    def process_topology(self):
        if self.config.get_peers() is None or len(self.config.get_peers()) == 0:
            self.logger.debug("No peers specified")
        else:
            self.create_proxies(self.config.get_peers())

    def process_exports(self):
        for ei in self.to_export:
            self.export(ei)

    def process_claims(self):
        self.logger.debug("process_claims {}".format(len(self.to_export)))
        for ei in self.to_export:
            self.claim(ei)

    def create_proxies(self, peers: list):
        for e in peers:
            self.process_peer(e)

    def vertex_to_registry_cache(self, peer: Peer):
        self.logger.debug("Adding vertex for {}".format(peer.get_name()))

        if peer.get_name() is None:
            raise Exception("Actor must specify a name")

        if peer.get_guid() is None:
            raise Exception("Actor must specify a guid")

        protocol = Constants.ProtocolLocal
        kafka_topic = None
        if peer.get_kafka_topic() is not None:
            protocol = Constants.ProtocolKafka
            kafka_topic = peer.get_kafka_topic()

        actor_type = peer.get_type().lower()

        entry = {
            RemoteActorCache.ActorName: peer.get_name(),
            RemoteActorCache.ActorGuid: ID(peer.get_guid()),
            RemoteActorCache.ActorType: actor_type,
            RemoteActorCache.ActorProtocol: protocol
        }
        if kafka_topic is not None:
            entry[RemoteActorCache.ActorLocation] = kafka_topic

        RemoteActorCacheSingleton.get().add_partial_cache_entry(ID(peer.get_guid()), entry)

    def process_peer(self, peer: Peer):
        from_guid = ID(peer.get_guid())
        from_type = Actor.get_actor_type_from_string(peer.get_type())
        to_guid = self.actor.get_guid()
        to_type = self.actor.get_type()

        # We only like peers broker->site and service->broker
        # Reverse the peer if it connects site->broker or broker->service

        if from_type == Constants.ActorTypeSiteAuthority and to_type == Constants.ActorTypeBroker:
            temp = from_guid
            from_guid = to_guid
            to_guid = temp
            temp = from_type
            from_type = to_type
            to_type = temp

        if from_type == Constants.BROKER and to_type == Constants.ActorTypeController:
            temp = from_guid
            from_guid = to_guid
            to_guid = temp
            temp = from_type
            from_type = to_type
            to_type = temp

        # Check if this is a valid peer (error if authority -> * or controller -> authority).
        if from_type == Constants.ActorTypeSiteAuthority:
            raise Exception("Invalid peer type: controller can only talk to broker or site authority")

        # peers between actors of same type aren't allowed unless the actors are both brokers
        if from_type == to_type and from_type != Constants.ActorTypeBroker:
            raise Exception("Invalid peer type: broker can only talk to broker, controller or site authority")

        # peers controller->site or vice versa are not allowed
        #if from_type == Constants.ActorTypeController and to_type == Constants.ActorTypeSiteAuthority:
        #    raise Exception("Invalid peer type: Peers between Controller and sites are not allowed")

        if to_type == Constants.ActorTypeController and from_type == Constants.ActorTypeSiteAuthority:
            raise Exception("Invalid peer type: Peers between Controller and sites are not allowed")

        container = ManagementUtils.connect(self.actor.get_identity())
        to_mgmt_actor = container.get_actor(to_guid)
        from_mgmt_actor = container.get_actor(from_guid)

        self.vertex_to_registry_cache(peer)

        try:
            client = RemoteActorCacheSingleton.get().establish_peer(from_guid, from_mgmt_actor, to_guid, to_mgmt_actor)
            self.logger.debug("Client returned {}".format(client))
            if client is not None:
                self.parse_exports(peer, client, to_mgmt_actor)
        except Exception as e:
            Exception("Could not process exports from: {} to {}. e= {}" .format(peer.get_guid(), self.actor.get_guid(), e))

    def parse_exports(self, peer: Peer, client: ClientMng, mgmt_actor: IMgmtActor):
        for rset in peer.get_rsets():
            info = ExportInfo(mgmt_actor, client, rset.get_units(), ResourceType(rset.get_type()), peer.get_kafka_topic())
            if rset.get_start() is not None:
                info.start = rset.get_start()
            if rset.get_end() is not None:
                info.end = rset.get_end()

            self.to_export.append(info)

    def export(self, info: ExportInfo):
        from fabric.actor.core.container.Globals import GlobalsSingleton
        now = GlobalsSingleton.get().get_container().get_current_cycle()
        start = info.start
        if start is None:
            start = GlobalsSingleton.get().get_container().cycle_start_date(now)
        end = info.end
        if end is None:
            # export for one year
            length = 1000 * 60 * 60 * 24 * 365
            end = GlobalsSingleton.get().get_container().cycle_end_date(now + length)

        self.logger.debug("Using Server Actor {} to export resources".format(info.exporter.__class__.__name__))

        info.exported = info.exporter.export_resources(info.rtype, start, end, info.units, None, None, None,
                                                       AuthToken(info.client.get_name(), ID(info.client.get_guid())))

        if info.exported is None:
            raise Exception("Could not export resources from actor: {} to actor: {} Error = {}".
                            format(info.exporter.get_name(), info.client.get_name(), info.exporter.get_last_error()))

    # TODO needs to be fixed
    def claim(self, info: ExportInfo):
        if info.exported is None:
            self.logger.error("No reservation to export from {} to {}".format(info.exporter.get_name(), info.client.get_name()))
            return

        self.logger.debug("Claiming resources from {} to {}".format(info.exporter.get_name(), info.client.get_name()))

        container = ManagementUtils.connect(self.actor.get_identity())
        client_mgmt_actor = container.get_actor(ID(info.client.get_guid()))

        if client_mgmt_actor is None:
            self.logger.info("{} is a remote client. Not performing claim".format(info.client.get_name()))
            #self.trigger_remote_claim(info)
            return

        self.logger.info("Claiming resources from {} to {}".format(info.exporter.get_name(), info.client.get_name()))

        reservation = client_mgmt_actor.claim_resources(info.exporter.get_guid(), info.exported)

        if reservation is not None:
            self.logger.info("Successfully initiated claim for resources from {} to {}".format(info.exporter.get_name(),
                                                                                               info.client.get_name()))
        else:
            self.logger.error("Could not initiate claim for resources from {} to {}".format(info.exporter.get_name(),
                                                                                            info.client.get_name()))

    def trigger_remote_claim(self, info: ExportInfo):
        try:
            claim_req = ClaimResourcesAvro()
            claim_req.guid = info.client.guid
            claim_req.auth = Translate.translate_auth_to_avro(self.actor.get_identity())
            claim_req.broker_id = str(info.exporter.get_guid())
            claim_req.reservation_id = str(info.exported)
            claim_req.message_id = "test_claim_1"
            claim_req.callback_topic = self.config.actor.get_kafka_topic()
            claim_req.slice_id = "null"

            bootstrap_server = self.config.get_global_config().get_runtime()[Constants.PropertyConfKafkaServer]
            schema_registry = self.config.get_global_config().get_runtime()[Constants.PropertyConfKafkaSchemaRegistry]
            key_schema_file = self.config.get_global_config().get_runtime()[Constants.PropertyConfKafkaKeySchema]
            value_schema_file = self.config.get_global_config().get_runtime()[Constants.PropertyConfKafkaValueSchema]

            from confluent_kafka import avro
            conf = {'bootstrap.servers': bootstrap_server,
                    'schema.registry.url': schema_registry}

            file = open(key_schema_file, "r")
            kbytes = file.read()
            file.close()
            key_schema = avro.loads(kbytes)
            file = open(value_schema_file, "r")
            vbytes = file.read()
            file.close()
            val_schema = avro.loads(vbytes)

            # create a producer
            producer = AvroProducerApi(conf, key_schema, val_schema, self.logger)
            if producer.produce_sync(info.client_topic, claim_req):
                self.logger.debug("Message {} written to {}".format(claim_req.name, info.client_topic))
            else:
                self.logger.error("Failed to send message {} to {}".format(claim_req.name, info.client_topic))
        except Exception as e:
            traceback.print_exc()
            self.logger.error(e)
            print(e)