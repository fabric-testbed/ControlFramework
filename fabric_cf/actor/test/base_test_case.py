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
import logging

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin, ActorType
from fabric_cf.actor.core.apis.abc_authority import ABCAuthority
from fabric_cf.actor.core.apis.abc_authority_policy import ABCAuthorityPolicy
from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin
from fabric_cf.actor.core.apis.abc_broker_mixin import ABCBrokerMixin
from fabric_cf.actor.core.apis.abc_broker_policy_mixin import ABCBrokerPolicyMixin
from fabric_cf.actor.core.apis.abc_container_database import ABCContainerDatabase
from fabric_cf.actor.core.apis.abc_controller import ABCController
from fabric_cf.actor.core.apis.abc_controller_policy import ABCControllerPolicy
from fabric_cf.actor.core.apis.abc_database import ABCDatabase
from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
from fabric_cf.actor.core.core.authority_policy import AuthorityPolicy
from fabric_cf.actor.core.core.broker_policy import BrokerPolicy
from fabric_cf.actor.core.core.policy import Policy
from fabric_cf.actor.core.plugins.base_plugin import BasePlugin
from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.core.policy.controller_ticket_review_policy import ControllerTicketReviewPolicy
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.security.auth_token import AuthToken


class BaseTestCase:
    actor_name = "testActor"
    actor_guid = ID()

    authority_name = "Test-Authority"
    broker_name = "Test-Broker"
    controller_name = "Test-Controller"

    controller_guid = "test-orchestrator-guid"
    authority_guid = "test-authority-guid"
    broker_guid = "test-broker-guid"

    db_user = 'fabric'
    db_pwd = 'fabric'
    db_name = 'test'
    db_host = '152.54.15.56:5432'

    logger = logging.getLogger('BaseTestCase')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="./actor.log")
    logger.setLevel(logging.INFO)

    Term.set_cycles = False

    def get_container_database(self) -> ABCContainerDatabase:
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        return GlobalsSingleton.get().get_container().get_database()

    def get_actor_clock(self) -> ActorClock:
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        return GlobalsSingleton.get().get_container().get_actor_clock()

    def make_actor_database(self) -> ABCDatabase:
        return ActorDatabase(user=self.db_user, password=self.db_pwd, database=self.db_name,
                             db_host=self.db_host,
                             logger=self.logger)

    def make_controller_database(self) -> ABCDatabase:
        return self.make_actor_database()

    def make_broker_database(self) -> ABCDatabase:
        return self.make_actor_database()

    def make_authority_database(self) -> ABCDatabase:
        return self.make_actor_database()

    def initialize_database(self, *, db: ActorDatabase, name: str):
        db.set_actor_name(name=name)
        db.initialize()

    def get_actor_database(self, *, name: str = None) -> ABCDatabase:
        if name is None:
            name = self.actor_name
        db = self.make_actor_database()
        self.initialize_database(db=db, name=name)
        return db

    def get_controller_database(self, *, name: str) -> ABCDatabase:
        db = self.make_controller_database()
        db.set_actor_name(name=name)
        db.initialize()
        return db

    def get_authority_database(self, *, name: str) -> ABCDatabase:
        db = self.make_authority_database()
        db.set_actor_name(name=name)
        db.initialize()
        return db

    def get_broker_database(self, *, name: str) -> ABCDatabase:
        db = self.make_broker_database()
        db.set_actor_name(name=name)
        db.initialize()
        return db

    def make_plugin(self) -> ABCBasePlugin:
        return BasePlugin(actor=None, db=None, handler_processor=None)

    def get_plugin(self, *, name: str = None) -> ABCBasePlugin:
        if name is None:
            name = self.actor_name
        plugin = self.make_plugin()
        plugin.set_database(db=self.get_actor_database(name=name))
        return plugin

    def get_controller_plugin(self, *, name: str) -> ABCBasePlugin:
        plugin = self.make_plugin()
        plugin.set_database(db=self.get_controller_database(name=name))
        return plugin

    def get_broker_plugin(self, *, name: str) -> ABCBasePlugin:
        plugin = self.make_plugin()
        plugin.set_database(db=self.get_broker_database(name=name))
        return plugin

    def get_authority_plugin(self, *, name: str) -> ABCBasePlugin:
        plugin = self.make_plugin()
        plugin.set_database(db=self.get_authority_database(name=name))
        return plugin

    def get_policy(self) -> ABCPolicy:
        return Policy()

    def get_authority_policy(self) -> ABCAuthorityPolicy:
        return AuthorityPolicy(actor=None)

    def get_broker_policy(self) -> ABCBrokerPolicyMixin:
        return BrokerPolicy(actor=None)

    def get_controller_policy(self) -> ABCControllerPolicy:
        return ControllerTicketReviewPolicy()

    def get_actor_instance(self) -> ABCActorMixin:
        from fabric_cf.actor.test.test_actor import TestActor
        actor = TestActor()
        actor.type = ActorType.All
        return actor

    def get_authority_instance(self) -> ABCActorMixin:
        from fabric_cf.actor.core.core.authority import Authority
        return Authority()

    def get_broker_instance(self) -> ABCActorMixin:
        from fabric_cf.actor.core.core.broker import Broker
        return Broker()

    def get_controller_instance(self) -> ABCActorMixin:
        from fabric_cf.actor.core.core.controller import Controller
        return Controller()

    def get_uninitialized_actor(self, *, name: str, guid: ID) -> ABCActorMixin:
        actor = self.get_actor_instance()
        token = AuthToken(name=name, guid=guid)
        actor.set_identity(token=token)
        actor.set_actor_clock(clock=self.get_actor_clock())
        actor.set_policy(policy=self.get_policy())
        actor.set_plugin(plugin=self.get_plugin(name=name))

        return actor

    def get_uninitialized_controller(self, *, name: str, guid: ID) -> ABCController:
        actor = self.get_controller_instance()
        token = AuthToken(name=name, guid=guid)
        actor.set_identity(token=token)
        actor.set_actor_clock(clock=self.get_actor_clock())
        actor.set_policy(policy=self.get_controller_policy())
        actor.set_plugin(plugin=self.get_plugin(name=name))
        return actor

    def get_uninitialized_broker(self, *, name: str, guid: ID) -> ABCBrokerMixin:
        actor = self.get_broker_instance()
        token = AuthToken(name=name, guid=guid)
        actor.set_identity(token=token)
        actor.set_actor_clock(clock=self.get_actor_clock())
        actor.set_policy(policy=self.get_broker_policy())
        actor.set_plugin(plugin=self.get_plugin(name=name))
        return actor

    def get_uninitialized_authority(self, *, name: str, guid: ID) -> ABCAuthority:
        actor = self.get_authority_instance()
        token = AuthToken(name=name, guid=guid)
        actor.set_identity(token=token)
        actor.set_actor_clock(clock=self.get_actor_clock())
        actor.set_policy(policy=self.get_authority_policy())
        actor.set_plugin(plugin=self.get_plugin(name=name))
        return actor

    def get_actor(self, *, name: str = actor_name, guid: ID = actor_guid) -> ABCActorMixin:
        actor = self.get_uninitialized_actor(name=name, guid=guid)
        actor.initialize(config=None)
        self.register_new_actor(actor=actor)
        return actor

    def get_controller(self, *, name: str = controller_name, guid: ID = controller_guid) -> ABCController:
        actor = self.get_uninitialized_controller(name=name, guid=guid)
        actor.initialize(config=None)
        self.register_new_actor(actor=actor)
        return actor

    def get_broker(self, *, name: str = broker_name, guid: ID = broker_guid) -> ABCBrokerMixin:
        actor = self.get_uninitialized_broker(name=name, guid=guid)
        actor.initialize(config=None)
        self.register_new_actor(actor=actor)
        return actor

    def get_authority(self, *, name: str = authority_name, guid: ID = authority_guid) -> ABCAuthority:
        actor = self.get_uninitialized_authority(name=name, guid=guid)
        actor.initialize(config=None)
        self.register_new_actor(actor=actor)
        return actor

    def register_new_actor(self, *, actor: ABCActorMixin):
        db = self.get_container_database()
        db.remove_actor(actor_name=actor.get_name())
        db.add_actor(actor=actor)
        ActorRegistrySingleton.get().unregister(actor=actor)
        ActorRegistrySingleton.get().register_actor(actor=actor)
        actor.actor_added(config=None)
        actor.start()

    def get_registered_new_actor(self) -> ABCActorMixin:
        actor = self.get_actor()
        self.register_new_actor(actor=actor)
        return actor