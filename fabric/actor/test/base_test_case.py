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

from fabric.actor.core.apis.i_actor import IActor
from fabric.actor.core.apis.i_authority_policy import IAuthorityPolicy
from fabric.actor.core.apis.i_base_plugin import IBasePlugin
from fabric.actor.core.apis.i_broker import IBroker
from fabric.actor.core.apis.i_broker_policy import IBrokerPolicy
from fabric.actor.core.apis.i_container_database import IContainerDatabase
from fabric.actor.core.apis.i_controller_policy import IControllerPolicy
from fabric.actor.core.apis.i_database import IDatabase
from fabric.actor.core.apis.i_policy import IPolicy
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.core.authority_policy import AuthorityPolicy
from fabric.actor.core.core.broker_policy import BrokerPolicy
from fabric.actor.core.core.policy import Policy
from fabric.actor.core.delegation.simple_resource_ticket_factory import SimpleResourceTicketFactory
from fabric.actor.core.plugins.base_plugin import BasePlugin
from fabric.actor.core.plugins.db.actor_database import ActorDatabase
from fabric.actor.core.policy.controller_calendar_policy import ControllerCalendarPolicy
from fabric.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric.actor.core.time.actor_clock import ActorClock
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.security.auth_token import AuthToken


class BaseTestCase:
    ActorName = "testActor"
    ActorGuid = ID()

    AuthorityName = "Authority"
    BrokerName = "broker"
    ControllerName = "controller"

    ControllerGuid = "test-controller-guid"
    AuthorityGuid = "test-authority-guid"
    BrokerGuid = "test-broker-guid"

    DbUser = 'test'
    DbPwd = 'test'
    DbName = 'test'
    DbHost = '127.0.0.1:9432'

    Logger = logging.getLogger('BaseTestCase')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="./actor.log")
    Logger.setLevel(logging.INFO)

    Term.set_cycles = False

    def get_container_database(self) -> IContainerDatabase:
        from fabric.actor.core.container.globals import GlobalsSingleton
        return GlobalsSingleton.get().get_container().get_database()

    def get_actor_clock(self) -> ActorClock:
        from fabric.actor.core.container.globals import GlobalsSingleton
        return GlobalsSingleton.get().get_container().get_actor_clock()

    def make_actor_database(self) -> IDatabase:
        return ActorDatabase(self.DbUser, self.DbPwd, self.DbName, self.DbHost, self.Logger)

    def make_controller_database(self) -> IDatabase:
        return self.make_actor_database()

    def make_broker_database(self) -> IDatabase:
        return self.make_actor_database()

    def make_authority_database(self) -> IDatabase:
        return self.make_actor_database()

    def initialize_database(self, db: ActorDatabase, name: str):
        db.set_actor_name(name)
        db.initialize()

    def get_actor_database(self, name: str = None):
        if name is None:
            name = self.ActorName
        db = self.make_actor_database()
        self.initialize_database(db, name)
        return db

    def get_controller_database(self, name: str):
        db = self.make_controller_database()
        db.set_actor_name(name)
        db.initialize()
        return db

    def get_authority_database(self, name: str):
        db = self.make_authority_database()
        db.set_actor_name(name)
        db.initialize()
        return db

    def get_broker_database(self, name: str):
        db = self.make_broker_database()
        db.set_actor_name(name)
        db.initialize()
        return db

    def make_plugin(self) -> IBasePlugin:
        return BasePlugin(None, None, None)

    def get_plugin(self, name: str = None) -> IBasePlugin:
        if name is None:
            name = self.ActorName
        plugin = self.make_plugin()
        plugin.set_database(self.get_actor_database(name))
        return plugin

    def get_controller_plugin(self, name: str) -> IBasePlugin:
        plugin = self.make_plugin()
        plugin.set_database(self.get_controller_database(name))
        return plugin

    def get_broker_plugin(self, name: str) -> IBasePlugin:
        plugin = self.make_plugin()
        plugin.set_database(self.get_broker_database(name))
        return plugin

    def get_authority_plugin(self, name: str) -> IBasePlugin:
        plugin = self.make_plugin()
        plugin.set_database(self.get_authority_database(name))
        return plugin

    def get_policy(self) -> IPolicy:
        return Policy()

    def get_authority_policy(self) -> IAuthorityPolicy:
        return AuthorityPolicy(None)

    def get_broker_policy(self) -> IBrokerPolicy:
        return BrokerPolicy(None)

    def get_controller_policy(self) -> IControllerPolicy:
        return ControllerCalendarPolicy()

    def get_actor_instance(self) -> IActor:
        from fabric.actor.test.test_actor import TestActor
        actor = TestActor()
        actor.type = Constants.ActorTypeAll
        return actor

    def get_authority_instance(self) -> IActor:
        from fabric.actor.core.core.authority import Authority
        return Authority()

    def get_broker_instance(self) -> IActor:
        from fabric.actor.core.core.broker import Broker
        return Broker()

    def get_controller_instance(self) -> IActor:
        from fabric.actor.core.core.controller import Controller
        return Controller()

    def get_uninitialized_actor(self, name: str, guid: ID):
        actor = self.get_actor_instance()
        token = AuthToken(name, guid)
        actor.set_identity(token)
        actor.set_actor_clock(self.get_actor_clock())
        actor.set_policy(self.get_policy())
        actor.set_plugin(self.get_plugin(name))
        tf = SimpleResourceTicketFactory()
        tf.set_actor(actor)
        actor.get_plugin().set_ticket_factory(tf)
        return actor

    def get_uninitialized_controller(self, name: str, guid: ID):
        actor = self.get_controller_instance()
        token = AuthToken(name, guid)
        actor.set_identity(token)
        actor.set_actor_clock(self.get_actor_clock())
        actor.set_policy(self.get_controller_policy())
        actor.set_plugin(self.get_plugin(name))
        tf = SimpleResourceTicketFactory()
        tf.set_actor(actor)
        actor.get_plugin().set_ticket_factory(tf)
        return actor

    def get_uninitialized_broker(self, name: str, guid: ID):
        actor = self.get_broker_instance()
        token = AuthToken(name, guid)
        actor.set_identity(token)
        actor.set_actor_clock(self.get_actor_clock())
        actor.set_policy(self.get_broker_policy())
        actor.set_plugin(self.get_plugin(name))
        tf = SimpleResourceTicketFactory()
        tf.set_actor(actor)
        actor.get_plugin().set_ticket_factory(tf)
        return actor

    def get_uninitialized_authority(self, name: str, guid: ID):
        actor = self.get_authority_instance()
        token = AuthToken(name, guid)
        actor.set_identity(token)
        actor.set_actor_clock(self.get_actor_clock())
        actor.set_policy(self.get_authority_policy())
        actor.set_plugin(self.get_plugin(name))
        tf = SimpleResourceTicketFactory()
        tf.set_actor(actor)
        actor.get_plugin().set_ticket_factory(tf)
        return actor

    def get_actor(self, name: str = ActorName, guid: ID = ActorGuid):
        actor = self.get_uninitialized_actor(name, guid)
        actor.initialize()
        self.register_new_actor(actor)
        return actor

    def get_controller(self, name: str = ControllerName, guid: ID = ControllerGuid):
        actor = self.get_uninitialized_controller(name, guid)
        actor.initialize()
        self.register_new_actor(actor)
        return actor

    def get_broker(self, name: str = BrokerName, guid: ID = BrokerGuid) -> IBroker:
        actor = self.get_uninitialized_broker(name, guid)
        actor.initialize()
        self.register_new_actor(actor)
        return actor

    def get_authority(self, name: str = AuthorityName, guid: ID = AuthorityGuid):
        actor = self.get_uninitialized_authority(name, guid)
        actor.initialize()
        self.register_new_actor(actor)
        return actor

    def register_new_actor(self, actor: IActor):
        db = self.get_container_database()
        db.remove_actor(actor.get_name())
        db.add_actor(actor)
        ActorRegistrySingleton.get().unregister(actor)
        ActorRegistrySingleton.get().register_actor(actor)
        actor.actor_added()
        actor.start()

    def get_registered_new_actor(self) -> IActor:
        actor = self.get_actor()
        self.register_new_actor(actor)
        return actor