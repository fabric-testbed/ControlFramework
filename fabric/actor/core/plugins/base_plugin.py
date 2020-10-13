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

from fabric.actor.core.apis.i_delegation import IDelegation
from fabric.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_actor import IActor
    from fabric.actor.core.apis.i_database import IDatabase
    from fabric.actor.core.apis.i_reservation import IReservation
    from fabric.actor.core.apis.i_slice import ISlice
    from fabric.actor.core.core.actor import Actor
    from fabric.actor.core.apis.i_resource_ticket_factory import IResourceTicketFactory
    from fabric.actor.core.plugins.config.config_token import ConfigToken
    from fabric.actor.core.util.resource_data import ResourceData
    from fabric.actor.security.auth_token import AuthToken

from fabric.actor.core.apis.i_actor import ActorType
from fabric.actor.core.apis.i_actor_event import IActorEvent
from fabric.actor.core.apis.i_base_plugin import IBasePlugin
from fabric.actor.core.delegation.simple_resource_ticket_factory import SimpleResourceTicketFactory
from fabric.actor.core.kernel.slice_factory import SliceFactory
from fabric.actor.core.plugins.config.config import Config


class BasePlugin(IBasePlugin):
    """
    The base implementation for actor-specific extensions.
    """
    PropertyConfig = "PluginConfig"
    PropertyConfigProperties = "PluginConfigProperties"
    PropertyActorName = "PluginActorName"

    def __init__(self, *, actor: Actor, db: IDatabase, config: Config):
        super().__init__()
        self.db = db
        self.config = config
        self.config_properties = None
        self.actor = actor
        self.logger = None
        self.from_config = False
        self.ticket_factory = None
        self.initialized = False

    def __getstate__(self):
        state = self.__dict__.copy()
        state['actor_id'] = self.actor.get_guid()
        del state['logger']
        del state['ticket_factory']
        del state['actor']
        del state['initialized']

        return state

    def __setstate__(self, state):
        actor_id = state['actor_id']
        # TODO fetch actor via actor_id
        del state['actor_id']
        self.__dict__.update(state)

    def initialize(self):
        if not self.initialized:
            try:
                if self.actor is None:
                    raise Exception("Missing actor")

                if self.ticket_factory is None:
                    self.ticket_factory = self.make_ticket_factory()

                if self.db is not None:
                    self.db.set_logger(logger=self.logger)
                    self.db.set_actor_name(name=self.actor.get_name())
                    # TODO
                    self.db.set_reset_state(state=True)
                    self.db.initialize()

                self.ticket_factory.initialize()
                self.initialized = True
            except Exception as e:
                raise e

    def configure(self, *, properties):
        self.config_properties = properties
        self.from_config = True

    def actor_added(self):
        if self.db is not None:
            self.db.actor_added()

        if self.config is not None:
            self.config.set_slices_plugin(plugin=self)
            self.config.initialize()

    def recovery_starting(self):
        return

    def restart_configuration_actions(self, *, reservation: IReservation):
        return

    def revisit(self, *, slice_obj: ISlice = None, reservation: IReservation = None, delegation: IDelegation = None):
        return

    def recovery_ended(self):
        return

    def create_slice(self, *, slice_id: ID, name: str, properties: ResourceData):
        slice_obj = SliceFactory.create(slice_id=slice_id, name=name, data=properties)
        return slice_obj

    def release_slice(self, *, slice_obj: ISlice):
        return

    def validate_incoming(self, *, reservation: IReservation, auth: AuthToken):
        return True

    def process_configuration_complete(self, *, token: ConfigToken, properties: dict):
        target = properties[Config.PropertyTargetName]
        unsupported = False

        if target == Config.TargetCreate:
            self.process_create_complete(token=token, properties=properties)
        elif target == Config.TargetDelete:
            self.process_delete_complete(token=token, properties=properties)
        elif target == Config.TargetModify:
            self.process_modify_complete(token=token, properties=properties)
        else:
            unsupported = True
            self.logger.warning("Unsupported target in configurationComplete(): {}".format(target))

        if not unsupported:
            self.actor.get_policy().configuration_complete(action=target, token=token, out_properties=properties)

    class ConfigurationCompleteEvent(IActorEvent):
        def __init__(self, *, token: ConfigToken, properties: dict, outer_class):
            self.token = token
            self.properties = properties
            self.outer_class = outer_class

        def process(self):
            self.outer_class.process_configuration_complete(token=self.token, properties=self.properties)

    def configuration_complete(self, *, token: ConfigToken, properties: dict):
        self.actor.queue_event(incoming=BasePlugin.ConfigurationCompleteEvent(token=token, properties=properties,
                                                                              outer_class=self))

    def get_actor(self):
        return self.actor

    def get_config(self):
        return self.config

    def get_database(self) -> IDatabase:
        return self.db

    def get_logger(self):
        return self.logger

    def make_ticket_factory(self) -> IResourceTicketFactory:
        ticket_factory = SimpleResourceTicketFactory()
        ticket_factory.set_actor(actor=self.actor)
        return ticket_factory

    def process_create_complete(self, *, token: ConfigToken, properties: dict):
        return

    def process_delete_complete(self, *, token: ConfigToken, properties: dict):
        return

    def process_modify_complete(self, *, token: ConfigToken, properties: dict):
        return

    def set_actor(self, *, actor: IActor):
        self.actor = actor

    def set_config(self, *, config: Config):
        self.config = config

    def set_database(self, *, db: IDatabase):
        self.db = db

    def set_logger(self, *, logger):
        self.logger = logger

    def get_ticket_factory(self) -> IResourceTicketFactory:
        return self.ticket_factory

    def set_ticket_factory(self, *, ticket_factory):
        self.ticket_factory = ticket_factory

    def get_config_properties(self) -> dict:
        return self.config_properties

    def is_site_authority(self):
        return self.actor.get_type() == ActorType.Authority

