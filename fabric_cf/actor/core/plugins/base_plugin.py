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

from fabric_cf.actor.boot.configuration import ActorConfig
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import PluginException
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.apis.abc_actor_event import ABCActorEvent
from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.plugins.handlers.handler_processor import HandlerProcessor

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_database import ABCDatabase
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.core.actor import ActorMixin
    from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
    from fabric_cf.actor.security.auth_token import AuthToken


class BasePlugin(ABCBasePlugin):
    """
    The base implementation for actor-specific extensions.
    """

    def __init__(self, *, actor: ActorMixin, db: ABCDatabase, handler_processor: HandlerProcessor):
        super().__init__()
        self.db = db
        self.handler_processor = handler_processor
        self.config_properties = None
        self.actor = actor
        self.logger = None
        self.from_config = False
        self.initialized = False

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['initialized']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.actor = None
        self.initialized = False

    def initialize(self):
        if not self.initialized:
            if self.actor is None:
                raise PluginException(Constants.NOT_SPECIFIED_PREFIX.format("actor"))

            if self.db is not None:
                self.db.set_logger(logger=self.logger)
                self.db.set_actor_name(name=self.actor.get_name())
                from fabric_cf.actor.core.container.globals import GlobalsSingleton
                is_fresh = GlobalsSingleton.get().get_container().is_fresh()
                self.db.set_reset_state(state=is_fresh)
                self.db.initialize()

            self.initialized = True

    def configure(self, *, properties):
        self.config_properties = properties
        self.from_config = True

    def actor_added(self, *, config: ActorConfig):
        if self.db is not None:
            self.db.actor_added(actor=self.actor)

        if self.handler_processor is not None:
            self.handler_processor.set_plugin(plugin=self)
            self.handler_processor.initialize(config=config)

    def recovery_starting(self):
        return

    def restart_configuration_actions(self, *, reservation: ABCReservationMixin):
        return

    def revisit(self, *, slice_obj: ABCSlice = None, reservation: ABCReservationMixin = None,
                delegation: ABCDelegation = None):
        return

    def recovery_ended(self):
        return

    def create_slice(self, *, slice_id: ID, name: str, project_id: str, project_name: str):
        slice_obj = SliceFactory.create(slice_id=slice_id, name=name, project_id=project_id,
                                        project_name=project_name)
        return slice_obj

    def release_slice(self, *, slice_obj: ABCSlice):
        return

    def validate_incoming(self, *, reservation: ABCReservationMixin, auth: AuthToken):
        return True

    def process_configuration_complete(self, *, unit: ConfigToken, properties: dict):
        target = properties[Constants.PROPERTY_TARGET_NAME]
        unsupported = False

        if target == Constants.TARGET_CREATE:
            self.process_create_complete(unit=unit, properties=properties)
        elif target == Constants.TARGET_DELETE:
            self.process_delete_complete(unit=unit, properties=properties)
        elif target == Constants.TARGET_MODIFY:
            self.process_modify_complete(unit=unit, properties=properties)
        else:
            unsupported = True
            self.logger.warning("Unsupported target in configurationComplete(): {}".format(target))

        if not unsupported:
            self.actor.get_policy().configuration_complete(action=target, token=unit,
                                                           out_properties=properties)

    class ConfigurationCompleteEvent(ABCActorEvent):
        def __init__(self, *, token: ConfigToken, properties: dict, outer_class):
            self.token = token
            self.properties = properties
            self.outer_class = outer_class

        def process(self):
            self.outer_class.process_configuration_complete(unit=self.token, properties=self.properties)

    def configuration_complete(self, *, token: ConfigToken, properties: dict):
        self.actor.queue_event(incoming=BasePlugin.ConfigurationCompleteEvent(token=token, properties=properties,
                                                                              outer_class=self))

    def get_actor(self):
        return self.actor

    def get_handler_processor(self) -> HandlerProcessor:
        return self.handler_processor

    def get_database(self) -> ABCDatabase:
        return self.db

    def get_logger(self):
        return self.logger

    def process_create_complete(self, *, unit: ConfigToken, properties: dict):
        return

    def process_delete_complete(self, *, unit: ConfigToken, properties: dict):
        return

    def process_modify_complete(self, *, unit: ConfigToken, properties: dict):
        return

    def set_actor(self, *, actor: ABCActorMixin):
        self.actor = actor

    def set_database(self, *, db: ABCDatabase):
        self.db = db

    def set_logger(self, *, logger):
        self.logger = logger
        if self.db is not None:
            self.db.set_logger(logger=self.logger)

    def get_config_properties(self) -> dict:
        return self.config_properties

    def is_site_authority(self):
        return self.actor.get_type() == ActorType.Authority
