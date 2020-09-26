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
from typing import TYPE_CHECKING

from fabric.actor.core.common.constants import Constants
from fabric.actor.core.extensions.plugin import Plugin
from fabric.actor.core.apis.i_actor import IActor, ActorType

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_management_object import IManagementObject
    from fabric.actor.core.util.id import ID

from fabric.actor.core.apis.i_container_database import IContainerDatabase
from fabric.actor.db.psql_database import PsqlDatabase


class ContainerDatabase(IContainerDatabase):
    PropertyTime = "time"
    PropertyContainer = "container"

    def __init__(self, *, user: str, password: str, database: str, db_host: str, logger):
        self.user = user
        self.password = password
        self.database = database
        self.db_host = db_host
        self.db = PsqlDatabase(user=user, password=password, database=database, db_host=db_host, logger=logger)
        self.initialized = False
        self.reset_state = False
        self.logger = logger

    def __getstate__(self):
        state = self.__dict__.copy()
        self.db = PsqlDatabase(user=self.user, password=self.password, database=self.database, db_host=self.db_host, logger=self.logger)
        del state['initialized']
        del state['reset_state']
        del state['logger']

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.initialized = False
        self.reset_state = False

    def initialize(self):
        if not self.initialized:
            if self.reset_state:
                self.db.create_db()
                self.db.reset_db()
            self.initialized = True

    def set_reset_state(self, *, value: bool):
        self.reset_state = value

    def reset_db(self):
        self.db.reset_db()

    def add_actor(self, *, actor: IActor):
        properties = pickle.dumps(actor)
        self.db.add_actor(name=actor.get_name(), guid=str(actor.get_guid()), act_type=actor.get_type().value,
                          properties=properties)

    def remove_actor(self, *, actor_name: str):
        self.db.remove_actor(name=actor_name)

    def remove_actor_database(self, *, actor_name: str):
        self.db.remove_actor(name=actor_name)

    def get_actors(self, *, name: str = None, actor_type: int = None) -> list:
        result = None
        try:
            if name is None and actor_type is None:
                result = self.db.get_actors()
            elif name is not None and actor_type is not None:
                name = "%{}%".format(name)
                if actor_type != ActorType.All.value:
                    result = self.db.get_actors_by_name_and_type(actor_name=name, act_type=actor_type)
                else:
                    result = self.db.get_actors_by_name(act_name=name)
        except Exception as e:
            self.logger(e)
        return result

    def get_actor(self, *, actor_name: str) -> dict:
        result = None
        try:
            result = self.db.get_actor(name=actor_name)
        except Exception as e:
            self.logger(e)
        return result

    def add_time(self, *, properties: dict):
        self.db.add_miscellaneous(name=self.PropertyTime, properties=properties)

    def get_time(self) -> dict:
        result = None
        try:
            result = self.db.get_miscellaneous(name=self.PropertyTime)
        except Exception as e:
            self.logger(e)
        return result

    def add_container_properties(self, *, properties: dict):
        self.db.add_miscellaneous(name=self.PropertyContainer, properties=properties)

    def get_container_properties(self) -> dict:
        result = None
        try:
            result = self.db.get_miscellaneous(name=self.PropertyContainer)
        except Exception as e:
            self.logger(e)
        return result

    def _get_plugin_from_dict(self, *, plug_obj: dict) -> Plugin:
        if Constants.PropertyPickleProperties not in plug_obj:
            raise Exception("Invalid arguments")

        serialized_plugin = plug_obj[Constants.PropertyPickleProperties]
        deserialized_plugin = pickle.loads(serialized_plugin)
        return deserialized_plugin

    def get_plugin(self, *, plugin_id: str) -> Plugin:
        """
        Returns the specified plugin.
        @param plugin_id plugin identifier
        @return returns the specified plugin
        """
        result = None
        try:
            plug_obj = self.db.get_plugin(plugin_id=plugin_id)
            result = self._get_plugin_from_dict(plug_obj=plug_obj)
        except Exception as e:
            self.logger.error(e)
        return result

    def get_plugins(self, *, plugin_type: int, actor_type: int) -> list:
        """
        Returns an array of installed plugins.
        @param plugin_type plugin type
        @param actor_type plugin actor type
        @return returns the list of installed plugins
        """
        result = []
        try:
            for p in self.db.get_plugins(plg_type=plugin_type, plg_actor_type=actor_type):
                plugin = self._get_plugin_from_dict(plug_obj=p)
                result.append(plugin)
        except Exception as e:
            self.logger.error(e)
        return result

    def add_plugin(self, *, plugin: Plugin):
        properties = pickle.dumps(plugin)
        self.db.add_plugin(plugin_id=plugin.get_id(), plg_type=plugin.get_plugin_type(),
                           plg_actor_type=plugin.get_actor_type(), properties=properties)

    def remove_plugin(self, *, plugin_id: str):
        self.db.remove_plugin(plugin_id=plugin_id)

    def get_manager_objects_by_actor_name(self, *, actor_name: str) -> list:
        result = None
        try:
            result = self.db.get_manager_objects_by_actor_name(act_name=actor_name)
        except Exception as e:
            self.logger(e)
        return result

    def get_manager_containers(self) -> list:
        result = None
        try:
            result = self.db.get_manager_containers()
        except Exception as e:
            self.logger(e)
        return result

    def add_manager_object(self, *, manager: IManagementObject):
        properties = manager.save()
        self.db.add_manager_object(manager_key=str(manager.get_id()), properties=properties)

    def remove_manager_object(self, *, mid: ID):
        self.db.remove_manager_object(manager_key=str(mid))



