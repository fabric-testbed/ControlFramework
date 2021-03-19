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
from typing import TYPE_CHECKING, List

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import DatabaseException
from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin, ActorType
from fabric_cf.actor.core.apis.abc_container_database import ABCContainerDatabase
from fabric_cf.actor.db.psql_database import PsqlDatabase

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_management_object import ABCManagementObject
    from fabric_cf.actor.core.util.id import ID


class ContainerDatabase(ABCContainerDatabase):
    """
    Implements Container Interface to various Database operations
    """
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
        self.db = PsqlDatabase(user=self.user, password=self.password, database=self.database,
                               db_host=self.db_host, logger=self.logger)
        del state['initialized']
        del state['reset_state']
        del state['logger']

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.initialized = False
        self.reset_state = False

    def initialize(self):
        """
        Initialize
        """
        if not self.initialized:
            self.db.create_db()
            if self.reset_state:
                self.db.reset_db()
            self.initialized = True

    def set_reset_state(self, *, value: bool):
        """
        Set Reset State
        """
        self.reset_state = value

    def reset_db(self):
        """
        Reset the database
        """
        self.db.reset_db()

    def add_actor(self, *, actor: ABCActorMixin):
        """
        Add an actor
        @param actor actor
        """
        properties = pickle.dumps(actor)
        self.db.add_actor(name=actor.get_name(), guid=str(actor.get_guid()), act_type=actor.get_type().value,
                          properties=properties)

    def remove_actor(self, *, actor_name: str):
        """
        Remove an actor
        @param actor_name actor name
        """
        self.db.remove_actor(name=actor_name)

    def remove_actor_database(self, *, actor_name: str):
        """
        Remove an actor
        @param actor_name actor name
        """
        self.db.remove_actor(name=actor_name)

    def get_actors(self, *, name: str = None, actor_type: int = None) -> List[ABCActorMixin]:
        """
        Get Actors
        @param name actor name
        @param actor_type actor type
        @return list of actors
        """
        result = None
        try:
            act_dict_list = None
            if name is None and actor_type is None:
                act_dict_list = self.db.get_actors()
            elif name is not None and actor_type is not None:
                name = "%{}%".format(name)
                if actor_type != ActorType.All.value:
                    act_dict_list = self.db.get_actors_by_name_and_type(actor_name=name, act_type=actor_type)
                else:
                    act_dict_list = self.db.get_actors_by_name(act_name=name)
            if act_dict_list is not None:
                result = []
                for a in act_dict_list:
                    pickled_actor = a.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                    act_obj = pickle.loads(pickled_actor)
                    result.append(act_obj)
            return result
        except Exception as e:
            self.logger.error(e)
        return result

    def get_actor(self, *, actor_name: str) -> dict:
        """
        Get Actor
        @param name actor name
        @return actor
        """
        result = None
        try:
            act_dict = self.db.get_actor(name=actor_name)
            if act_dict is not None:
                pickled_actor = act_dict.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                return pickle.loads(pickled_actor)
        except Exception as e:
            self.logger.error(e)
        return result

    def get_actor_id(self, *, actor_name: str) -> dict:
        """
        Get Actor
        @param name actor name
        @return actor
        """
        result = None
        try:
            act_dict = self.db.get_actor(name=actor_name)
            if act_dict is not None:
                return act_dict['act_id']
        except Exception as e:
            self.logger.error(e)
        return result

    def add_time(self, *, properties: dict):
        """
        Add time
        @param properties properties
        """
        self.db.add_miscellaneous(name=self.PropertyTime, properties=properties)

    def get_time(self) -> dict:
        """
        Get Time
        @param time properties
        """
        result = None
        try:
            result = self.db.get_miscellaneous(name=self.PropertyTime)
        except Exception as e:
            self.logger.error(e)
        return result

    def add_container_properties(self, *, properties: dict):
        """
        Add container properties
        @param properties properties
        """
        self.db.add_miscellaneous(name=self.PropertyContainer, properties=properties)

    def get_container_properties(self) -> dict:
        """
        Get Container Properties
        @return properties
        """
        result = None
        try:
            result = self.db.get_miscellaneous(name=self.PropertyContainer)
        except Exception as e:
            self.logger.error(e)
        return result

    def get_manager_objects_by_actor_name(self, *, actor_name: str) -> list:
        """
        Get Management Object by actor name
        @param actor_name actor name
        @return list of management objects
        """
        result = None
        try:
            result = self.db.get_manager_objects_by_actor_name(act_name=actor_name)
        except Exception as e:
            self.logger.error(e)
        return result

    def get_manager_container(self) -> List[dict]:
        """
        Get Management Container
        @return list of management objects for containers
        """
        result = None
        try:
            result = self.db.get_manager_containers()
        except Exception as e:
            self.logger.error(e)
        return result

    def add_manager_object(self, *, manager: ABCManagementObject):
        """
        Add Management object
        @param manager management object
        """
        properties = manager.save()
        act_id = None
        actor_name = manager.get_actor_name()
        if actor_name is not None:
            act_id = self.get_actor_id(actor_name=actor_name)
        self.db.add_manager_object(manager_key=str(manager.get_id()), properties=properties, act_id=act_id)

    def remove_manager_object(self, *, mid: ID):
        """
        Remove management object
        @param mid management object id
        """
        self.db.remove_manager_object(manager_key=str(mid))
