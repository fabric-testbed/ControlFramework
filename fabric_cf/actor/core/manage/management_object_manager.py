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
from typing import TYPE_CHECKING, List

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.util.reflection_utils import ReflectionUtils
from fabric_cf.actor.core.manage.management_object import ManagementObject

if TYPE_CHECKING:
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.apis.abc_container_database import ABCContainerDatabase
    from fabric_cf.actor.core.apis.abc_management_object import ABCManagementObject


class ManagementObjectManager:
    """
    This class makes it possible to register, index, and persist information about ManagementObjects.
    A ManagementObject can be registered only if no other object with the same identifier has been registered.
    Registering the same object more than once is not permitted.
    Each successfully registered management object is serialized and stored in the database.
    Note that once stored, this object cannot be updated. The stored information is sufficient to recreate an instance
    of the ManagementObject. Objects deriving from ManagementObject are responsible for their own persistence.
    """

    def __init__(self):
        # "Live" manager objects. These are all manager objects that have been instantiated inside this container.
        self.objects = {}
        # The database.
        self.db = None
        # logger
        self.logger = None
        # Initialization status.
        self.initialized = False
        self.lock = threading.Lock()

    def create_instance(self, *, mo_obj: dict) -> ManagementObject:
        if Constants.PROPERTY_CLASS_NAME not in mo_obj or Constants.PROPERTY_MODULE_NAME not in mo_obj:
            raise ManageException("Missing class name")

        class_name = mo_obj[Constants.PROPERTY_CLASS_NAME]
        module_name = mo_obj[Constants.PROPERTY_MODULE_NAME]

        self.logger.debug("Creating management object: {}".format(class_name))

        obj = ReflectionUtils.create_instance(module_name=module_name, class_name=class_name)

        if not isinstance(obj, ManagementObject):
            raise ManageException("Object does not implement ManagementObject interface")

        obj.reset(properties=mo_obj)
        obj.initialize()

        return obj

    def get_management_object(self, *, key: ID):
        """
        Retrieves the specified manager object.
        @param key object guid
        @return returns Management object
        """
        try:
            self.logger.debug("key: {} object: {}".format(key, self.objects.get(key, None)))
            self.lock.acquire()
            return self.objects.get(key, None)
        finally:
            self.lock.release()

    def initialize(self, *, db: ABCContainerDatabase):
        """
        Performs initialization. If the system is recovering after a
        shutdown/crash, loads all manager objects that pertain to the container.
        @param db database
        @throws Exception in case of error
        """
        if db is None:
            raise ManageException("database cannot be null")

        if not self.initialized:
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            self.logger = GlobalsSingleton.get().get_logger()
            self.db = db
            if not GlobalsSingleton.get().get_container().is_fresh():
                self.load_container_manager_objects()
            self.initialized = True

    def load_actor_manager_objects(self, *, actor_name: str):
        """
        Loads all manager objects associated with the specific actor.
        @param actor_name actor name
        @throws Exception in case of error
        """
        self.logger.info("Loading container-level management objects for actor: {}".format(actor_name))
        manager_objects = self.db.get_manager_objects_by_actor_name(actor_name=actor_name)
        self.load_objects(manager_objects=manager_objects)
        self.logger.info("Finished loading container-level management objects for actor: {}".format(actor_name))

    def load_container_manager_objects(self):
        """
        Loads all manager objects not associated with specific actors
        @throws Exception in case of error
        """
        self.logger.info("Loading container-level management objects")
        manager_objects = self.db.get_manager_container()
        if manager_objects is None:
            return
        self.load_objects(manager_objects=manager_objects)
        self.logger.info("Finished loading container-level management objects")

    def load_objects(self, *, manager_objects: List[dict]):
        """
        Loads the specified management objects
        @param manager_objects list of properties
        @throws Exception in case of error
        """
        if manager_objects is None:
            return
        for m in manager_objects:
            manager = self.create_instance(mo_obj=m[Constants.PROPERTY_PICKLE_PROPERTIES])
            try:
                self.lock.acquire()
                if manager.get_id() in self.objects:
                    raise ManageException("there is already a management object in memory with the specified id")

                self.objects[manager.get_id()] = manager

            finally:
                self.lock.release()

            self.logger.info("Loaded management object with id: {}".format(manager.get_id()))

    def register_manager_object(self, *, manager: ABCManagementObject):
        self.db.add_manager_object(manager=manager)
        try:
            self.lock.acquire()
            self.objects[manager.get_id()] = manager
        finally:
            self.lock.release()

    def unload_actor_manager_objects(self, *, actor_name: str):
        manager_objects = self.db.get_manager_objects_by_actor_name(actor_name=actor_name)
        self.unload_objects(manager_objects=manager_objects)

    def unload_objects(self, *, manager_objects: list):
        if manager_objects is not None:
            for manager in manager_objects:
                mgr_id = manager['mo_key']
                try:
                    self.lock.acquire()
                    if mgr_id in self.objects:
                        self.objects.pop(mgr_id)
                finally:
                    self.lock.release()

    def unregister_manager_object(self, *, mid: ID):
        try:
            self.lock.acquire()
            if mid in self.objects:
                self.objects.pop(mid)
        finally:
            self.lock.release()

            self.db.remove_manager_object(manager_key=mid)
