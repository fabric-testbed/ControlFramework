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
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.manage.proxy_protocol_descriptor import ProxyProtocolDescriptor
from fabric.actor.core.util.id import ID
from fabric.actor.core.apis.i_management_object import IManagementObject

if TYPE_CHECKING:
    from fabric.message_bus.messages.result_avro import ResultAvro


class ManagementObject(IManagementObject):
    """
    Base class for all manager objects. A manager object is part of the management layer.
    It provides a set of management operations for a given component, for example, actor or slice.
    Each manager object is registered with the management layer under a unique
    identifier. The creator of the object is responsible for assigning the
    correct identifier. The default constructor of ManagerObject
    generates a unique identifier for each new instance. This identifier can be
    modified and replaced with the desired identifier. Once a manager object has
    been registered with the management layer, its identifier cannot change.
 
    The type_id field of each ManagerObject can be used to
    assign the same identifier to all instances of a given manager object class.
    The type identifier field can then be used to construct an appropriate proxy
    to the ManagerObject.
 
    Each ManagerObject can be accessed using a number of protocols.
    By default, each object can be accessed using local communication.
    In addition to local communication, the object may support remote
    communication protocols such as KAFKA. Each ManagerObject maintains an array of protocol descriptors for
    each supported protocol.
 
    Each ManagerObject is responsible for its own persistence. The
    save() method is going to be invoked only once (when the
    object is registered). The reset method is going to be invoked
    every time the system has to reinstantiate the ManagerObject.
 
    A ManagerObject can be associated with a given actor. A
    ManagerObject is associated with an actor if
    ManagementObject#getActorName() returns a valid actor name. All manager objects not
    associated with an actor are considered "container-level", even though they
    may interact with one or more actors.
    """

    def __init__(self):
        self.type_id = None
        self.proxies = None
        self.id = ID()
        from fabric.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.initialized = False
        self.serial = None

    def register_protocols(self):
        return

    def initialize(self):
        """
        Performs initialization of the manager object
        @throws Exception in case of error
        """
        if not self.initialized:
            self.register_protocols()
            if self.serial is not None:
                self.recover()

            self.initialized = True

    def save(self) -> dict:
        properties = {
                        Constants.property_class_name: ManagementObject.__name__,
                        Constants.property_module_name: ManagementObject.__module__,
                        Constants.property_id: str(self.id)
                     }

        if self.type_id is not None:
            properties[Constants.property_type_id] = str(self.type_id)

        self.save_protocols(properties=properties)

        if self.get_actor_name() is not None:
            properties[Constants.property_actor_name] = self.get_actor_name()

        return properties

    def save_protocols(self, *, properties: dict) -> dict:
        if self.proxies is not None:
            properties[Constants.property_proxies_length] = len(self.proxies)
            i = 0
            for p in self.proxies:
                properties[
                    Constants.property_proxies_prefix + str(i) + Constants.property_proxies_protocol] = p.get_protocol()
                properties[
                    Constants.property_proxies_prefix + str(i) + Constants.property_proxies_class] = p.get_proxy_class()
                properties[
                    Constants.property_proxies_prefix + str(i) + Constants.property_proxies_module] = p.get_proxy_module()
                i += 1
        return properties

    def load_protocols(self, *, properties: dict):
        if Constants.property_proxies_length in properties:
            count = int(properties[Constants.property_proxies_length])
            self.proxies = []
            for i in range(count):
                proxy = ProxyProtocolDescriptor()
                proxy.set_protocol(protocol=properties[
                    Constants.property_proxies_prefix + str(i) + Constants.property_proxies_protocol])
                proxy.set_proxy_class(proxy_class=properties[
                    Constants.property_proxies_prefix + str(i) + Constants.property_proxies_class])
                proxy.set_proxy_module(proxy_module=properties[
                    Constants.property_proxies_prefix + str(i) + Constants.property_proxies_module])

    def reset(self, *, properties: dict):
        self.id = ID(uid=properties[Constants.property_id])

        if Constants.property_type_id in properties:
            self.type_id = ID(uid=properties[Constants.property_type_id])

        self.load_protocols(properties=properties)

        self.serial = properties

    def recover(self):
        """
        Performs recovery actions for this manager object.
        @throws Exception in case of error
        """

    def get_id(self) -> ID:
        return self.id

    def get_actor_name(self) -> str:
        return None

    def get_type_id(self) -> ID:
        return self.type_id

    def get_proxies(self) -> list:
        return self.proxies

    @staticmethod
    def set_exception_details(*, result: ResultAvro, e: Exception):
        result.message += str(e)
        result.set_details(traceback.format_exc())
        return result