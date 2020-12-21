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

from datetime import datetime

from fabric_cf.actor.core.common.exceptions import ResourcesException
from fabric_cf.actor.core.util.resource_type import ResourceType

from fabric_cf.actor.core.common.resource_pool_attribute_descriptor import ResourcePoolAttributeDescriptor


class ResourcePoolDescriptor:
    property_type = "type"
    property_label = "label"
    property_description = "description"
    property_attributes_prefix = "attribute."
    property_attributes_count = "attributescount"
    property_key = "key"

    def __init__(self):
        self.attributes = {}
        self.units = 0
        self.start = None
        self.end = None
        self.handler_module = None
        self.handler_class = None
        self.handler_properties = {}
        self.pool_properties = {}
        self.resource_type = None
        self.resource_label = None
        self.description = None
        self.factory_module = None
        self.factory_class = None

    def __str__(self):
        return "units:{} start:{} end:{} handler_module:{} handler_class:{} handler_properties:{} pool_properties:{} " \
               "resource_type:{} resource_label:{} description:{} factory_module:{} factory_class:{}".\
            format(self.units, self.start, self.end, self.handler_module, self.handler_class, self.handler_properties,
                   self.pool_properties, self.resource_type, self.resource_label, self.description, self.factory_module,
                   self.factory_class)

    def get_resource_type(self) -> ResourceType:
        """
        Get resource type
        @return resource type
        """
        return self.resource_type

    def set_resource_type(self, *, rtype: ResourceType):
        """
        Set resource type
        @param rtype resource type
        """
        self.resource_type = rtype

    def get_handler_module(self) -> str:
        """
        Get Handler Module
        @return handler module
        """
        return self.handler_module

    def set_handler_module(self, *, module: str):
        """
        Set Handler module
        @param module Handler module
        """
        self.handler_module = module

    def set_handler_class(self, *, handler_class: str):
        """
        Set Handler class
        @param handler_class Handler class
        """
        self.handler_class = handler_class

    def get_handler_class(self) -> str:
        """
        Get Handler class
        @return handler class
        """
        return self.handler_class

    def get_handler_properties(self) -> dict:
        """
        Get Handler Properties
        @return handler Properties
        """
        return self.handler_properties

    def set_handler_properties(self, *, properties: dict):
        """
        Set handler Properties
        @param properties handler Properties
        """
        self.handler_properties = properties

    def get_pool_properties(self) -> dict:
        """
        Get Pool Properties
        @return Pool Properties
        """
        return self.pool_properties

    def set_pool_properties(self, *, properties: dict):
        """
        Set Pool Properties
        @param properties Pool Properties
        """
        self.pool_properties = properties

    def get_resource_type_label(self) -> str:
        """
        Get resource type label
        @return resource type label
        """
        return self.resource_label

    def set_resource_type_label(self, *, rtype_label: str):
        """
        Set resource type label
        @param rtype_label resource type label
        """
        self.resource_label = rtype_label

    def get_units(self) -> int:
        """
        Get units
        @return units
        """
        return self.units

    def set_units(self, *, units: int):
        """
        Set units
        @param units units
        """
        self.units = units

    def get_start(self) -> datetime:
        """
        Get Start Lease Time
        @return Start Lease Time
        """
        return self.start

    def set_start(self, *, start: datetime):
        """
        Set start Lease Time
        @param start start Lease Time
        """
        self.start = start

    def get_end(self) -> datetime:
        """
        Get End Lease Time
        @return End Lease Time
        """
        return self.end

    def set_end(self, *, end: datetime):
        """
        Set End Lease Time
        @param end End Lease Time
        """
        self.end = end

    def get_description(self) -> str:
        """
        Get Description
        @return description
        """
        return self.description

    def set_description(self, *, description: str):
        """
        Set description
        @param description description
        """
        self.description = description

    def get_pool_factory_module(self) -> str:
        """
        Get Pool Factory Module
        @return factory module
        """
        return self.factory_module

    def set_pool_factory_module(self, *, factory_module: str):
        """
        Set Pool factory module
        @param factory_module factory module
        """
        self.factory_module = factory_module

    def get_pool_factory_class(self) -> str:
        """
        Get Pool Factory class
        @return pool factory class
        """
        return self.factory_class

    def set_pool_factory_class(self, *, factory_class: str):
        """
        Set Pool Factory Class
        @param factory_class factory class
        """
        self.factory_module = factory_class

    def get_attribute(self, *, key: str) -> ResourcePoolAttributeDescriptor:
        """
        Get an attribute
        @param key attribute key
        """
        if key in self.attributes:
            return self.attributes[key]
        return None

    def get_attributes(self) -> list:
        """
        Return attributes
        @return list of attributes
        """
        return [x for x in self.attributes.values()]

    def add_attribute(self, *, attribute: ResourcePoolAttributeDescriptor):
        """
        Add an attribute
        @param attribute Attribute
        """
        self.attributes[attribute.get_key()] = attribute

    def save(self, *, properties: dict, prefix: str = None) -> dict:
        """
        Save properties
        @param properties properties
        @param prefix prefix
        @return properties
        """
        if prefix is None:
            prefix = ""

        properties[prefix + self.property_type] = str(self.resource_type)
        properties[prefix + self.property_label] = self.resource_label
        if self.description is not None:
            properties[prefix + self.property_description] = self.description

        properties[prefix + self.property_attributes_count] = str(len(self.attributes))
        i = 0
        for a in self.attributes.values():
            properties[prefix + self.property_attributes_prefix + str(i) + "." + self.property_key] = a.get_key()
            if len(prefix) > 0:
                temp = prefix + a.get_key() + "."
            else:
                temp = a.get_key() + "."
            properties = a.save(properties=properties, prefix=temp)
            i += 1
        return properties

    def reset(self, *, properties: dict, prefix: str = None):
        """
        Reset properties
        @param properties properties
        @param prefix prefix
        """
        if prefix is None:
            prefix = ""

        if (prefix + self.property_type) not in properties:
            raise ResourcesException("Missing resource type")

        self.resource_type = ResourceType(resource_type=properties[prefix + self.property_type])

        if (prefix + self.property_label) not in properties:
            raise ResourcesException("Missing resource label")

        self.resource_label = properties[prefix + self.property_label]

        if prefix + self.property_description in properties:
            self.description = properties[prefix + self.property_description]

        if (prefix + self.property_attributes_count) not in properties:
            raise ResourcesException("Missing attributes count")

        count = int(properties[prefix + self.property_attributes_count])
        for i in range(count):
            key = prefix + self.property_attributes_prefix + str(i) + "." + self.property_key
            if key not in properties:
                raise ResourcesException("Could not find key for attribute #{}".format(i))
            key_value = properties[key]

            if len(prefix) > 0:
                temp = prefix + key_value + "."
            else:
                temp = key_value + "."

            attribute = ResourcePoolAttributeDescriptor()
            attribute.reset(properties=properties, prefix=temp)
            attribute.set_key(value=key_value)
            self.add_attribute(attribute=attribute)

    def clone(self):
        """
        Clone an object
        @return copy of current object
        """
        properties = {}
        self.save(properties=properties)
        copy = ResourcePoolDescriptor()
        try:
            copy.reset(properties=properties)
        except Exception as e:
            raise ResourcesException("Unexpected error during deserialization={}".format(e))
        return copy
