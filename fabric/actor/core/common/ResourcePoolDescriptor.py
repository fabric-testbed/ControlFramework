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
from fabric.actor.core.util.ResourceType import ResourceType

from fabric.actor.core.common.ResourcePoolAttributeDescriptor import ResourcePoolAttributeDescriptor


class ResourcePoolDescriptor:
    PropertyType = "type"
    PropertyLabel = "label"
    PropertyDescription = "description"
    PropertyAttributesPrefix = "attribute."
    PropertyAttributesCount = "attributescount"
    PropertyKey = "key"

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
        return self.resource_type

    def set_resource_type(self, rtype: ResourceType):
        self.resource_type = rtype

    def get_handler_module(self) -> str:
        return self.handler_module

    def set_handler_module(self, module: str):
        self.handler_module = module

    def set_handler_class(self, handler_class: str):
        self.handler_class = handler_class

    def get_handler_class(self) -> str:
        return self.handler_class

    def get_handler_properties(self) -> dict:
        return self.handler_properties

    def set_handler_properties(self, properties: dict):
        self.handler_properties = properties

    def get_pool_properties(self) -> dict:
        return self.pool_properties

    def set_pool_properties(self, properties: dict):
        self.pool_properties = properties

    def get_resource_type_label(self) -> str:
        return self.resource_label

    def set_resource_type_label(self, rtype_label: str):
        self.resource_label = rtype_label

    def get_units(self) -> int:
        return self.units

    def set_units(self, units: int):
        self.units = units

    def get_start(self) -> datetime:
        return self.start

    def set_start(self, start: datetime):
        self.start = start

    def get_end(self) -> datetime:
        return self.end

    def set_end(self, end: datetime):
        self.end = end

    def get_description(self) -> str:
        return self.description

    def set_description(self, description: str):
        self.description = description

    def get_pool_factory_module(self) -> str:
        return self.factory_module

    def set_pool_factory_module(self, factory_module: str):
        self.factory_module = factory_module

    def get_pool_factory_class(self) -> str:
        return self.factory_class

    def set_pool_factory_class(self, factory_class: str):
        self.factory_module = factory_class

    def get_attribute(self, key: str) -> ResourcePoolAttributeDescriptor:
        if key in self.attributes:
            return self.attributes[key]

    def get_attributes(self) -> list:
        return self.attributes.values()

    def add_attribute(self, attribute: ResourcePoolAttributeDescriptor):
        self.attributes[attribute.get_key()] = attribute

    def save(self, properties: dict, prefix: str) -> dict:
        if prefix is None:
            prefix = ""

        properties[prefix + self.PropertyType] = str(self.resource_type)
        properties[prefix + self.PropertyLabel] = self.resource_label
        if self.description is not None:
            properties[prefix + self.PropertyDescription] = self.description

        properties[prefix + self.PropertyAttributesCount] = str(len(self.attributes))
        i = 0
        for a in self.attributes.values():
            properties[prefix + self.PropertyAttributesPrefix + str(i) + "." + self.PropertyKey] = a.get_key()
            if len(prefix) > 0:
                temp = prefix + a.get_key() + "."
            else:
                temp = a.get_key() + "."
            properties = a.save(properties, temp)
            i += 1
        return properties

    def reset(self, properties: dict, prefix: str):
        if prefix is None:
            prefix = ""

        if (prefix + self.PropertyType) not in properties:
            raise Exception("Missing resource type")

        self.resource_type = ResourceType(properties[prefix + self.PropertyType])

        if (prefix + self.PropertyLabel) not in properties:
            raise Exception("Missing resource label")

        self.resource_label = properties[prefix + self.PropertyLabel]

        if prefix + self.PropertyDescription in properties:
            self.description = properties[prefix + self.PropertyDescription]

        if (prefix + self.PropertyAttributesCount) not in properties:
            raise Exception("Missing attributes count")

        count = int(properties[prefix + self.PropertyAttributesCount])
        for i in range(count):
            key = prefix + self.PropertyAttributesPrefix + str(i) + "." + self.PropertyKey
            if key not in properties:
                raise Exception("Could not find key for attribute #{}".format(i))
            key_value = properties[key]

            if len(prefix) > 0:
                temp = prefix + key_value + "."
            else:
                temp = key_value + "."

            attribute = ResourcePoolAttributeDescriptor()
            attribute.reset(properties, temp)
            attribute.set_key(key_value)
            self.add_attribute(attribute)
