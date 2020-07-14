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
from enum import Enum


class ResourcePoolAttributeType(Enum):
    UNDEFINED = 1
    INTEGER = 2
    FLOAT = 3
    STRING = 4
    NEO4J = 5
    CLASS = 6


class ResourcePoolAttributeDescriptor:
    PropertyType = "type"
    PropertyUnit = "unit"
    PropertyValue = "value"
    PropertyLabel = "label"
    PropertyUnit = "unit"

    def __init__(self):
        self.key = None
        self.type = None
        self.value = None
        self.label = None
        self.unit = None

    def get_type(self) -> ResourcePoolAttributeType:
        return self.type

    def set_type(self, rtype: ResourcePoolAttributeType):
        self.type = rtype

    def get_value(self) -> str:
        return self.value

    def get_int_value(self) -> int:
        return int(self.value)

    def set_value(self, value: str):
        self.value = value

    def get_key(self) -> str:
        return self.key

    def set_key(self, value: str):
        self.key = value

    def set_label(self, label: str):
        self.label = label

    def get_label(self) -> str:
        return self.label

    def get_unit(self) -> str:
        return self.unit

    def set_unit(self, unit: str):
        self.unit = unit

    def __str__(self):
        return "{}:{}:{}".format(self.key, self.value, self.type)

    def save(self, properties: dict, prefix: str) -> dict:
        if prefix is None:
            prefix = ""

        properties[prefix + self.PropertyType] = str(self.type.value)
        if self.value is not None:
            properties[prefix + self.PropertyValue] = self.value

        if self.label is not None:
            properties[prefix + self.PropertyLabel] = self.label

        if self.unit is not None:
            properties[prefix + self.PropertyUnit] = self.unit

        return properties

    def reset(self, properties: dict, prefix: str):
        if prefix is None:
            prefix = ""

        self.type = ResourcePoolAttributeType(int(properties[prefix + self.PropertyType]))
        self.value = properties[prefix + self.PropertyValue]
        if prefix + self.PropertyLabel in properties:
            self.label = properties[prefix + self.PropertyLabel]
        if prefix + self.PropertyUnit in properties:
            self.unit = properties[prefix + self.PropertyUnit]
