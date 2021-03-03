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
from typing import Dict

from fim.slivers.base_sliver import BaseSliver

from fabric_cf.actor.core.common.exceptions import ResourcesException


class ResourceData:
    """
    A ResourceData contains several collections of properties describing resources.
    Some of these collections are passed between actors during calls.
    """
    def __init__(self):
        self.local_properties = {}
        self.request_properties = {}
        self.resource_properties = {}
        self.configuration_properties = {}
        self.sliver = None
        self.comp_node_ids = {}

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __str__(self):
        result = f"local: {self.local_properties}, request: {self.request_properties}, " \
                 f"resource: {self.resource_properties}, config: {self.configuration_properties} " \
                 f"sliver: {self.sliver}"
        if self.sliver is not None:
            result += f" cbm_node_id: {self.sliver.cbm_node_id}"
        return result

    def clone(self):
        """
        Clone an object
        """
        obj = ResourceData()
        obj.local_properties = self.local_properties.copy()
        obj.request_properties = self.request_properties.copy()
        obj.resource_properties = self.resource_properties.copy()
        obj.configuration_properties = self.configuration_properties.copy()
        obj.sliver = self.sliver
        obj.comp_node_ids = self.comp_node_ids.copy()
        return obj

    def get_local_properties(self) -> dict:
        """
        Get local properties
        @return local properties
        """
        return self.local_properties

    def get_request_properties(self) -> dict:
        """
        Get Request properties
        @return request properties
        """
        return self.request_properties

    def get_resource_properties(self) -> dict:
        """
        Get resource properties
        @return resource properties
        """
        return self.resource_properties

    def get_configuration_properties(self) -> dict:
        """
        Get handlers properties
        @return handlers properties
        """
        return self.configuration_properties

    def get_sliver(self) -> BaseSliver:
        return self.sliver

    def get_components_node_ids(self) -> Dict[str, str]:
        return self.comp_node_ids

    def merge(self, *, other):
        """
        Merge with other instance of ResourceData
        @param other other
        """
        if other is None or not isinstance(other, ResourceData):
            raise ResourcesException("Invalid object type")

        self.local_properties = self.merge_properties(from_props=other.local_properties,
                                                      to_props=self.local_properties)

        self.request_properties = self.merge_properties(from_props=other.request_properties,
                                                        to_props=self.request_properties)

        self.resource_properties = self.merge_properties(from_props=other.resource_properties,
                                                         to_props=self.resource_properties)

        self.configuration_properties = self.merge_properties(from_props=other.configuration_properties,
                                                              to_props=self.configuration_properties)

    @staticmethod
    def merge_properties(*, from_props: dict, to_props: dict) -> dict:
        """
        Merges both properties lists. Elements in from overwrite elements in two.

        @params from_props : from list
        @params to_props : to list
        """
        if from_props is None:
            from_props = {}

        if to_props is None:
            to_props = {}

        if from_props == to_props:
            return from_props

        result = {**from_props, **to_props}
        return result

    def set_sliver(self, *, sliver: BaseSliver):
        self.sliver = sliver

    def set_sliver_node_id(self, *, node_id: str):
        if self.sliver is not None:
            self.sliver.cbm_node_id = node_id

    def get_sliver_node_id(self) -> str:
        if self.sliver is not None:
            return self.sliver.cbm_node_id
        return self.sliver

    def set_components_node_ids(self, *, comp_node_ids: Dict[str, str]):
        self.comp_node_ids = comp_node_ids