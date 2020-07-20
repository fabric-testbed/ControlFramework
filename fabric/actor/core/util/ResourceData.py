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

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __str__(self):
        return "local: {}, request: {}, resource: {}, config: {}".format(self.local_properties, self.request_properties,
                                                                         self.resource_properties,
                                                                         self.configuration_properties)

    def clone(self):
        object = ResourceData()
        object.local_properties = self.local_properties
        object.request_properties = self.request_properties
        object.resource_properties = self.resource_properties
        object.configuration_properties = self.configuration_properties
        return object

    def get_local_properties(self) -> dict:
        return self.local_properties

    def get_request_properties(self) -> dict:
        return self.request_properties

    def get_resource_properties(self) -> dict:
        return self.resource_properties

    def get_configuration_properties(self) -> dict:
        return self.configuration_properties

    def merge(self, other):
        if other is None or not isinstance(other, ResourceData):
            raise Exception("Invalid object type")

        self.local_properties = self.merge_properties(other.local_properties, self.local_properties)
        self.request_properties = self.merge_properties(other.request_properties, self.request_properties)
        self.resource_properties = self.merge_properties(other.resource_properties, self.resource_properties)
        self.configuration_properties = self.merge_properties(other.configuration_properties, self.configuration_properties)

    @staticmethod
    def merge_properties(from_props: dict, to_props: dict) -> dict:
        """
        Merges both properties lists. Elements in from overwrite elements in two.

        @params from_props : from list
        @params to_props : to list
        """
        if from_props is None:
            return to_props

        if to_props is None:
            return from_props

        if from_props == to_props:
            return from_props

        result = {**from_props, **to_props}
        return result