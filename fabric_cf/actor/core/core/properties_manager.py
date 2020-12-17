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
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.resource_set import ResourceSet


class PropertiesManager:

    @staticmethod
    def get_request_properties(*, rset: ResourceSet, create: bool) -> dict:
        """
        Get Request properties
        @param rset resource set
        @param create if true, request properties are created if not present
        """
        properties = rset.get_request_properties()
        if properties is None and create:
            properties = {}
            rset.set_request_properties(p=properties)
        return properties

    @staticmethod
    def set_elastic_size(*, rset: ResourceSet, value: bool):
        """
        Marks this resource set to indicate that the request that it represents
        is elastic in size (can use less units than requested)
        @param rset resource set
        @param value value
        """
        properties = PropertiesManager.get_request_properties(rset=rset, create=True)
        properties[Constants.elastic_size] = value
        return properties

    @staticmethod
    def is_elastic_size(*, rset: ResourceSet):
        """
        Checks if the request represented by this resource set is elastic in size
        @param rset resource set
        @return true or false
        """
        result = False
        properties = rset.get_request_properties()
        if properties is not None and Constants.elastic_size in properties:
            result = properties[Constants.elastic_size]
        return result

    @staticmethod
    def set_elastic_time(*, rset: ResourceSet, value: bool):
        """
        Marks this resource set to indicate that the request that it represents
        is elastic in size (can use less units than requested)
        @param rset resource set
        @param value value
        """
        properties = PropertiesManager.get_request_properties(rset=rset, create=True)
        properties[Constants.elastic_time] = value
        rset.set_request_properties(p=properties)
        return rset

    @staticmethod
    def is_elastic_time(*, rset: ResourceSet):
        """
        Checks if the request represented by this resource set is elastic in size
        @param rset resource set
        @return true or false
        """
        result = False
        properties = rset.get_request_properties()
        if properties is not None and Constants.elastic_time in properties:
            result = properties[Constants.elastic_time]
        return result
