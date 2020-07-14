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
from fabric.actor.core.kernel.ResourceSet import ResourceSet


class PropertiesManager:
    ElasticSize = "request.elasticSize"
    ElasticTime = "request.elasticTime"

    @staticmethod
    def get_request_properties(rset: ResourceSet, create: bool) -> dict:
        properties = rset.get_request_properties()
        if properties is None and create:
            properties = {}
            rset.set_request_properties(properties)
        return properties

    @staticmethod
    def set_elastic_size(rset: ResourceSet, value: bool):
        properties = PropertiesManager.get_request_properties(rset, True)
        properties[PropertiesManager.ElasticSize] = value
        return properties

    @staticmethod
    def is_elastic_size(rset: ResourceSet):
        result = False
        properties = rset.get_request_properties()
        if properties is not None and PropertiesManager.ElasticSize in properties:
            result = properties[PropertiesManager.ElasticSize]
        return result

    @staticmethod
    def set_elastic_time(rset: ResourceSet, value: bool):
        properties = PropertiesManager.get_request_properties(rset, True)
        properties[PropertiesManager.ElasticTime] = value
        rset.set_request_properties(properties)
        return rset

    @staticmethod
    def is_elastic_time(rset: ResourceSet):
        result = False
        properties = rset.get_request_properties()
        if properties is not None and PropertiesManager.ElasticTime in properties:
            result = properties[PropertiesManager.ElasticTime]
        return result
