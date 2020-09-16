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


from typing import TYPE_CHECKING

from fabric.actor.core.common.constants import Constants

if TYPE_CHECKING:
    from fabric.actor.core.kernel.resource_set import ResourceSet


class PropList:
    @staticmethod
    def merge_properties(incoming: dict, outgoing: dict):
        if incoming is None:
            return outgoing

        if outgoing is None:
            return incoming

        if incoming == outgoing:
            return incoming

        if incoming is not None and outgoing is not None:
            return {**incoming, **outgoing}

    @staticmethod
    def is_elastic_time(rset: ResourceSet):
        properties = rset.get_request_properties()
        if properties is not None and Constants.ElasticTime in properties:
            value = properties[Constants.ElasticTime]
            if value.lower() == 'true':
                return True

        return False

    @staticmethod
    def is_elastic_size(rset: ResourceSet):
        properties = rset.get_request_properties()
        if properties is not None and Constants.ElasticSize in properties:
            value = properties[Constants.ElasticSize]
            if value.lower() == 'true':
                return True

        return False