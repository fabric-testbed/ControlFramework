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


class ResourceType:
    """
    ResourceType is used to name a particular resource type. ResourceType consists of a
    single string describing the particular resource type. ResourceType is a read-only class:
    once created it cannot be modified.
    """
    def __init__(self, *, resource_type: str = None):
        self.resource_type = resource_type

    def get_type(self) -> str:
        """
        Get Type
        :return:
        """
        return self.resource_type

    def __eq__(self, other):
        if not isinstance(other, ResourceType):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.resource_type == other.resource_type

    def __lt__(self, other):
        if not isinstance(other, ResourceType):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.resource_type < other.resource_type

    def __str__(self):
        return self.resource_type

    def __hash__(self):
        return hash(self.resource_type)
