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
from fabric.actor.core.common.resource_vector import ResourceVector
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID


class ResourceBin:
    """
    ResourceBin represents a grouping of resources with identical properties.
    Each resource bin consists of one or more physical units with a given resource vector.
    Each resource bin has a unique identifier and a unique parent resource bin.
    """
    def __init__(self, parent_guid: ID = None, physical_units:int = None, vector:ResourceVector = None, term:Term = None):
        self.guid = ID()
        self.parent_guid = parent_guid
        self.physical_units = physical_units
        self.vector = vector
        self.term = term

    def get_guid(self) -> ID:
        return self.guid

    def get_parent_guid(self) -> ID:
        return self.parent_guid

    def get_physical_units(self) -> int:
        return self.physical_units

    def get_vector(self) -> ResourceVector:
        return self.vector

    def get_term(self) -> Term:
        return self.term
