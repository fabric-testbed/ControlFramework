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
from fabric.actor.core.util.resource_type import ResourceType


class ResourceDelegation:
    """
    ResourceDelegation expresses a delegation of a number of resources with
    identical properties from one actor to another. Each delegation is backed by resources
    stored in one or more resource bins and covers a fixed time interval (term). Individual units
    in a delegation are indistinguishable from each other.
    """
    def __init__(self, *, units: int = None, vector: ResourceVector = None, term: Term = None,
                 rtype: ResourceType = None, sources: list = None, bins: list = None, properties: dict = None,
                 issuer : ID = None, holder: ID = None):
        # The delegation's unique identifier.
        self.guid = ID()
        # Lease interval.
        self.term = term
        # Number of units delegated. This is the total number of
        # units represented by the delegation and can be different
        # from the number of physical units
        self.units = units
        # Resource vector for each delegated unit.
        self.vector = vector
        # Resource type.
        self.type = rtype
        # Resource properties (optional).
        self.properties = properties
        # Issuer identifier.
        self.issuer = issuer
        # Holder identifier.
        self.holder = holder
        # Source bins used for this delegation.
        self.sources = sources
        # Actor-local resource bins referenced by the delegation.
        self.bins = bins

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.vector = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['vector']
        return state

    def __str__(self):
        result = "delegation=[guid={},units={},vector={},type={},issuer={},holder={}]".format(self.guid, self.units,
                                                                                              self.vector, self.type,
                                                                                              self.issuer, self.holder)
        return result

    def is_valid(self) -> bool:
        if self.guid is None or self.term is None or self.units <= 0 or self.vector is None or \
                self.vector.is_positive() == False or self.type is None or self.sources is None \
                or len(self.sources) == 0 or self.bins is None or len(self.bins) == 0:
            return False
        return True

    def get_guid(self) -> ID:
        return self.guid

    def get_resource_vector(self) -> ResourceVector:
        return self.vector

    def get_resource_type(self) -> ResourceType:
        return self.type

    def get_properties(self) -> dict:
        return self.properties

    def get_term(self) -> Term:
        return self.term

    def get_units(self) -> int:
        return self.units

    def get_issuer(self) -> ID:
        return self.issuer

    def get_holder(self) -> ID:
        return self.holder

    def set_issuer(self, *, id: ID):
        self.issuer = id

    def set_holder(self, *, id: ID):
        self.holder = id

    def set_units(self, *, units: int):
        self.units = units

    def get_map(self):
        """
        Returns a hash map of all actor-local bins referenced by this delegation.
        The map indexes bins by guid.
        @return hash map
        """
        result = {}
        for i in range(len(self.bins)):
            result[self.bins[i].guid] = self.bins[i]
        return result
