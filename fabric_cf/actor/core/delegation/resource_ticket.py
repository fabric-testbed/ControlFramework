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
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType


class ResourceTicket:
    """
    ResourceDelegation expresses a delegation of a number of resources with
    identical properties from one actor to another. Each delegation is backed by resources
    stored in one or more resource bins and covers a fixed time interval (term). Individual units
    in a delegation are indistinguishable from each other.
    """
    def __init__(self, *, units: int = None, term: Term = None, rtype: ResourceType = None,
                 properties: dict = None, issuer: ID = None, holder: ID = None):
        # The delegation's unique identifier.
        self.guid = ID()
        # Lease interval.
        self.term = term
        # Number of units delegated. This is the total number of
        # units represented by the delegation and can be different
        # from the number of physical units
        self.units = units
        # Resource type.
        self.type = rtype
        # Resource properties (optional).
        self.properties = properties
        # Issuer identifier.
        self.issuer = issuer
        # Holder identifier.
        self.holder = holder

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __getstate__(self):
        state = self.__dict__.copy()
        return state

    def __str__(self):
        result = f"delegation=[guid={self.guid},units={self.units},type={self.type},issuer={self.issuer}," \
                 f"holder={self.holder}]"
        return result

    def is_valid(self) -> bool:
        if self.guid is None or self.term is None or self.units <= 0 or self.type is None:
            return False
        return True

    def get_guid(self) -> ID:
        """
        Get guid
        @return guid
        """
        return self.guid

    def get_resource_type(self) -> ResourceType:
        """
        Get Resource Type
        @return resource type
        """
        return self.type

    def get_properties(self) -> dict:
        """
        Get Properties
        @return properties
        """
        return self.properties

    def get_term(self) -> Term:
        """
        Get Term
        @return term
        """
        return self.term

    def get_units(self) -> int:
        """
        Get Units
        @return units
        """
        return self.units

    def get_issuer(self) -> ID:
        """
        Get Issuer
        @return issuer
        """
        return self.issuer

    def get_holder(self) -> ID:
        """
        Get holder
        @return holder
        """
        return self.holder

    def set_issuer(self, *, guid: ID):
        """
        Set issuer
        @param guid issuer guid
        """
        self.issuer = guid

    def set_holder(self, *, guid: ID):
        """
        Set holder
        @param guid holder guid
        """
        self.holder = guid

    def set_units(self, *, units: int):
        """
        Set units
        @param units units
        """
        self.units = units

    def clone(self):
        properties = None
        if self.properties is not None:
            properties = self.properties.copy()
        obj = ResourceTicket(units=self.units, term=self.term, rtype=self.type, properties=properties,
                             holder=self.holder, issuer=self.issuer)
        obj.guid = self.guid

        return obj


class ResourceTicketFactory:
    @staticmethod
    def create(*, issuer: ID, units: int = None, term: Term = None, rtype: ResourceType = None,
               properties: dict = None, holder: ID = None) -> ResourceTicket:
        """
        Create Resource Delegation
        :param issuer: Issuer Actor Guid
        :param units: Number of units
        :param term: term
        :param rtype: resource type
        :param properties: properties
        :param holder: Actor issuing the ticket
        :return: Resource Delegation Object
        """
        return ResourceTicket(units=units, term=term, rtype=rtype, properties=properties, issuer=issuer,
                              holder=holder)
