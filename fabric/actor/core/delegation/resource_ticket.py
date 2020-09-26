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
from fabric.actor.core.apis.i_resource_ticket_factory import IResourceTicketFactory
from fabric.actor.core.delegation.resource_delegation import ResourceDelegation
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_type import ResourceType


class ResourceTicket:
    """
    ResourceTicket represents a sequence of one or more delegations of resources from one party to another.
    """

    def __init__(self, *, factory: IResourceTicketFactory, delegation: ResourceDelegation, source):
        """
        Creates a root ticket from the specified delegation.
        @params delegation: root delegation
        @params factory: factory
        """
        if factory is None:
            raise Exception("factory Null")
        if delegation is None:
            raise Exception("delegation Null")

        self.factory = factory
        # All delegation records represented by this ticket.
        self.delegations = []
        if delegation is not None:
            self.delegations.append(delegation)

        if source is not None:
            if not isinstance(source, ResourceTicket):
                raise Exception("Invalid argument")

            for d in source.delegations:
                self.delegations.append(d)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['factory']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.factory = None

    def __str__(self):
        result = "factory: {}".format(self.factory)
        for d in self.delegations:
            result += " {}".format(d)
        return result

    def is_valid(self) -> bool:
        if self.delegations is None or len(self.delegations) == 0:
            return False

        for i in range(len(self.delegations)):
            if not self.delegations[i].is_valid():
                return False

        return True

    def get_delegation(self) -> ResourceDelegation:
        return self.delegations[0]

    def get_resource_type(self) -> ResourceType:
        return self.get_delegation().get_resource_type()

    def get_properties(self) -> dict:
        return self.get_delegation().get_properties()

    def get_issuer(self) -> ID:
        return self.get_delegation().get_issuer()

    def get_holder(self) -> ID:
        return self.get_delegation().get_holder()

    def get_guid(self) -> ID:
        return self.get_delegation().get_guid()

    def get_term(self) -> Term:
        return self.get_delegation().get_term()

    def get_units(self) -> int:
        return self.get_delegation().get_units()

    def get_resource_vector(self) -> ResourceVector:
        return self.get_delegation().get_resource_vector()

    def get_factory(self) -> IResourceTicketFactory:
        return self.factory

    def set_factory(self, *, factory: IResourceTicketFactory):
        self.factory = factory
