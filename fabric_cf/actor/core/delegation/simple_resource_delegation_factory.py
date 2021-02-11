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

from fabric_cf.actor.core.apis.i_actor import IActor
from fabric_cf.actor.core.common.exceptions import ResourcesException
from fabric_cf.actor.core.apis.i_resource_ticket_factory import IResourceTicketFactory
from fabric_cf.actor.core.delegation.resource_delegation import ResourceDelegation
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType


class SimpleResourceTicketFactory(IResourceTicketFactory):
    """
    Factory class to create Resource Tickets
    """
    def __init__(self):
        self.actor = None
        self.initialized = False

    def initialize(self):
        """
        Initialize
        """
        if not self.initialized:
            if self.actor is None:
                raise ResourcesException("Factory does not have an actor")
            self.initialized = True

    def ensure_initialized(self):
        """
        Ensure if the factory is initialized
        """

        if not self.initialized:
            raise ResourcesException("ticket factory has not been initialized")

    def get_issuer_id(self):
        """
        Get Actor Guid
        """
        return self.actor.get_identity().get_guid()

    def make_delegation(self, *, units: int = None, term: Term = None, rtype: ResourceType = None,
                        properties: dict = None, holder: ID = None) -> ResourceDelegation:
        self.ensure_initialized()
        issuer = self.get_issuer_id()

        return ResourceDelegation(units=units, term=term, rtype=rtype, properties=properties, issuer=issuer,
                                  holder=holder)

    def set_actor(self, *, actor: IActor):
        self.actor = actor

    def get_actor(self) -> IActor:
        return self.actor
