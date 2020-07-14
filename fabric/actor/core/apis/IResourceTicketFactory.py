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
if TYPE_CHECKING:
    from fabric.actor.core.apis.IActor import IActor
    from fabric.actor.core.common.ResourceVector import ResourceVector
    from fabric.actor.core.delegation.ResourceDelegation import ResourceDelegation
    from fabric.actor.core.delegation.ResourceTicket import ResourceTicket
    from fabric.actor.core.time.Term import Term
    from fabric.actor.core.util.ID import ID
    from fabric.actor.core.util.ResourceType import ResourceType


class IResourceTicketFactory:
    def set_actor(self, actor: IActor):
        """
        Sets the actor this factory belongs to.
        @param actor actor
        """
        raise NotImplementedError("Should have implemented this")

    def get_actor(self) -> IActor:
        """
        Returns the actor represented by this factory.
        @return actor
        """
        raise NotImplementedError("Should have implemented this")

    def initialize(self):
        """
        Initializes the factory.
        @throws Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def make_delegation(self, units: int, vector: ResourceVector, term: Term, rtype: ResourceType, sources: list = None,
                        bins: list = None, properties: dict = None, holder: ID = None) -> ResourceDelegation:
        """
        Creates a new ResourceDelegation
        @param units number of units
        @param vector resource vector
        @param term term
        @param rtype resource type
        @param sources source bins used for the delegation
        @param bins bins references by the delegation
        @param properties properties list
        @param holder identifier of the holder of the delegation
        @return ResourceDelegation
        @throws Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def make_ticket(self, source: ResourceTicket, delegation: ResourceDelegation) -> ResourceTicket:
        """
        Creates a new ticket from the specified source tickets.
        Note: all sources must share the same root delegation.
        @param source source
        @param delegation delegation
        @return ResourceTicket
        @throws TicketException in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def clone(self, original: ResourceTicket) -> ResourceTicket:
        """
        Makes a deep clone of the specified resource ticket.
        @param original original
        @return ResourceTicket
        """
        raise NotImplementedError("Should have implemented this")

    def toJson(self, ticket: ResourceTicket) -> str:
        """
        Converts the resource ticket to JSON.
        @param ticket ticket to convert
        @return JSON string representation
        """
        raise NotImplementedError("Should have implemented this")

    def fromJson(self, json_string: str) -> ResourceTicket:
        """
        Obtains a resource ticket from JSON.
        @param json_string JSON string representation
        @return resource ticket
        """
        raise NotImplementedError("Should have implemented this")