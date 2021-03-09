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

from abc import abstractmethod
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_actor import IActor
    from fabric_cf.actor.core.delegation.resource_delegation import ResourceDelegation
    from fabric_cf.actor.core.time.term import Term
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.util.resource_type import ResourceType


class IResourceDelegationFactory:
    """
    Interface for Factory class for Resource Tickets
    """
    @abstractmethod
    def set_actor(self, *, actor: IActor):
        """
        Sets the actor this factory belongs to.
        @param actor actor
        """

    @abstractmethod
    def get_actor(self) -> IActor:
        """
        Returns the actor represented by this factory.
        @return actor
        """

    @abstractmethod
    def initialize(self):
        """
        Initializes the factory.
        @throws Exception in case of error
        """

    @abstractmethod
    def make_delegation(self, *, units: int, term: Term, rtype: ResourceType, properties: dict = None,
                        holder: ID = None) -> ResourceDelegation:
        """
        Creates a new ResourceDelegation
        @param units number of units
        @param term term
        @param rtype resource type
        @param properties properties list
        @param holder identifier of the holder of the delegation
        @return ResourceDelegation
        @throws Exception in case of error
        """
