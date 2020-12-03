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

from fabric.actor.core.common.exceptions import PolicyException

if TYPE_CHECKING:
    from fabric.actor.core.util.resource_type import ResourceType
    from fabric.actor.core.apis.i_client_reservation import IClientReservation
    from fabric.actor.core.common.resource_pool_descriptor import ResourcePoolDescriptor


class InventoryForType:
    def __init__(self):
        self.resource_type = None
        self.properties = {}
        self.source = None
        self.resource_pool_descriptor = None

    def get_type(self) -> ResourceType:
        return self.resource_type

    def set_type(self, *, rtype: ResourceType):
        self.resource_type = rtype

    def donate(self, *, source: IClientReservation):
        if self.source is not None:
            raise PolicyException("This inventory pool already has a source.")
        self.source = source

    def get_source(self) -> IClientReservation:
        return self.source

    def set_descriptor(self, *, rpd: ResourcePoolDescriptor):
        self.resource_pool_descriptor = rpd

    def get_descriptor(self) -> ResourcePoolDescriptor:
        return self.resource_pool_descriptor

    def get_properties(self) -> dict:
        return self.properties

    @abstractmethod
    def allocate(self, *, count: int, request: dict, resource: dict = None) -> dict:
        """
        Allocates the specified number of units given the client request
        properties. This method is called for new ticketing/extending reservations.
        @param count how many units to allocate
        @param request request properties
        @param resource what is currently allocated
        @return the resource properties to be passed back to the client
        """

    @abstractmethod
    def allocate_revisit(self, *, count: int, resource: dict):
        """
        Called during revisit to indicate that a ticketed reservation is being
        recovered.
        @param count number of units
        @param resource resource properties
        """

    @abstractmethod
    def free(self, *, count: int, request: dict = None, resource: dict = None) -> dict:
        """
        Frees the specified number of resource units.
        @param count number of units
        @param request request properties
        @param resource resource properties
        @return new resource properties
        """

    @abstractmethod
    def get_free(self) -> int:
        """
        Returns the number of free units in the inventory pool.
        @return number of free units
        """

    @abstractmethod
    def get_allocated(self) -> int:
        """
        Returns the number of allocated units from this invento
        @return number of allocated units
        """
