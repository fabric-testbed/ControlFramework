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

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import PolicyException
from fabric_cf.actor.core.util.resource_type import ResourceType

if TYPE_CHECKING:
    from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
    from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation


class Inventory:
    def __init__(self):
        self.map = {}

    def contains_type(self, *, resource_type: ResourceType):
        if resource_type is None:
            raise PolicyException(Constants.INVALID_ARGUMENT)

        if resource_type in self.map:
            return True

        return False

    def get(self, *, resource_type: ResourceType) -> InventoryForType:
        if resource_type is None:
            raise PolicyException(Constants.INVALID_ARGUMENT)

        return self.map.get(resource_type, None)

    def remove(self, *, source: ABCClientReservation):
        """
        Removes the inventory derived from the specified source.
        @param source source reservation
        @return true if the inventory was update, false otherwise
        """
        rtype = source.get_type()

        if rtype in self.map:
            inv = self.map[rtype]
            if inv.source == source:
                self.map.pop(rtype)
                return True

        return False

    def get_inventory(self) -> dict:
        return self.map

    def remove_inventory_by_type(self, *, rtype: ResourceType):
        if rtype in self.map:
            self.map.pop(rtype)

    def add_inventory_by_type(self, *, inventory: InventoryForType, rtype: ResourceType):
        if rtype not in self.map:
            self.map[rtype] = inventory
