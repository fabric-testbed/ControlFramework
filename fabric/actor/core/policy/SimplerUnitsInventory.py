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
    from fabric.actor.core.apis.i_client_reservation import IClientReservation

from fabric.actor.core.policy.free_allocated_set import FreeAllocatedSet
from fabric.actor.core.policy.inventory_for_type import InventoryForType


class SimplerUnitsInventory(InventoryForType):
    def __init__(self):
        super().__init__()
        self.set = FreeAllocatedSet()

    def donate(self, source: IClientReservation):
        super().donate(source)
        rset = source.get_resources()
        cset = rset.get_resources()
        ticket = cset.get_ticket()

        for i in range(ticket.get_units()):
            self.set.add_inventory(i)

    def allocate(self, count:int, request: dict, resource: dict = None) -> dict:
        self.set.allocate(count)
        result = {}
        return result

    def allocate_revisit(self, count: int, resource: dict):
        self.set.allocate(count)

    def free(self, count: int, request: dict = None, resource: dict = None) -> dict:
        self.set.free(count)
        result = {}
        return result

    def get_free(self) -> int:
        return self.set.get_free()

    def get_allocated(self) -> int:
        return self.get_allocated()
