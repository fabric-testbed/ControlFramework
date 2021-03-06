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
from typing import TYPE_CHECKING, List, Tuple, Dict

from fim.slivers.base_sliver import BaseSliver


if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_actor import IActor
    from fabric_cf.actor.core.apis.i_reservation import IReservation


class InventoryForType:
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
    def allocate(self, *, reservation: IReservation, actor: IActor, graph_node: BaseSliver,
                 reservation_info: List[IReservation]) -> Tuple[str, BaseSliver]:
        """
        Allocate the sliver. This method is called for new ticketing/extending reservations.
        @param reservation reservation
        @param actor actor
        @param graph_node graph_node
        @param reservation_info reservation_info
        @return delegation id
        """
