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
from typing import TYPE_CHECKING, List, Tuple

from fim.slivers.base_sliver import BaseSliver

from fabric_cf.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin


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
    def allocate(self, *, rid: ID, requested_sliver: BaseSliver, graph_id: str, graph_node: BaseSliver,
                 existing_reservations: List[ABCReservationMixin]) -> Tuple[str, BaseSliver]:
        """
        Allocate an extending or ticketing reservation
        :param rid: reservation to be allocated
        :param requested_sliver: requested sliver
        :param graph_id: BQM graph id
        :param graph_node: BQM graph node identified to serve the reservation
        :param existing_reservations: Existing Reservations served by the same BQM node
        :return: Tuple of Delegation Id and the Requested Sliver annotated with BQM Node Id and other properties
        :raises: BrokerException in case the request cannot be satisfied
        """
