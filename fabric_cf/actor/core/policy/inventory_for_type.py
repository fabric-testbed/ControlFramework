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

from fim.slivers.base_sliver import BaseSliver

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin


class InventoryForType:
    def __init__(self):
        self.logger = None

    def set_logger(self, *, logger):
        self.logger = logger

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None

    @abstractmethod
    def free(self, *, count: int, request: dict = None, resource: dict = None) -> dict:
        """
        Frees the specified number of resource units.
        @param count number of units
        @param request request properties
        @param resource resource properties
        @return new resource properties
        """

    @staticmethod
    def _get_allocated_sliver(reservation: ABCReservationMixin) -> BaseSliver:
        """
        Retrieve the allocated sliver from the reservation.

        :param reservation: An instance of ABCReservationMixin representing the reservation to retrieve the sliver from.
        :return: The allocated NetworkServiceSliver if available, otherwise None.
        """
        if reservation.is_ticketing() and reservation.get_approved_resources() is not None:
            return reservation.get_approved_resources().get_sliver()
        if (reservation.is_active() or reservation.is_ticketed()) and reservation.get_resources() is not None:
            return reservation.get_resources().get_sliver()
