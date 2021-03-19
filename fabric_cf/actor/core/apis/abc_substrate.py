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

from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.core.unit import Unit
    from fabric_cf.actor.core.apis.abc_substrate_database import ABCSubstrateDatabase


class ABCSubstrate(ABCBasePlugin):
    """
    Base class for Substrate Plugin
    """
    @abstractmethod
    def get_substrate_database(self) -> ABCSubstrateDatabase:
        """
        Returns the substrate database.
        @return the substrate database
        """

    @abstractmethod
    def transfer_in(self, *, reservation: ABCReservationMixin, unit: Unit):
        """
        Transfers in a new unit into the substrate. Transfer in involves
        configuring the unit so that it is part of the substrate.
        @param reservation reservation that contains the unit
        @param unit unit to be transferred
        """

    @abstractmethod
    def transfer_out(self, *, reservation: ABCReservationMixin, unit: Unit):
        """
        Transfers an existing unit out of the substrate. Transfer out involves
        unconfiguring the unit, so that it is no longer part of the substrate.
        @param reservation reservation that contains the unit
        @param unit unit to be transferred
        """

    @abstractmethod
    def modify(self, *, reservation: ABCReservationMixin, unit: Unit):
        """
        Modifies an existing unit that is already part of the substrate.
        @param reservation reservation that contains the unit
        @param unit unit to be modified.
        """

    @abstractmethod
    def update_props(self, *, reservation: ABCReservationMixin, unit: Unit):
        """
        Updates only the properties of an existing unit that is already part of the substrate.
        @param reservation reservation that contains the unit
        @param unit unit to be modified.
        """
