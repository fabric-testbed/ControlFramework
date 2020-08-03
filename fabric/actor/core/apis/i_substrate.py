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

from fabric.actor.core.apis.i_base_plugin import IBasePlugin

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_reservation import IReservation
    from fabric.actor.core.core.unit import Unit
    from fabric.actor.core.apis.i_substrate_database import ISubstrateDatabase


class ISubstrate(IBasePlugin):
    def get_substrate_database(self) -> ISubstrateDatabase:
        """
        Returns the substrate database.
        @return the substrate database
        """
        raise NotImplementedError("Should have implemented this")

    def transfer_in(self, reservation: IReservation, unit: Unit):
        """
        Transfers in a new unit into the substrate. Transfer in involves
        configuring the unit so that it is part of the substrate.
        @param reservation reservation that contains the unit
        @param unit unit to be transferred
        """
        raise NotImplementedError("Should have implemented this")

    def transfer_out(self, reservation: IReservation, unit: Unit):
        """
        Transfers an existing unit out of the substrate. Transfer out involves
        unconfiguring the unit, so that it is no longer part of the substrate.
        @param reservation reservation that contains the unit
        @param unit unit to be transferred
        """
        raise NotImplementedError("Should have implemented this")

    def modify(self, reservation: IReservation, unit: Unit):
        """
        Modifies an existing unit that is already part of the substrate.
        @param reservation reservation that contains the unit
        @param unit unit to be modified.
        """
        raise NotImplementedError("Should have implemented this")

    def update_props(self, reservation: IReservation, unit: Unit):
        """
        Updates only the properties of an existing unit that is already part of the substrate.
        @param reservation reservation that contains the unit
        @param unit unit to be modified.
        """
        raise NotImplementedError("Should have implemented this")
