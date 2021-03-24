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
from typing import TYPE_CHECKING, List

from fabric_cf.actor.core.apis.abc_mgmt_client_actor import ABCMgmtClientActor
from fabric_cf.actor.core.apis.abc_mgmt_actor import ABCMgmtActor

if TYPE_CHECKING:
    from fabric_cf.actor.core.util.id import ID
    from fabric_mb.message_bus.messages.unit_avro import UnitAvro


class ABCMgmtControllerMixin(ABCMgmtActor, ABCMgmtClientActor):
    """
    Interface for Management Interface for a Controller
    """
    @abstractmethod
    def get_reservation_units(self, *, rid: ID, id_token: str = None) -> List[UnitAvro]:
        """
        Return reservation units
        @params rid: reservation is
        @param id_token: id token
        @returns list of units
        """

    @abstractmethod
    def modify_reservation(self, *, rid: ID, modify_properties: dict) -> bool:
        """
        Modify a reservation
        @params rid: reservation id
        @params modify_properties: modify_properties
        @returns true for success and false for failure
        """
