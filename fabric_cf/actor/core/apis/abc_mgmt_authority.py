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

from fabric_cf.actor.core.apis.abc_mgmt_server_actor import ABCMgmtServerActor

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.unit_avro import UnitAvro
    from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
    from fabric_cf.actor.core.util.id import ID


class ABCMgmtAuthority(ABCMgmtServerActor):
    """
    Interface for Management Authority
    """
    @abstractmethod
    def get_authority_reservations(self, *, id_token: str = None) -> List[ReservationMng]:
        """
        Retrieves all leases the site has issued to service managers.
        @return returns list of reservations
        """

    @abstractmethod
    def get_reservation_units(self, *, rid: ID, id_token: str = None) -> List[UnitAvro]:
        """
        Retrieves all units in the specified reservation
        @param rid reservation id
        @param id_token id token
        @return returns list of units for specific reservation
        """

    @abstractmethod
    def get_reservation_unit(self, *, uid: ID, id_token: str = None) -> UnitAvro:
        """
        Returns the specified inventory item
        @param uid unit id
        @param id_token id token
        @return return the specified inventory item
        """
