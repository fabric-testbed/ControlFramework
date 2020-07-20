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

from datetime import datetime
from typing import TYPE_CHECKING

from fabric.actor.core.manage.messages.ResultStringsMng import ResultStringsMng
from fabric.message_bus.messages.ResultAvro import ResultAvro

if TYPE_CHECKING:
    from fabric.message_bus.messages.TicketReservationAvro import TicketReservationMng
    from fabric.actor.security.AuthToken import AuthToken
    from fabric.actor.core.manage.messages.ResultStringMng import ResultStringMng
    from fabric.actor.core.util.ResourceType import ResourceType
    from fabric.message_bus.messages.ReservationMng import ReservationMng
    from fabric.actor.core.util.ID import ID
    from fabric.actor.core.manage.messages.ResultReservationMng import ResultReservationMng
    from fabric.actor.core.manage.messages.ProxyMng import ProxyMng
    from fabric.actor.core.manage.messages.ResultProxyMng import ResultProxyMng
    from fabric.actor.core.manage.messages.ResultPoolInfoMng import ResultPoolInfoMng


class IClientActorManagementObject:
    def add_reservation(self, reservation: TicketReservationMng, caller: AuthToken) -> ResultStringMng:
        raise NotImplementedError

    def add_reservations(self, reservations: list, caller: AuthToken) -> ResultStringsMng:
        raise NotImplementedError

    def extend_reservation(self, reservation: id, new_end_time: datetime, new_units: int,
                            new_resource_type: ResourceType, request_properties: dict,
                            config_properties: dict, caller: AuthToken) -> ResultAvro:
        raise NotImplementedError

    def demand_reservation(self, reservation: ReservationMng, caller: AuthToken) -> ResultStringMng:
        raise NotImplementedError

    def demand_reservation_rid(self, rid: ID, caller: AuthToken) -> ResultAvro:
        raise NotImplementedError

    def get_brokers(self, caller: AuthToken) -> ResultProxyMng:
        raise NotImplementedError

    def get_broker(self, broker_id: ID, caller: AuthToken) -> ResultProxyMng:
        raise NotImplementedError

    def add_broker(self, broker_proxy: ProxyMng, caller: AuthToken) -> ResultAvro:
        raise NotImplementedError

    def claim_resources_slice(self, broker: ID, slice_id: ID, rid: ID, caller: AuthToken) -> ResultReservationMng:
        raise NotImplementedError

    def claim_resources(self, broker: ID, rid: ID, caller: AuthToken) -> ResultReservationMng:
        raise NotImplementedError

    def get_pool_info(self, broker: ID, caller: AuthToken) -> ResultPoolInfoMng:
        raise NotImplementedError