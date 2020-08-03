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

from fabric.actor.core.common.constants import Constants
from fabric.actor.core.manage.broker_management_object import BrokerManagementObject
from fabric.actor.core.apis.i_mgmt_broker import IMgmtBroker
from fabric.actor.core.manage.local.local_server_actor import LocalServerActor
from fabric.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric.actor.core.manage.management_object import ManagementObject
    from fabric.actor.security.auth_token import AuthToken
    from fabric.actor.core.manage.messages.proxy_mng import ProxyMng
    from fabric.message_bus.messages.reservation_mng import ReservationMng
    from fabric.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
    from fabric.actor.core.util.resource_type import ResourceType


class LocalBroker(LocalServerActor, IMgmtBroker):
    def __init__(self, manager: ManagementObject = None, auth: AuthToken = None):
        super().__init__(manager, auth)
        if not isinstance(manager, BrokerManagementObject):
            raise Exception("Invalid manager object. Required: {}".format(type(BrokerManagementObject)))

    def add_broker(self, broker: ProxyMng) -> bool:
        self.clear_last()
        try:
            result = self.manager.add_broker(broker, self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def get_brokers(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_brokers(self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def get_broker(self, broker: ID) -> ProxyMng:
        self.clear_last()
        try:
            result = self.manager.get_broker(broker, self.auth)
            self.last_status = result.status

            if result.get_status().get_code() == 0:
                return self.get_first(result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def get_pool_info(self, broker: ID) -> list:
        self.clear_last()
        try:
            result = self.manager.get_pool_info(broker, self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def claim_resources_slice(self, broker: ID, slice_id: ID, rid: ID) -> ReservationMng:
        self.clear_last()
        try:
            result = self.manager.claim_resources_slice(broker, slice_id, rid, self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def claim_resources(self, broker: ID, rid: ID) -> ReservationMng:
        self.clear_last()
        try:
            result = self.manager.claim_resources(broker, rid, self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def add_reservation(self, reservation: TicketReservationAvro) -> ID:
        self.clear_last()
        try:
            result = self.manager.add_reservation(reservation, self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return ID(result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def add_reservations(self, reservations: list) -> list:
        self.clear_last()
        try:
            result = self.manager.add_reservations(reservations, self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                rids = []
                for r in result.result:
                    rids.append(ID(r))

                return rids
        except Exception as e:
            self.last_exception = e

        return None

    def demand_reservation_rid(self, rid: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.demand_reservation_rid(rid, self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def demand_reservation(self, reservation: ReservationMng) -> bool:
        self.clear_last()
        try:
            result = self.manager.demand_reservation(reservation, self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def extend_reservation(self, reservation: id, new_end_time: datetime, new_units: int,
                           new_resource_type: ResourceType, request_properties: dict,
                           config_properties: dict) -> bool:
        self.clear_last()
        try:
            result = self.manager.extend_reservation(reservation, new_end_time, new_units, new_resource_type,
                                                     request_properties, config_properties, self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def extend_reservation_end_time(self, reservation: id, new_end_time: datetime) -> bool:
        return self.extend_reservation(reservation, new_end_time, Constants.ExtendSameUnits, None, None, None)

    def extend_reservation_end_time_request(self, reservation: id, new_end_time: datetime,
                                            request_properties: dict) -> bool:
        return self.extend_reservation(reservation, new_end_time, Constants.ExtendSameUnits, None, request_properties,
                                       None)

    def extend_reservation_end_time_request_config(self, reservation: id, new_end_time: datetime,
                                                   request_properties: dict, config_properties: dict) -> bool:
        return self.extend_reservation(reservation, new_end_time, Constants.ExtendSameUnits, None, request_properties,
                                       config_properties)
