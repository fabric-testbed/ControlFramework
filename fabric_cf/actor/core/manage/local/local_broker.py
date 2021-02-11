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
from typing import TYPE_CHECKING, List

from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.pool_info_avro import PoolInfoAvro

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.broker_management_object import BrokerManagementObject
from fabric_cf.actor.core.apis.i_mgmt_broker import IMgmtBroker
from fabric_cf.actor.core.manage.local.local_server_actor import LocalServerActor
from fabric_cf.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
    from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
    from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
    from fabric_cf.actor.core.util.resource_type import ResourceType
    from fabric_cf.actor.core.manage.management_object import ManagementObject
    from fabric_cf.actor.security.auth_token import AuthToken


class LocalBroker(LocalServerActor, IMgmtBroker):
    def __init__(self, *, manager: ManagementObject, auth: AuthToken):
        super().__init__(manager=manager, auth=auth)
        if not isinstance(manager, BrokerManagementObject):
            raise ManageException("Invalid manager object. Required: {}".format(type(BrokerManagementObject)))

    def add_broker(self, *, broker: ProxyAvro) -> bool:
        self.clear_last()
        try:
            result = self.manager.add_broker(broker=broker, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def get_brokers(self, *, broker: ID = None, id_token: str = None) -> List[ProxyAvro]:
        self.clear_last()
        try:
            result = self.manager.get_brokers(caller=self.auth, broker_id=broker)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.proxies
        except Exception as e:
            self.last_exception = e

        return None

    def get_pool_info(self, *, broker: ID, id_token: str) -> List[PoolInfoAvro]:
        self.clear_last()
        try:
            result = self.manager.get_pool_info(broker=broker, caller=self.auth, id_token=id_token)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.pools
        except Exception as e:
            self.last_exception = e

        return None

    def claim_delegations(self, *, broker: ID, did: str, id_token: str = None) -> DelegationAvro:
        self.clear_last()
        try:
            result = self.manager.claim_delegations(broker=broker, did=did, caller=self.auth, id_token=id_token)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result_list=result.delegations)
        except Exception as e:
            self.last_exception = e

        return None

    def reclaim_delegations(self, *, broker: ID, did: str, id_token: str = None) -> DelegationAvro:
        self.clear_last()
        try:
            result = self.manager.reclaim_delegations(broker=broker, did=did, caller=self.auth, id_token=id_token)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result_list=result.delegations)
        except Exception as e:
            self.last_exception = e

        return None

    def add_reservation(self, *, reservation: TicketReservationAvro) -> ID:
        self.clear_last()
        try:
            result = self.manager.add_reservation(reservation=reservation, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return ID(uid=result.get_result())
        except Exception as e:
            self.last_exception = e

        return None

    def add_reservations(self, *, reservations: List[TicketReservationAvro]) -> List[ID]:
        self.clear_last()
        try:
            result = self.manager.add_reservations(reservations=reservations, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                rids = []
                for r in result.result:
                    rids.append(ID(uid=r))

                return rids
        except Exception as e:
            self.last_exception = e

        return None

    def demand_reservation_rid(self, *, rid: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.demand_reservation_rid(rid=rid, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def demand_reservation(self, *, reservation: ReservationMng) -> bool:
        self.clear_last()
        try:
            result = self.manager.demand_reservation(reservation=reservation, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def extend_reservation(self, *, reservation: id, new_end_time: datetime, new_units: int,
                           new_resource_type: ResourceType = None, request_properties: dict = None,
                           config_properties: dict = None) -> bool:
        self.clear_last()
        try:
            result = self.manager.extend_reservation(reservation=reservation, new_end_time=new_end_time,
                                                     new_units=new_units, new_resource_type=new_resource_type,
                                                     request_properties=request_properties,
                                                     config_properties=config_properties, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def extend_reservation_end_time(self, *, reservation: id, new_end_time: datetime) -> bool:
        return self.extend_reservation(reservation=reservation, new_end_time=new_end_time,
                                       new_units=Constants.EXTEND_SAME_UNITS)

    def extend_reservation_end_time_request(self, *, reservation: id, new_end_time: datetime,
                                            request_properties: dict) -> bool:
        return self.extend_reservation(reservation=reservation, new_end_time=new_end_time,
                                       new_units=Constants.EXTEND_SAME_UNITS,
                                       request_properties=request_properties)

    def extend_reservation_end_time_request_config(self, *, reservation: id, new_end_time: datetime,
                                                   request_properties: dict, config_properties: dict) -> bool:
        return self.extend_reservation(reservation=reservation, new_end_time=new_end_time,
                                       new_units=Constants.EXTEND_SAME_UNITS,
                                       request_properties=request_properties,
                                       config_properties=config_properties)
