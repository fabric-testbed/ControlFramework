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

import traceback
from datetime import datetime
from typing import TYPE_CHECKING, List

from fabric.actor.core.common.constants import Constants
from fabric.actor.core.manage.controller_management_object import ControllerManagementObject
from fabric.actor.core.apis.i_mgmt_controller import IMgmtController
from fabric.actor.core.manage.local.local_actor import LocalActor
from fabric.message_bus.messages.delegation_avro import DelegationAvro
from fabric.message_bus.messages.pool_info_avro import PoolInfoAvro
from fabric.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric.actor.core.util.id import ID
from fabric.message_bus.messages.unit_avro import UnitAvro

if TYPE_CHECKING:
    from fabric.actor.core.manage.management_object import ManagementObject
    from fabric.actor.security.auth_token import AuthToken
    from fabric.message_bus.messages.proxy_avro import ProxyAvro
    from fabric.message_bus.messages.reservation_mng import ReservationMng
    from fabric.actor.core.util.resource_type import ResourceType


class LocalController(LocalActor, IMgmtController):
    def __init__(self, *, manager: ManagementObject, auth: AuthToken):
        super().__init__(manager=manager, auth=auth)

        if not isinstance(manager, ControllerManagementObject):
            raise Exception("Invalid manager object. Required: {}".format(type(ControllerManagementObject)))

    def add_broker(self, *, broker: ProxyAvro) -> bool:
        self.clear_last()
        try:
            result = self.manager.add_broker(broker=broker, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False

    def get_brokers(self) -> List[ProxyAvro]:
        self.clear_last()
        try:
            result = self.manager.get_brokers(caller=self.auth)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.proxies
        except Exception as e:
            self.last_exception = e
            traceback.print_exc()

        return None

    def get_broker(self, *, broker: ID) -> ProxyAvro:
        self.clear_last()
        try:
            result = self.manager.get_broker(broker=broker, caller=self.auth)
            self.last_status = result.status

            if result.get_status().get_code() == 0:
                return self.get_first(result_list=result.proxies)
        except Exception as e:
            self.last_exception = e

        return None

    def get_pool_info(self, *, broker: ID) -> List[PoolInfoAvro]:
        self.clear_last()
        try:
            result = self.manager.get_pool_info(broker=broker, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.pools
        except Exception as e:
            self.last_exception = e

        return None

    def claim_resources_slice(self, *, broker: ID, slice_id: ID, rid: ID) -> ReservationMng:
        self.clear_last()
        try:
            result = self.manager.claim_resources_slice(broker=broker, slice_id=slice_id, rid=rid, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result_list=result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def claim_resources(self, *, broker: ID, rid: ID) -> ReservationMng:
        self.clear_last()
        try:
            result = self.manager.claim_resources(broker=broker, rid=rid, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result_list=result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def claim_delegations(self, *, broker: ID, did: str) -> DelegationAvro:
        self.clear_last()
        try:
            result = self.manager.claim_delegations(broker=broker, did=did, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result_list=result.result)
        except Exception as e:
            self.last_exception = e

        return None

    def get_reservation_units(self, *, rid: ID) -> List[UnitAvro]:
        self.clear_last()
        try:
            result = self.manager.get_reservation_units(caller=self.auth, rid=rid)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.result
        except Exception as e:
            self.last_exception = e

        return None

    def add_reservation(self, *, reservation: TicketReservationAvro) -> ID:
        self.clear_last()
        try:
            result = self.manager.add_reservation(reservation=reservation, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return ID(id=result.result)
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
                    rids.append(ID(id=r))

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

    def extend_reservation_end_time(self, *, reservation: ID, new_end_time: datetime) -> bool:
        return self.extend_reservation(reservation=reservation, new_end_time=new_end_time,
                                       new_units=Constants.ExtendSameUnits, new_resource_type=None,
                                       request_properties=None, config_properties=None)

    def extend_reservation_end_time_request(self, *, reservation: ID, new_end_time: datetime,
                                            request_properties: dict) -> bool:
        return self.extend_reservation(reservation=reservation, new_end_time=new_end_time,
                                       new_units=Constants.ExtendSameUnits, new_resource_type=None,
                                       request_properties=request_properties, config_properties=None)

    def extend_reservation_end_time_request_config(self, *, reservation: ID, new_end_time: datetime,
                                                   request_properties: dict, config_properties: dict) -> bool:
        return self.extend_reservation(reservation=reservation, new_end_time=new_end_time,
                                       new_units=Constants.ExtendSameUnits, new_resource_type=None,
                                       request_properties=request_properties, config_properties=config_properties)

    def clone(self):
        return LocalController(manager=self.manager, auth=self.auth)

    def modify_reservation(self, *, rid: ID, modify_properties: dict) -> bool:
        self.clear_last()
        try:
            result = self.manager.modify_reservation(rid=rid, modify_properties=modify_properties, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.last_exception = e

        return False
