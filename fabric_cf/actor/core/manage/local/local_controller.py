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

from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.broker_query_model_avro import BrokerQueryModelAvro
from fabric_mb.message_bus.messages.poa_avro import PoaAvro
from fabric_mb.message_bus.messages.reservation_predecessor_avro import ReservationPredecessorAvro
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric_mb.message_bus.messages.unit_avro import UnitAvro
from fim.slivers.base_sliver import BaseSliver
from fim.user import GraphFormat

from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.controller_management_object import ControllerManagementObject
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.manage.local.local_actor import LocalActor
from fabric_cf.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
    from fabric_mb.message_bus.messages.reservation_mng import ReservationMng

    from fabric_cf.actor.core.apis.abc_client_actor_management_object import ABCClientActorManagementObject
    from fabric_cf.actor.security.auth_token import AuthToken


class LocalController(LocalActor, ABCMgmtControllerMixin):
    def __init__(self, *, manager: ABCClientActorManagementObject, auth: AuthToken):
        super().__init__(manager=manager, auth=auth)

        if not isinstance(manager, ControllerManagementObject):
            raise ManageException("Invalid manager object. Required: {}".format(type(ControllerManagementObject)))

    def add_broker(self, *, broker: ProxyAvro) -> bool:
        self.clear_last()
        try:
            result = self.manager.add_broker(broker=broker, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def update_broker(self, *, broker: ProxyAvro) -> bool:
        self.clear_last()
        try:
            result = self.manager.update_broker(broker=broker, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def get_brokers(self, *, broker: ID = None, id_token: str = None) -> List[ProxyAvro]:
        self.clear_last()
        try:
            result = self.manager.get_brokers(caller=self.auth, broker_id=broker)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.proxies
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

    def get_broker_query_model(self, *, broker: ID, id_token: str, level: int, graph_format: GraphFormat,
                               start: datetime = None, end: datetime = None,
                               includes: str = None, excludes: str = None) -> BrokerQueryModelAvro:
        self.clear_last()
        try:
            result = self.manager.get_broker_query_model(broker=broker, caller=self.auth, id_token=id_token,
                                                         level=level, graph_format=graph_format,
                                                         start=start, end=end, excludes=excludes, includes=includes)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.model
        except Exception as e:
            print(e)
            self.on_exception(e=e, traceback_str=traceback.format_exc())

    def build_broker_query_model(self, level_0_broker_query_model: str, level: int,
                                 graph_format: GraphFormat = GraphFormat.GRAPHML,
                                 start: datetime = None, end: datetime = None,
                                 includes: str = None, excludes: str = None) -> str:
        self.clear_last()
        try:
            return self.manager.build_broker_query_model(level_0_broker_query_model=level_0_broker_query_model,
                                                         level=level, graph_format=graph_format, start=start,
                                                         end=end, includes=includes, excludes=excludes)
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

    def claim_delegations(self, *, broker: ID, did: ID, id_token: str = None) -> DelegationAvro:
        self.clear_last()
        try:
            result = self.manager.claim_delegations(broker=broker, did=did, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result_list=result.delegations)
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

    def reclaim_delegations(self, *, broker: ID, did: ID, id_token: str = None) -> DelegationAvro:
        self.clear_last()
        try:
            result = self.manager.reclaim_delegations(broker=broker, did=did, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return self.get_first(result_list=result.delegations)
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

    def get_reservation_units(self, *, rid: ID, id_token: str = None) -> List[UnitAvro]:
        self.clear_last()
        try:
            result = self.manager.get_reservation_units(caller=self.auth, rid=rid)
            self.last_status = result.status
            if result.status.get_code() == 0:
                return result.units
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

    def add_reservation(self, *, reservation: TicketReservationAvro) -> ID:
        self.clear_last()
        try:
            result = self.manager.add_reservation(reservation=reservation, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return ID(uid=result.get_result())
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

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
            self.on_exception(e=e, traceback_str=traceback.format_exc())

    def demand_reservation_rid(self, *, rid: ID) -> bool:
        self.clear_last()
        try:
            result = self.manager.demand_reservation_rid(rid=rid, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def demand_reservation(self, *, reservation: ReservationMng) -> bool:
        self.clear_last()
        try:
            result = self.manager.demand_reservation(reservation=reservation, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def extend_reservation(self, *, reservation: ID, new_end_time: datetime, sliver: BaseSliver,
                           dependencies: List[ReservationPredecessorAvro] = None) -> bool:
        self.clear_last()
        try:
            result = self.manager.extend_reservation(reservation=reservation,
                                                     new_end_time=new_end_time, sliver=sliver,
                                                     caller=self.auth, dependencies=dependencies)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def clone(self):
        return LocalController(manager=self.manager, auth=self.auth)

    def modify_reservation(self, *, rid: ID, modified_sliver: BaseSliver) -> bool:
        self.clear_last()
        try:
            result = self.manager.modify_reservation(rid=rid, modified_sliver=modified_sliver, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False

    def poa(self, *, poa: PoaAvro) -> bool:
        self.clear_last()
        try:
            result = self.manager.poa(poa=poa, caller=self.auth)
            self.last_status = result

            return result.get_code() == 0
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())

        return False
