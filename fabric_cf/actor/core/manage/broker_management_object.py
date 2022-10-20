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

from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
from fabric_mb.message_bus.messages.reservation_predecessor_avro import ReservationPredecessorAvro
from fabric_mb.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fim.slivers.base_sliver import BaseSliver
from fim.user import GraphFormat

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.manage.client_actor_management_object_helper import ClientActorManagementObjectHelper
from fabric_cf.actor.core.manage.proxy_protocol_descriptor import ProxyProtocolDescriptor
from fabric_cf.actor.core.manage.server_actor_management_object import ServerActorManagementObject
from fabric_cf.actor.core.apis.abc_client_actor_management_object import ABCClientActorManagementObject

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.result_broker_query_model_avro import ResultBrokerQueryModelAvro
    from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
    from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
    from fabric_mb.message_bus.messages.result_strings_avro import ResultStringsAvro
    from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
    from fabric_mb.message_bus.messages.result_avro import ResultAvro
    from fabric_mb.message_bus.messages.result_proxy_avro import ResultProxyAvro

    from fabric_cf.actor.core.apis.abc_broker_mixin import ABCBrokerMixin
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.util.id import ID


class BrokerManagementObject(ServerActorManagementObject, ABCClientActorManagementObject):
    def __init__(self, *, broker: ABCBrokerMixin = None):
        super().__init__(sa=broker)
        self.client_helper = ClientActorManagementObjectHelper(client=broker)

    def register_protocols(self):
        from fabric_cf.actor.core.manage.local.local_broker import LocalBroker
        local = ProxyProtocolDescriptor(protocol=Constants.PROTOCOL_LOCAL,
                                        proxy_class=LocalBroker.__name__,
                                        proxy_module=LocalBroker.__module__)

        from fabric_cf.actor.core.manage.kafka.kafka_broker import KafkaBroker
        kafka = ProxyProtocolDescriptor(protocol=Constants.PROTOCOL_KAFKA,
                                        proxy_class=KafkaBroker.__name__,
                                        proxy_module=KafkaBroker.__module__)

        self.proxies = []
        self.proxies.append(local)
        self.proxies.append(kafka)

    def save(self) -> dict:
        properties = super().save()
        properties[Constants.PROPERTY_CLASS_NAME] = BrokerManagementObject.__name__
        properties[Constants.PROPERTY_MODULE_NAME] = BrokerManagementObject.__module__

        return properties

    def set_actor(self, *, actor: ABCActorMixin):
        if self.actor is None:
            super().set_actor(actor=actor)
            self.client_helper = ClientActorManagementObjectHelper(client=actor)

    def get_brokers(self, *, caller: AuthToken, broker_id: ID = None) -> ResultProxyAvro:
        return self.client_helper.get_brokers(caller=caller, broker_id=broker_id)

    def add_broker(self, *, broker: ProxyAvro, caller: AuthToken) -> ResultAvro:
        return self.client_helper.add_broker(broker=broker, caller=caller)

    def get_broker_query_model(self, *, broker: ID, caller: AuthToken, id_token: str,
                               level: int, graph_format: GraphFormat) -> ResultBrokerQueryModelAvro:
        return self.client_helper.get_broker_query_model(broker=broker, caller=caller, id_token=id_token, level=level,
                                                         graph_format=graph_format)

    def add_reservation(self, *, reservation: TicketReservationAvro, caller: AuthToken) -> ResultStringAvro:
        return self.client_helper.add_reservation(reservation=reservation, caller=caller)

    def add_reservations(self, *, reservations: List[TicketReservationAvro], caller: AuthToken) -> ResultStringsAvro:
        return self.client_helper.add_reservations(reservations=reservations, caller=caller)

    def demand_reservation_rid(self, *, rid: ID, caller: AuthToken) -> ResultAvro:
        return self.client_helper.demand_reservation_rid(rid=rid, caller=caller)

    def demand_reservation(self, *, reservation: ReservationMng, caller: AuthToken) -> ResultAvro:
        return self.client_helper.demand_reservation(reservation=reservation, caller=caller)

    def extend_reservation(self, *, reservation: id, new_end_time: datetime, sliver: BaseSliver,
                           caller: AuthToken, dependencies: List[ReservationPredecessorAvro] = None) -> ResultAvro:
        return self.client_helper.extend_reservation(reservation=reservation, new_end_time=new_end_time,
                                                     sliver=sliver, caller=caller, dependencies=dependencies)

    def claim_delegations(self, *, broker: ID, did: str, caller: AuthToken) -> ResultDelegationAvro:
        return self.client_helper.claim_delegations(broker=broker, did=did, caller=caller)

    def reclaim_delegations(self, *, broker: ID, did: str, caller: AuthToken) -> ResultDelegationAvro:
        return self.client_helper.reclaim_delegations(broker=broker, did=did, caller=caller)