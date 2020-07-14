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

from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.manage.ClientActorManagementObjectHelper import ClientActorManagementObjectHelper
from fabric.actor.core.manage.ProxyProtocolDescriptor import ProxyProtocolDescriptor
from fabric.actor.core.manage.ServerActorManagementObject import ServerActorManagementObject
from fabric.actor.core.apis.IClientActorManagementObject import IClientActorManagementObject

if TYPE_CHECKING:
    from fabric.actor.core.apis.IBroker import IBroker
    from fabric.actor.core.apis.IActor import IActor
    from fabric.message_bus.messages.ResultAvro import ResultAvro
    from fabric.actor.core.manage.messages.ResultProxyMng import ResultProxyMng
    from fabric.actor.security.AuthToken import AuthToken
    from fabric.actor.core.util.ID import ID
    from fabric.actor.core.manage.messages.ProxyMng import ProxyMng
    from fabric.actor.core.manage.messages.ResultPoolInfoMng import ResultPoolInfoMng
    from fabric.actor.core.manage.messages.ResultStringMng import ResultStringMng
    from fabric.message_bus.messages.TicketReservationMng import TicketReservationMng
    from fabric.actor.core.manage.messages.ResultStringsMng import ResultStringsMng
    from fabric.message_bus.messages.ReservationMng import ReservationMng
    from fabric.actor.core.manage.messages.ResultReservationMng import ResultReservationMng
    from fabric.actor.core.util.ResourceType import ResourceType


class BrokerManagementObject(ServerActorManagementObject, IClientActorManagementObject):
    def __init__(self, broker: IBroker = None):
        super().__init__(broker)
        self.client_helper = ClientActorManagementObjectHelper(broker)

    def register_protocols(self):
        from fabric.actor.core.manage.local.LocalBroker import LocalBroker
        local = ProxyProtocolDescriptor(Constants.ProtocolLocal, LocalBroker.__name__, LocalBroker.__module__)

        from fabric.actor.core.manage.kafka.KafkaBroker import KafkaBroker
        kafka = ProxyProtocolDescriptor(Constants.ProtocolKafka, KafkaBroker.__name__, KafkaBroker.__module__)

        self.proxies = []
        self.proxies.append(local)
        self.proxies.append(kafka)

    def save(self) -> dict:
        properties = super().save()
        properties[Constants.PropertyClassName] = BrokerManagementObject.__name__,
        properties[Constants.PropertyModuleName] = BrokerManagementObject.__name__

        return properties

    def set_actor(self, actor: IActor):
        if self.actor is None:
            super().set_actor(actor)
            self.client_helper = ClientActorManagementObjectHelper(actor)

    def get_brokers(self, caller: AuthToken) -> ResultProxyMng:
        return self.client_helper.get_brokers(caller)

    def get_broker(self, broker_id: ID, caller: AuthToken) -> ResultProxyMng:
        return self.client_helper.get_broker(broker_id, caller)

    def add_broker(self, broker_proxy: ProxyMng, caller: AuthToken) -> ResultAvro:
        return self.client_helper.add_broker(broker_proxy, caller)

    def get_pool_info(self, broker: ID, caller: AuthToken) -> ResultPoolInfoMng:
        return self.client_helper.get_pool_info(broker, caller)

    def add_reservation(self, reservation: TicketReservationMng, caller: AuthToken) -> ResultStringMng:
        return self.client_helper.add_reservation(reservation, caller)

    def add_reservations(self, reservations: list, caller: AuthToken) -> ResultStringsMng:
        return self.client_helper.add_reservations(reservations, caller)

    def demand_reservation_rid(self, rid: ID, caller: AuthToken) -> ResultAvro:
        return self.client_helper.demand_reservation_rid(rid, caller)

    def demand_reservation(self, reservation: ReservationMng, caller: AuthToken) -> ResultAvro:
        return self.client_helper.demand_reservation(reservation, caller)

    def claim_resources(self, broker: ID, rid: ID, caller: AuthToken) -> ResultReservationMng:
        return self.client_helper.claim_resources(broker, rid, caller)

    def claim_resources_slice(self, broker: ID, slice_id: ID, rid: ID, caller: AuthToken) -> ResultReservationMng:
        return self.client_helper.claim_resources_slice(broker, slice_id, rid, caller)

    def extend_reservation(self, reservation: id, new_end_time: datetime, new_units: int,
                           new_resource_type: ResourceType, request_properties: dict,
                           config_properties: dict, caller: AuthToken) -> ResultAvro:
        return self.client_helper.extend_reservation(reservation, new_end_time, new_units, new_resource_type,
                                                     request_properties, config_properties, caller)
