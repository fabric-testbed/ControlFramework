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

from typing import List

from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
from fabric_mb.message_bus.messages.actor_avro import ActorAvro
from fabric_mb.message_bus.messages.get_actors_request_avro import GetActorsRequestAvro

from fabric_cf.actor.core.apis.abc_mgmt_authority import ABCMgmtAuthority
from fabric_cf.actor.core.apis.abc_mgmt_broker_mixin import ABCMgmtBrokerMixin
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.apis.abc_component import ABCComponent
from fabric_cf.actor.core.apis.abc_mgmt_actor import ABCMgmtActor
from fabric_cf.actor.core.apis.abc_mgmt_container import ABCMgmtContainer
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.kafka.kafka_proxy import KafkaProxy
from fabric_cf.actor.core.util.id import ID


class KafkaContainer(KafkaProxy, ABCMgmtContainer):
    def get_management_object(self, *, key: ID) -> ABCComponent:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def do_get_actors(self, *, actor_type: int) -> List[ActorAvro]:
        request = GetActorsRequestAvro()
        request = self.fill_request_by_id_message(request=request)
        request.type = actor_type
        status, response = self.send_request(request)

        if status.code == 0:
            return response.actors
        return None

    def get_actor(self, *, guid: ID) -> ABCMgmtActor:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_actors(self) -> List[ActorAvro]:
        return self.do_get_actors(actor_type=ActorType.All.value)

    def get_actors_from_database(self) -> List[ActorAvro]:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_authorities(self) -> List[ActorAvro]:
        return self.do_get_actors(actor_type=ActorType.Authority.value)

    def get_brokers(self) -> List[ActorAvro]:
        return self.do_get_actors(actor_type=ActorType.Broker.value)

    def get_controllers(self) -> List[ActorAvro]:
        return self.do_get_actors(actor_type=ActorType.Orchestrator.value)

    def get_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_broker_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_authority_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_controller(self, *, guid: ID) -> ABCMgmtControllerMixin:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_authority(self, *, guid: ID) -> ABCMgmtAuthority:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_broker(self, *, guid: ID) -> ABCMgmtBrokerMixin:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def clone(self):
        return KafkaContainer(guid=self.management_id,
                              kafka_topic=self.kafka_topic,
                              auth=self.auth, logger=self.logger,
                              message_processor=self.message_processor,
                              producer=self.producer)
