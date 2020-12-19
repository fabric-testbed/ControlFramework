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

import traceback
from typing import List

from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
from fabric_mb.message_bus.messages.actor_avro import ActorAvro
from fabric_mb.message_bus.messages.get_actors_request_avro import GetActorsRequestAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro

from fabric_cf.actor.core.apis.i_mgmt_authority import IMgmtAuthority
from fabric_cf.actor.core.apis.i_mgmt_broker import IMgmtBroker
from fabric_cf.actor.core.apis.i_mgmt_controller import IMgmtController
from fabric_cf.actor.core.apis.i_actor import ActorType
from fabric_cf.actor.core.apis.i_component import IComponent
from fabric_cf.actor.core.apis.i_mgmt_actor import IMgmtActor
from fabric_cf.actor.core.apis.i_mgmt_container import IMgmtContainer
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.kafka.kafka_proxy import KafkaProxy
from fabric_cf.actor.core.util.id import ID


class KafkaContainer(KafkaProxy, IMgmtContainer):
    def get_management_object(self, *, key: ID) -> IComponent:
        raise ManageException(Constants.not_implemented)

    def do_get_actors(self, *, type: int) -> List[ActorAvro]:
        self.clear_last()
        status = ResultAvro()
        rret_val = None

        try:
            request = GetActorsRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic
            request.type = type

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug(Constants.management_inter_actor_outbound_message.format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.management_api_timeout_in_seconds)

                if not message_wrapper.done:
                    self.logger.debug(Constants.management_api_timeout_occurred)
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug(Constants.management_inter_actor_inbound_message.format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.actors
            else:
                self.logger.debug(Constants.management_inter_actor_message_failed.format(
                    request.name, self.kafka_topic))
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def get_actor(self, *, guid: ID) -> IMgmtActor:
        raise ManageException(Constants.not_implemented)

    def get_actors(self) -> List[ActorAvro]:
        return self.do_get_actors(type=ActorType.All.value)

    def get_actors_from_database(self) -> List[ActorAvro]:
        raise ManageException(Constants.not_implemented)

    def get_authorities(self) -> List[ActorAvro]:
        return self.do_get_actors(type=ActorType.Authority.value)

    def get_brokers(self) -> List[ActorAvro]:
        return self.do_get_actors(type=ActorType.Broker.value)

    def get_controllers(self) -> List[ActorAvro]:
        return self.do_get_actors(type=ActorType.Orchestrator.value)

    def get_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        raise ManageException(Constants.not_implemented)

    def get_broker_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        raise ManageException(Constants.not_implemented)

    def get_authority_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        raise ManageException(Constants.not_implemented)

    def get_controller(self, *, guid: ID) -> IMgmtController:
        raise ManageException(Constants.not_implemented)

    def get_authority(self, *, guid: ID) -> IMgmtAuthority:
        raise ManageException(Constants.not_implemented)

    def get_broker(self, *, guid: ID) -> IMgmtBroker:
        raise ManageException(Constants.not_implemented)

    def clone(self):
        return KafkaContainer(guid=self.management_id,
                              kafka_topic=self.kafka_topic,
                              auth=self.auth, logger=self.logger,
                              message_processor=self.message_processor,
                              producer=self.producer)
