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

from fabric.actor.core.apis.i_actor import ActorType
from fabric.actor.core.apis.i_mgmt_container import IMgmtContainer
from fabric.actor.core.common.constants import Constants, ErrorCodes
from fabric.actor.core.manage.kafka.kafka_mgmt_message_processor import KafkaMgmtMessageProcessor
from fabric.actor.core.manage.kafka.kafka_proxy import KafkaProxy
from fabric.actor.core.util.id import ID
from fabric.message_bus.messages.actor_avro import ActorAvro
from fabric.message_bus.messages.auth_avro import AuthAvro
from fabric.message_bus.messages.get_actors_avro import GetActorsAvro
from fabric.message_bus.messages.result_actor_avro import ResultActorAvro
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.producer import AvroProducerApi


class KafkaContainer(KafkaProxy, IMgmtContainer):
    def __init__(self, *, guid: ID, kafka_topic: str, auth: AuthAvro, logger,
                 message_processor: KafkaMgmtMessageProcessor, producer: AvroProducerApi = None):
        super().__init__(guid=guid, kafka_topic=kafka_topic, auth=auth, logger=logger,
                         message_processor=message_processor, producer=producer)

    def do_get_actors(self, *, type: int) -> List[ActorAvro]:
        self.clear_last()
        status = ResultAvro()
        rret_val = None

        try:
            request = GetActorsAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic
            request.type = type

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.actors
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def get_actors(self) -> List[ActorAvro]:
        return self.do_get_actors(type=ActorType.All.value)

    def get_authorities(self) -> List[ActorAvro]:
        return self.do_get_actors(type=ActorType.Authority.value)

    def get_brokers(self) -> List[ActorAvro]:
        return self.do_get_actors(type=ActorType.Broker.value)

    def get_controllers(self) -> List[ActorAvro]:
        return self.do_get_actors(type=ActorType.Orchestrator.value)