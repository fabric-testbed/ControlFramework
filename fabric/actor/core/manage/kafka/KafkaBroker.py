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

from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.apis.IMgmtBroker import IMgmtBroker
from fabric.actor.core.manage.kafka.KafkaMgmtMessageProcessor import KafkaMgmtMessageProcessor
from fabric.actor.core.manage.kafka.KafkaServerActor import KafkaServerActor
from fabric.actor.core.util.ID import ID
from fabric.message_bus.messages.AuthAvro import AuthAvro
from fabric.message_bus.messages.ClaimResourcesAvro import ClaimResourcesAvro
from fabric.message_bus.messages.ClaimResourcesResponseAvro import ClaimResourcesResponseAvro
from fabric.message_bus.messages.ResultAvro import ResultAvro


class KafkaBroker(KafkaServerActor, IMgmtBroker):
    def __init__(self, guid: ID, kafka_topic: str, auth: AuthAvro, logger,
                 message_processor: KafkaMgmtMessageProcessor):
        super().__init__(guid, kafka_topic, auth, logger, message_processor)

    def claim_resources_slice(self, broker: ID, slice_id: ID, rid: ID) -> ClaimResourcesResponseAvro:

        self.clear_last()
        response = ClaimResourcesResponseAvro()
        response.status = ResultAvro()

        try:
            request = ClaimResourcesAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.broker_id = str(broker)
            request.reservation_id = str(rid)
            request.slice_id = str(slice_id)
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic

            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    return message_wrapper.response
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()

        return response

    def claim_resources(self, broker: ID, rid: ID) -> ClaimResourcesResponseAvro:
        self.clear_last()
        response = ClaimResourcesResponseAvro()
        response.status = ResultAvro()

        try:
            request = ClaimResourcesAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.broker_id = str(broker)
            request.reservation_id = str(rid)
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic

            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    return message_wrapper.response
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()

        return response

