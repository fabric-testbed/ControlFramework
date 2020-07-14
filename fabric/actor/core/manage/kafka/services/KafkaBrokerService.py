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

from fabric.actor.core.manage.kafka.services.KafkaClientActorService import KafkaClientActorService
from fabric.actor.core.manage.kafka.services.KafkaServerActorService import KafkaServerActorService
from fabric.message_bus.messages.message import IMessageAvro


class KafkaBrokerService(KafkaClientActorService, KafkaServerActorService):
    def __init__(self, producer_conf, key_schema, val_schema):
        super().__init__(producer_conf, key_schema, val_schema)

    def process(self, message: IMessageAvro):
        callback_topic = message.get_callback_topic()
        result = None

        self.logger.debug("Processing message: {}".format(message.get_message_name()))

        if message.get_message_name() == IMessageAvro.ClaimResources:
            result = self.claim_resources(message)
        else:
            super().process(message)
            return

        if callback_topic is None:
            self.logger.debug("No callback specified, ignoring the message")

        if self.producer.produce_sync(callback_topic, result):
            self.logger.debug("Successfully send back response: {}".format(result.to_dict()))
        else:
            self.logger.debug("Failed to send back response: {}".format(result.to_dict()))

