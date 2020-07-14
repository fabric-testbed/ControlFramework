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
from fabric.actor.core.apis.IMgmtServerActor import IMgmtServerActor
from fabric.actor.core.manage.kafka.KafkaActor import KafkaActor
from fabric.actor.core.manage.kafka.KafkaMgmtMessageProcessor import KafkaMgmtMessageProcessor
from fabric.actor.core.util.ID import ID
from fabric.message_bus.messages.AuthAvro import AuthAvro


class KafkaServerActor(KafkaActor, IMgmtServerActor):
    def __init__(self, guid: ID, kafka_topic: str, auth: AuthAvro, kafka_config: dict, logger,
                 message_processor: KafkaMgmtMessageProcessor):
        super().__init__(guid, kafka_topic, auth, kafka_config, logger, message_processor)