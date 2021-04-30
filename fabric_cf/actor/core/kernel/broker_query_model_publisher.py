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

from fabric_mb.message_bus.producer import AvroProducerApi

from fabric_cf.actor.core.apis.abc_mgmt_broker_mixin import ABCMgmtBrokerMixin
from fabric_cf.actor.core.apis.abc_timer_task import ABCTimerTask


class BrokerQueryModelPublisher(ABCTimerTask):
    """
    This class queries Broker for BQM and publishes it to Kafka Topic
    Queries are done periodically based on the configured timer and published to configured kafka topic.
    Published information will be used by the Portal
    """
    def __init__(self, *, broker: ABCMgmtBrokerMixin, logger, producer: AvroProducerApi, kafka_topic: str):
        self.broker = broker
        self.logger = logger
        self.producer = producer
        self.kafka_topic = kafka_topic

    def execute(self):
        """
        Process a claim timeout
        """
        models = self.broker.get_broker_query_model(broker=self.broker.get_guid(), level=1, id_token=None)
        if models is None or len(models) != 1:
            self.logger.error(f"Could not get broker query model: {self.broker.get_last_error()}")
            return

        for m in models:
            try:
                if m.get_model() is not None and m.get_model() != '':
                    # Publish to Kafka
                    if self.producer is not None and self.kafka_topic is not None:
                        self.producer.produce_sync(topic=self.kafka_topic, record=m)
                else:
                    self.logger.error(f"Could not get broker query model")
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.debug(f"Could not process get_broker_query_model response {e}")
