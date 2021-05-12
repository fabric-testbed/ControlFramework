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
from datetime import datetime

from confluent_kafka.cimpl import Producer
from fim.graph.abc_property_graph import GraphFormat

from fabric_cf.actor.core.apis.abc_broker_mixin import ABCBrokerMixin
from fabric_cf.actor.core.apis.abc_timer_task import ABCTimerTask
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.broker_policy import BrokerPolicy


class BrokerQueryModelPublisher(ABCTimerTask):
    """
    This class queries Broker for BQM and publishes it to Kafka Topic
    Queries are done periodically based on the configured timer and published to configured kafka topic.
    Published information will be used by the Portal
    """
    def __init__(self, *, broker: ABCBrokerMixin, logger, producer: Producer, kafka_topic: str):
        self.broker = broker
        self.logger = logger
        self.producer = producer
        self.kafka_topic = kafka_topic

    def execute(self):
        """
        Process a claim timeout
        """
        try:
            request = BrokerPolicy.get_broker_query_model_query(level=1, bqm_format=GraphFormat.JSON_NODELINK)
            response = self.broker.query(query=request, caller=self.broker.get_identity())
            if response is None:
                self.logger.error(f"Could not get broker query model!")
                return

            status = response.get(Constants.QUERY_RESPONSE_STATUS, None)
            bqm = response.get(Constants.BROKER_QUERY_MODEL, None)

            if status is None or status == 'False' or bqm is None or bqm == '':
                self.logger.error(f"Could not get broker query model!")
                return

            key = f'{self.broker.get_guid()}-{datetime.utcnow().timestamp()}'

            # Publish to Kafka
            if self.producer is not None and self.kafka_topic is not None:
                self.producer.produce(topic=self.kafka_topic, key=key, value=bqm)
            else:
                self.logger.error(f"Could not get broker query model")
        except Exception as e:
            self.logger.debug(f"Exception occurred in BQM publisher: {e}")
            self.logger.error(traceback.format_exc())

