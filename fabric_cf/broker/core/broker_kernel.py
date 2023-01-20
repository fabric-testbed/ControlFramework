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
from datetime import datetime, timezone

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import BrokerException
from fabric_cf.actor.core.kernel.broker_query_model_publisher import BrokerQueryModelPublisher


class BrokerKernel:
    """
    Class responsible for starting Broker Periodic; also holds Management Actor
    """

    def __init__(self):
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.broker = GlobalsSingleton.get().get_container().get_actor()
        self.producer = GlobalsSingleton.get().get_simple_kafka_producer()
        self.kafka_topic = GlobalsSingleton.get().get_config().get_global_config().get_bqm_config().get(
            Constants.KAFKA_TOPIC, None)
        self.publish_interval = GlobalsSingleton.get().get_config().get_global_config().get_bqm_config().get(
            Constants.PUBLISH_INTERVAL, None)
        self.last_query_time = None

    def do_periodic(self):
        """
        Periodically publish BQM to a Kafka Topic to be consumed by Portal
        """
        if self.kafka_topic is not None and self.publish_interval is not None and self.producer is not None:
            current_time = datetime.now(timezone.utc)
            if self.last_query_time is None or (current_time - self.last_query_time).seconds > self.publish_interval:
                bqm = BrokerQueryModelPublisher(broker=self.broker, logger=self.logger,
                                                kafka_topic=self.kafka_topic, producer=self.producer)
                bqm.execute()
                self.last_query_time = datetime.now(timezone.utc)


class BrokerKernelSingleton:
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise BrokerException(msg="Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = BrokerKernel()
        return self.__instance

    get = classmethod(get)