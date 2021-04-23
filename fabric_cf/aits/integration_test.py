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
import logging
import unittest

from fabric_cf.aits.kafka_processor import KafkaProcessorSingleton
from fabric_cf.aits.manage_helper import ManageHelper


class IntegrationTest(unittest.TestCase):
    logger = logging.getLogger('IntegrationTest')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="actor.log")
    logger.setLevel(logging.INFO)

    am_name = None
    broker_name = None
    orchestrator_name = None
    kafka_topic = None

    def setUp(self) -> None:
        KafkaProcessorSingleton.get().set_logger(logger=self.logger)
        self.am_name = KafkaProcessorSingleton.get().am_name
        self.broker_name = KafkaProcessorSingleton.get().broker_name
        self.orchestrator_name = KafkaProcessorSingleton.get().orchestrator_name
        self.kafka_topic = KafkaProcessorSingleton.get().kafka_topic

    def test_a_claim_resources(self):
        KafkaProcessorSingleton.get().start()
        manage_helper = ManageHelper(logger=self.logger)
        manage_helper.claim_delegations(broker=self.broker_name, am=self.am_name, callback_topic=self.kafka_topic,
                                        did=None, id_token=None)
        am_delegations = manage_helper.do_get_delegations(actor_name=self.am_name, callback_topic=self.kafka_topic,
                                                          did=None, id_token=None)
        broker_delegations = manage_helper.do_get_delegations(actor_name=self.broker_name,
                                                              callback_topic=self.kafka_topic, did=None, id_token=None)
        print(f"am_delegations {am_delegations}")
        print(f"broker_delegations {broker_delegations}")
        KafkaProcessorSingleton.get().stop()
