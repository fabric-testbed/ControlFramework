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

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.aits.kafka_processor import KafkaProcessorSingleton
from fabric_cf.aits.manage_helper import ManageHelper
from fabric_cf.aits.orchestrator_helper import OrchestratorHelper
import fim.user as fu


class IntegrationTest(unittest.TestCase):
    RESERVATIONS = "reservations"
    logger = logging.getLogger('IntegrationTest')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="ait.log")
    logger.setLevel(logging.INFO)

    am_name = None
    broker_name = None
    orchestrator_name = None
    kafka_topic = None
    slice_id = None

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
        self.assertEqual(len(am_delegations), len(broker_delegations))
        ad = am_delegations[0]
        bd = broker_delegations[0]
        self.assertEqual(ad.delegation_id, bd.delegation_id)
        self.assertEqual(ad.slice, bd.slice)
        KafkaProcessorSingleton.get().stop()

    def test_b_list_resources(self):
        oh = OrchestratorHelper()
        response = oh.resources()
        self.assertEqual(response.status_code, 200)
        json_obj = response.json()
        self.assertIsNotNone(json_obj)
        self.assertIsNotNone(json_obj.get(Constants.BROKER_QUERY_MODEL, None))
        self.assertEqual(json_obj.get(Constants.QUERY_RESPONSE_STATUS, None), "True")

    def test_c_create_slice_two_vms_with_components(self):
        t = fu.ExperimentTopology()
        n1 = t.add_node(name='n1', site='RENC')
        cap = fu.Capacities()
        cap.set_fields(core=4, ram=64, disk=500)
        n1.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')
        n1.add_component(ctype=fu.ComponentType.SmartNIC, model='ConnectX-6', name='nic1')

        n2 = t.add_node(name='n2', site='RENC')
        n2.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')
        n2.add_component(ctype=fu.ComponentType.NVME, model='P4510', name='c1')
        n2.add_component(ctype=fu.ComponentType.GPU, model='RTX6000', name='c2')

        slice_graph = t.serialize()
        oh = OrchestratorHelper()
        response = oh.create(slice_graph=slice_graph, slice_name="test-slice-1")
        self.assertEqual(response.status_code, 200)
        json_obj = response.json()
        self.assertIsNotNone(json_obj)
        reservations = json_obj.get(self.RESERVATIONS, None)
        self.assertIsNotNone(reservations)
        self.assertEqual(len(reservations), 2)
        self.slice_id = reservations[0]["slice_id"]

    def test_d_delete_slice(self):
        oh = OrchestratorHelper()
        response = oh.delete(self.slice_id)
        self.assertEqual(response.status_code, 200)
