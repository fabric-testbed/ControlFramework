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
import time
import unittest

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.aits.elements.slice import Slice
from fabric_cf.aits.kafka_processor import KafkaProcessorSingleton
from fabric_cf.aits.manage_helper import ManageHelper
from fabric_cf.aits.orchestrator_helper import OrchestratorHelper, Status
import fim.user as fu


class IntegrationTest(unittest.TestCase):
    VALUE = "value"
    STATUS = "status"
    STATUS_OK = "OK"
    STABLE_OK = "StableOK"
    CLOSING = "Closing"

    HTTP_OK = 200
    HTTP_NOT_FOUND = 404

    TEST_SLICE_NAME = "test-slice-1"
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

    def test_a_list_resources(self):
        oh = OrchestratorHelper()
        response = oh.resources()
        self.assertEqual(self.HTTP_NOT_FOUND, response.status_code)
        self.assertEqual("Resource(s) not found!", response.json())

    def test_b_claim_resources(self):
        KafkaProcessorSingleton.get().start()
        manage_helper = ManageHelper(logger=self.logger)
        manage_helper.claim_delegations(broker=self.broker_name, am=self.am_name, callback_topic=self.kafka_topic)
        am_delegations, status = manage_helper.do_get_delegations(actor_name=self.am_name,
                                                                  callback_topic=self.kafka_topic)
        self.assertEqual(0, status.get_status().code)
        broker_delegations, status = manage_helper.do_get_delegations(actor_name=self.broker_name,
                                                                      callback_topic=self.kafka_topic)
        self.assertEqual(0, status.get_status().code)
        self.assertEqual(len(am_delegations), len(broker_delegations))
        ad = am_delegations[0]
        bd = broker_delegations[0]
        self.assertEqual(ad.delegation_id, bd.delegation_id)
        self.assertEqual(ad.slice.slice_name, bd.slice.slice_name)
        KafkaProcessorSingleton.get().stop()

    def test_c_list_resources(self):
        oh = OrchestratorHelper()
        response = oh.resources()
        self.assertEqual(self.HTTP_OK, response.status_code)
        status = response.json()[self.VALUE][self.STATUS]
        self.assertEqual(status, self.STATUS_OK)
        json_obj = response.json()[self.VALUE]
        self.assertIsNotNone(json_obj)
        self.assertIsNotNone(json_obj.get(Constants.BROKER_QUERY_MODEL, None))

    def build_slice(self, include_components: bool = False, exceed_capacities: bool = False,
                    exceed_components: bool = False) -> str:
        t = fu.ExperimentTopology()
        n1 = t.add_node(name='n1', site='RENC')
        cap = fu.Capacities()
        if exceed_capacities:
            cap.set_fields(core=33, ram=64, disk=500)
        else:
            cap.set_fields(core=4, ram=64, disk=500)
        n1.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')

        n2 = t.add_node(name='n2', site='RENC')
        n2.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')

        if include_components:
            n1.add_component(ctype=fu.ComponentType.SmartNIC, model='ConnectX-6', name='nic1')
            n2.add_component(ctype=fu.ComponentType.NVME, model='P4510', name='c1')
            n2.add_component(ctype=fu.ComponentType.GPU, model='RTX6000', name='c2')
            if exceed_components:
                n2.add_component(ctype=fu.ComponentType.GPU, model='Tesla T4', name='c3')
                n2.add_component(ctype=fu.ComponentType.SmartNIC, model='ConnectX-6', name='nic1')
                n2.add_component(ctype=fu.ComponentType.SmartNIC, model='ConnectX-5', name='nic2')

        return t.serialize()

    def assert_am_broker_reservations(self, slice_id: str, res_id: str, am_res_state: int, broker_res_state: int):
        KafkaProcessorSingleton.get().start()
        manage_helper = ManageHelper(logger=self.logger)
        am_reservation, status = manage_helper.do_get_reservations(actor_name=KafkaProcessorSingleton.get().am_name,
                                                                   callback_topic=KafkaProcessorSingleton.get().kafka_topic,
                                                                   rid=res_id)
        if am_res_state == -1:
            self.assertEqual(8, status.status.code)
        else:
            self.assertEqual(0, status.status.code)
            self.assertEqual(1, len(am_reservation))
            self.assertEqual(slice_id, am_reservation[0].slice_id)
            self.assertEqual(res_id, am_reservation[0].reservation_id)
            self.assertEqual(am_res_state, am_reservation[0].state)

        broker_reservation, status = manage_helper.do_get_reservations(actor_name=KafkaProcessorSingleton.get().broker_name,
                                                                       callback_topic=KafkaProcessorSingleton.get().kafka_topic,
                                                                       rid=res_id)
        self.assertEqual(0, status.status.code)
        self.assertEqual(1, len(broker_reservation))
        self.assertEqual(slice_id, broker_reservation[0].slice_id)
        self.assertEqual(res_id, broker_reservation[0].reservation_id)
        self.assertEqual(broker_res_state, broker_reservation[0].state)

        KafkaProcessorSingleton.get().stop()

    def test_d_create_delete_slice_two_vms_with_components(self):

        # Create Slice
        slice_graph = self.build_slice(include_components=True)
        oh = OrchestratorHelper()
        status, response = oh.create(slice_graph=slice_graph, slice_name=self.TEST_SLICE_NAME)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(response, list))
        self.assertEqual(len(response), 2)
        self.slice_id = response[0].slice_id

        # wait for slice to be Stable
        slice_state = None
        while slice_state != self.STABLE_OK:
            status, slice_obj = oh.slice_status(slice_id=self.slice_id)
            self.assertEqual(Status.OK, status)
            self.assertTrue(isinstance(slice_obj, Slice))
            slice_state = slice_obj.slice_state
            time.sleep(5)

        # check Slivers and verify there states at all 3 actors
        status, slivers = oh.slivers(slice_id=self.slice_id)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(slivers, list))
        for s in slivers:
            self.assertEqual(ReservationStates.Active.name, s.get_state())
            self.assertIsNotNone(s.management_ip)
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)

            self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
                                               am_res_state=ReservationStates.Active.value,
                                               broker_res_state=ReservationStates.Ticketed.value)

        # Delete Slice
        status, response = oh.delete(self.slice_id)
        self.assertEqual(Status.OK, status)

        time.sleep(5)

        # check Slivers and verify there states at all 3 actors
        status, slivers = oh.slivers(slice_id=self.slice_id)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(slivers, list))
        for s in slivers:
            self.assertEqual(ReservationStates.Closed.name, s.get_state())
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)

            self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
                                               am_res_state=ReservationStates.Closed.value,
                                               broker_res_state=ReservationStates.Closed.value)

    def test_e_create_delete_slice_two_vms_no_components(self):
        # Create Slice
        slice_graph = self.build_slice()
        oh = OrchestratorHelper()
        status, response = oh.create(slice_graph=slice_graph, slice_name=self.TEST_SLICE_NAME)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(response, list))
        self.assertEqual(len(response), 2)
        self.slice_id = response[0].slice_id
        
        # Attempt creating the slice again with same name and verify it fails
        status, response = oh.create(slice_graph=slice_graph, slice_name=self.TEST_SLICE_NAME)
        self.assertEqual(Status.FAILURE, status)
        self.assertEqual(f"Slice {self.TEST_SLICE_NAME} already exists", response.json())

        # Wait for Slice to be Stable
        slice_state = None
        while slice_state != self.STABLE_OK:
            status, slice_obj = oh.slice_status(slice_id=self.slice_id)
            self.assertEqual(Status.OK, status)
            self.assertTrue(isinstance(slice_obj, Slice))
            slice_state = slice_obj.slice_state
            time.sleep(5)

        # check Slivers and verify there states at all 3 actors
        status, slivers = oh.slivers(slice_id=self.slice_id)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(slivers, list))
        for s in slivers:
            self.assertEqual(ReservationStates.Active.name, s.get_state())
            self.assertIsNotNone(s.management_ip)
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)

            self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
                                               am_res_state=ReservationStates.Active.value,
                                               broker_res_state=ReservationStates.Ticketed.value)

        # Delete Slice
        status, response = oh.delete(self.slice_id)
        self.assertEqual(Status.OK, status)

        time.sleep(5)

        # check Slivers and verify there states at all 3 actors
        status, slivers = oh.slivers(slice_id=self.slice_id)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(slivers, list))
        for s in slivers:
            self.assertEqual(ReservationStates.Closed.name, s.get_state())
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)

            self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
                                               am_res_state=ReservationStates.Closed.value,
                                               broker_res_state=ReservationStates.Closed.value)

    def test_f_create_delete_slice_with_capacities_exceeding_available_capacities(self):
        # Create Slice
        slice_graph = self.build_slice(exceed_capacities=True)
        oh = OrchestratorHelper()
        status, response = oh.create(slice_graph=slice_graph, slice_name=self.TEST_SLICE_NAME)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(response, list))
        self.assertEqual(len(response), 2)
        self.slice_id = response[0].slice_id

        # Wait for the Slice to be closed
        slice_state = None
        while slice_state != self.CLOSING:
            status, slice_obj = oh.slice_status(slice_id=self.slice_id)
            self.assertEqual(Status.OK, status)
            self.assertTrue(isinstance(slice_obj, Slice))
            slice_state = slice_obj.slice_state
            time.sleep(5)

        # check Slivers and verify there states at all 3 actors
        status, slivers = oh.slivers(slice_id=self.slice_id)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(slivers, list))
        for s in slivers:
            self.assertEqual(s.get_state(), ReservationStates.Closed.name)
            self.assertIsNone(s.management_ip)
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)

            self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
                                               am_res_state=-1,
                                               broker_res_state=ReservationStates.Closed.value)

        # Verify delete slice fails as slices is already closed
        status, response = oh.delete(self.slice_id)
        self.assertEqual(status, Status.FAILURE)
        self.assertEqual(f"Slice# {self.slice_id} already closed", response.json())

    def test_g_create_delete_slice_two_vms_with_components_not_available(self):
        # Create Slice
        slice_graph = self.build_slice(exceed_components=True, include_components=True)
        oh = OrchestratorHelper()
        status, response = oh.create(slice_graph=slice_graph, slice_name=self.TEST_SLICE_NAME)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(response, list))
        self.assertEqual(len(response), 2)
        self.slice_id = response[0].slice_id

        # Wait for slice to be closed
        slice_state = None
        while slice_state != self.CLOSING:
            status, slice_obj = oh.slice_status(slice_id=self.slice_id)
            self.assertEqual(Status.OK, status)
            self.assertTrue(isinstance(slice_obj, Slice))
            slice_state = slice_obj.slice_state
            time.sleep(5)

        # check Slivers and verify there states at all 3 actors
        status, slivers = oh.slivers(slice_id=self.slice_id)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(slivers, list))
        for s in slivers:
            self.assertEqual(s.get_state(), ReservationStates.Closed.name)
            self.assertIsNone(s.management_ip)
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)
            self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
                                               am_res_state=-1,
                                               broker_res_state=ReservationStates.Closed.value)

        # Verify delete slice fails as slices is already closed
        status, response = oh.delete(self.slice_id)
        self.assertEqual(status, Status.FAILURE)
        self.assertEqual(f"Slice# {self.slice_id} already closed", response.json())
