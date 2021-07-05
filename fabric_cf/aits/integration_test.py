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
from datetime import datetime, timedelta
from http.client import OK, NOT_FOUND, BAD_REQUEST

from fabric_cf.actor.core.apis.abc_delegation import DelegationState
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.aits.elements.slice import Slice
from fabric_cf.aits.kafka_processor import KafkaProcessorSingleton
from fabric_cf.aits.manage_helper import ManageHelper
from fabric_cf.aits.orchestrator_helper import OrchestratorHelper, Status
import fim.user as fu

from fabric_cf.orchestrator.core.exceptions import OrchestratorException


class IntegrationTest(unittest.TestCase):
    VALUE = "value"
    STATUS = "status"
    STATUS_OK = "OK"
    STABLE_OK = "StableOK"
    CLOSING = "Closing"

    TIME_FORMAT_IN_SECONDS = "%Y-%m-%d %H:%M"

    TEST_SLICE_NAME = "test-slice-1"
    logger = logging.getLogger('IntegrationTest')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="ait.log")
    logger.setLevel(logging.INFO)

    am_name = None
    net_am_name = None
    broker_name = None
    orchestrator_name = None
    kafka_topic = None
    slice_id = None

    def setUp(self) -> None:
        KafkaProcessorSingleton.get().set_logger(logger=self.logger)
        self.net_am_name = KafkaProcessorSingleton.get().net_am_name
        self.am_name = KafkaProcessorSingleton.get().am_name
        self.broker_name = KafkaProcessorSingleton.get().broker_name
        self.orchestrator_name = KafkaProcessorSingleton.get().orchestrator_name
        self.kafka_topic = KafkaProcessorSingleton.get().kafka_topic

    def test_a_list_resources(self):
        oh = OrchestratorHelper()
        response = oh.resources()
        self.assertEqual(NOT_FOUND, response.status_code)
        self.assertEqual("Resource(s) not found!", response.content)

        response = oh.portal_resources()
        self.assertEqual(NOT_FOUND, response.status_code)
        self.assertEqual("Resource(s) not found!", response.content)

    def test_b1_reclaim_resources(self):
        KafkaProcessorSingleton.get().start()
        manage_helper = ManageHelper(logger=self.logger)

        # Get AM Delegations
        am_delegations, status = manage_helper.do_get_delegations(actor_name=self.am_name,
                                                                  callback_topic=self.kafka_topic)
        self.assertEqual(0, status.get_status().code)
        self.assertIsNotNone(am_delegations)

        # Reclaim Delegations and verify it fails
        reclaim_status = manage_helper.reclaim_delegations(broker=self.broker_name, am=self.am_name,
                                                           callback_topic=self.kafka_topic)
        self.assertEqual(False, reclaim_status)

        # Get Broker Delegations
        broker_delegations, status = manage_helper.do_get_delegations(actor_name=self.broker_name,
                                                                      callback_topic=self.kafka_topic)

        self.assertEqual(0, status.get_status().code)
        self.assertIsNone(broker_delegations)

        KafkaProcessorSingleton.get().stop()

    def test_b2_claim_resources(self):
        KafkaProcessorSingleton.get().start()
        manage_helper = ManageHelper(logger=self.logger)

        # Claim Delegations
        manage_helper.claim_delegations(broker=self.broker_name, am=self.am_name, callback_topic=self.kafka_topic)

        # Get AM Delegations
        am_delegations, status = manage_helper.do_get_delegations(actor_name=self.am_name,
                                                                  callback_topic=self.kafka_topic)
        self.assertEqual(0, status.get_status().code)
        self.assertIsNotNone(am_delegations)

        # Get Broker Delegations
        broker_delegations, status = manage_helper.do_get_delegations(actor_name=self.broker_name,
                                                                      callback_topic=self.kafka_topic)
        self.assertEqual(0, status.get_status().code)
        self.assertIsNotNone(broker_delegations)

        # Verify AM and Broker Delegations
        self.assertEqual(len(am_delegations), len(broker_delegations))
        ad = am_delegations[0]
        bd = broker_delegations[0]
        self.assertEqual(ad.delegation_id, bd.delegation_id)
        self.assertEqual(ad.slice.slice_name, bd.slice.slice_name)

        KafkaProcessorSingleton.get().stop()

    def test_b3_claim_resources(self):
        KafkaProcessorSingleton.get().start()
        manage_helper = ManageHelper(logger=self.logger)

        # Claim Delegations
        manage_helper.claim_delegations(broker=self.broker_name, am=self.net_am_name, callback_topic=self.kafka_topic)

        # Get AM Delegations
        am_delegations, status = manage_helper.do_get_delegations(actor_name=self.net_am_name,
                                                                  callback_topic=self.kafka_topic)
        self.assertEqual(0, status.get_status().code)
        self.assertIsNotNone(am_delegations)

        # Get Broker Delegations
        broker_delegations, status = manage_helper.do_get_delegations(actor_name=self.broker_name,
                                                                      callback_topic=self.kafka_topic)
        self.assertEqual(0, status.get_status().code)
        self.assertIsNotNone(broker_delegations)

        # Verify AM and Broker Delegations
        self.assertEqual(len(am_delegations), len(broker_delegations))
        ad = am_delegations[0]
        bd = broker_delegations[0]
        self.assertEqual(ad.delegation_id, bd.delegation_id)
        self.assertEqual(ad.slice.slice_name, bd.slice.slice_name)

        KafkaProcessorSingleton.get().stop()

    def test_b4_reclaim_resources(self):
        # This test requires the previous test:test_b2_claim_resources to be executed before this
        KafkaProcessorSingleton.get().start()
        manage_helper = ManageHelper(logger=self.logger)

        # Reclaim Delegations
        reclaim_status = manage_helper.reclaim_delegations(broker=self.broker_name, am=self.am_name,
                                                           callback_topic=self.kafka_topic)

        # Verify reclaim is successful
        self.assertEqual(True, reclaim_status)
        time.sleep(10)

        # Get AM Delegations
        am_delegations, status = manage_helper.do_get_delegations(actor_name=self.am_name,
                                                                  callback_topic=self.kafka_topic)
        self.assertEqual(0, status.get_status().code)
        self.assertIsNotNone(am_delegations)

        # Get Broker Delegations
        broker_delegations, status = manage_helper.do_get_delegations(actor_name=self.broker_name,
                                                                      callback_topic=self.kafka_topic)

        self.assertEqual(0, status.get_status().code)
        self.assertIsNotNone(broker_delegations)
        self.assertEqual(DelegationState.Reclaimed.value, broker_delegations[0].get_state())

        self.assertEqual(len(am_delegations), len(broker_delegations))
        ad = am_delegations[0]
        bd = broker_delegations[0]
        self.assertEqual(ad.delegation_id, bd.delegation_id)
        self.assertEqual(ad.slice.slice_name, bd.slice.slice_name)
        self.assertEqual(ad.state, bd.state)

        # Claim Delegations
        manage_helper.claim_delegations(broker=self.broker_name, am=self.am_name, callback_topic=self.kafka_topic)
        time.sleep(10)

        # Get AM Delegations
        am_delegations, status = manage_helper.do_get_delegations(actor_name=self.am_name,
                                                                  callback_topic=self.kafka_topic)
        self.assertEqual(0, status.get_status().code)
        self.assertIsNotNone(am_delegations)

        # Get Broker Delegations
        broker_delegations, status = manage_helper.do_get_delegations(actor_name=self.broker_name,
                                                                      callback_topic=self.kafka_topic)
        self.assertEqual(0, status.get_status().code)
        self.assertIsNotNone(broker_delegations)
        self.assertEqual(DelegationState.Delegated.value, broker_delegations[0].get_state())

        # Verify Broker and AM have same delegations
        self.assertEqual(len(am_delegations), len(broker_delegations))
        ad = am_delegations[0]
        bd = broker_delegations[0]
        self.assertEqual(ad.delegation_id, bd.delegation_id)
        self.assertEqual(ad.slice.slice_name, bd.slice.slice_name)
        self.assertEqual(ad.state, bd.state)

        KafkaProcessorSingleton.get().stop()

    def test_c_list_resources(self):
        oh = OrchestratorHelper()
        response = oh.resources()
        self.assertEqual(OK, response.status_code)
        status = response.json()[self.VALUE][self.STATUS]
        self.assertEqual(status, self.STATUS_OK)
        json_obj = response.json()[self.VALUE]
        self.assertIsNotNone(json_obj)
        self.assertIsNotNone(json_obj.get(Constants.BROKER_QUERY_MODEL, None))

        response = oh.portal_resources()
        self.assertEqual(OK, response.status_code)
        status = response.json()[self.VALUE][self.STATUS]
        self.assertEqual(status, self.STATUS_OK)
        json_obj = response.json()[self.VALUE]
        self.assertIsNotNone(json_obj)
        self.assertIsNotNone(json_obj.get(Constants.BROKER_QUERY_MODEL, None))

    @staticmethod
    def build_slice_with_compute_only(include_components: bool = False, exceed_capacities: bool = False,
                                      exceed_components: bool = False, use_hints: bool = False, no_cap: bool = False,
                                      instance_type: str = "fabric.c8.m32.d500") -> str:
        t = fu.ExperimentTopology()
        n1 = t.add_node(name='n1', site='RENC')

        n1.set_properties(image_type='qcow2', image_ref='default_centos_8')

        n2 = t.add_node(name='n2', site='RENC')
        n2.set_properties(image_type='qcow2', image_ref='default_centos_8')

        if not no_cap:
            cap = fu.Capacities()
            if exceed_capacities:
                cap.set_fields(core=33, ram=64, disk=500)
            else:
                cap.set_fields(core=3, ram=61, disk=499)
            n1.set_properties(capacities=cap)
            n2.set_properties(capacities=cap)

        if include_components:
            n1.add_component(ctype=fu.ComponentType.SmartNIC, model='ConnectX-6', name='nic1')
            n2.add_component(ctype=fu.ComponentType.NVME, model='P4510', name='c1')
            n2.add_component(ctype=fu.ComponentType.GPU, model='RTX6000', name='c2')
            if exceed_components:
                n2.add_component(ctype=fu.ComponentType.GPU, model='Tesla T4', name='c3')
                n2.add_component(ctype=fu.ComponentType.SmartNIC, model='ConnectX-6', name='nic1')
                n2.add_component(ctype=fu.ComponentType.SmartNIC, model='ConnectX-5', name='nic2')

        if use_hints:
            cap_hints = fu.CapacityHints()
            cap_hints.set_fields(instance_type=instance_type)
            n1.set_properties(capacity_hints=cap_hints)
            n2.set_properties(capacity_hints=cap_hints)

        return t.serialize()

    @staticmethod
    def build_slice() -> str:
        t = fu.ExperimentTopology()
        n1 = t.add_node(name='n1', site='RENC', ntype=fu.NodeType.VM)
        n2 = t.add_node(name='n2', site='RENC')
        n3 = t.add_node(name='n3', site='RENC')

        cap = fu.Capacities()
        cap.set_fields(core=2, ram=8, disk=100)
        n1.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')
        n2.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')
        n3.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')

        n1.add_component(model_type=fu.ComponentModelType.SharedNIC_ConnectX_6, name='n1-nic1')

        n2.add_component(model_type=fu.ComponentModelType.SmartNIC_ConnectX_6, name='n2-nic1')
        n2.add_component(ctype=fu.ComponentType.NVME, model='P4510', name='c1')

        n3.add_component(model_type=fu.ComponentModelType.SmartNIC_ConnectX_5, name='n3-nic1')

        t.add_network_service(name='bridge1', nstype=fu.ServiceType.L2Bridge, interfaces=t.interface_list)

        return t.serialize()

    @staticmethod
    def build_2_site_ptp_slice() -> str:
        """
        2-site for PTP service between two dedicated card ports
        """
        t = fu.ExperimentTopology()
        n1 = t.add_node(name='n1', site='RENC', ntype=fu.NodeType.VM)
        n2 = t.add_node(name='n2', site='UKY')

        cap = fu.Capacities()
        cap.set_fields(core=2, ram=8, disk=100)
        n1.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')
        n2.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')

        n1.add_component(model_type=fu.ComponentModelType.SmartNIC_ConnectX_6, name='n1-nic1')
        n2.add_component(model_type=fu.ComponentModelType.SmartNIC_ConnectX_6, name='n2-nic1')
        n2.add_component(ctype=fu.ComponentType.NVME, model='P4510', name='c1')

        t.add_network_service(name='ptp1', nstype=fu.ServiceType.L2PTP,
                              interfaces=[n1.interface_list[0], n2.interface_list[0]])

        return t.serialize()

    @staticmethod
    def build_2_site_ptp_slice_sriov() -> str:
        """
        2-site for PTP service between two shared card ports
        """
        t = fu.ExperimentTopology()
        n1 = t.add_node(name='n1', site='RENC', ntype=fu.NodeType.VM)
        n2 = t.add_node(name='n2', site='UKY')

        cap = fu.Capacities()
        cap.set_fields(core=2, ram=8, disk=100)
        n1.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')
        n2.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')

        n1.add_component(model_type=fu.ComponentModelType.SharedNIC_ConnectX_6, name='n1-nic1')
        n2.add_component(model_type=fu.ComponentModelType.SharedNIC_ConnectX_6, name='n2-nic1')
        n2.add_component(ctype=fu.ComponentType.NVME, model='P4510', name='c1')

        t.add_network_service(name='ptp1', nstype=fu.ServiceType.L2PTP,
                              interfaces=[n1.interface_list[0], n2.interface_list[0]])

        return t.serialize()

    @staticmethod
    def build_2_site_sts_slice() -> str:
        """
        2-site for STS service between shared and smart card ports
        """
        t = fu.ExperimentTopology()
        n1 = t.add_node(name='n1', site='RENC', ntype=fu.NodeType.VM)
        n2 = t.add_node(name='n2', site='UKY')
        n3 = t.add_node(name='n3', site='UKY')

        cap = fu.Capacities()
        cap.set_fields(core=2, ram=8, disk=100)
        n1.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')
        n2.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')
        n3.set_properties(capacities=cap, image_type='qcow2', image_ref='default_centos_8')

        n1.add_component(model_type=fu.ComponentModelType.SharedNIC_ConnectX_6, name='n1-nic1')
        n2.add_component(model_type=fu.ComponentModelType.SharedNIC_ConnectX_6, name='n2-nic1')
        n2.add_component(ctype=fu.ComponentType.NVME, model='P4510', name='c1')
        n3.add_component(model_type=fu.ComponentModelType.SmartNIC_ConnectX_6, name='n3-nic1')

        t.add_network_service(name='sts1', nstype=fu.ServiceType.L2STS,
                              interfaces=[n1.interface_list[0], n2.interface_list[0], n3.interface_list[0]])

        return t.serialize()

    def assert_am_broker_reservations(self, slice_id: str, res_id: str, am_res_state: int, broker_res_state: int,
                                      new_time: str = None):
        KafkaProcessorSingleton.get().start()
        manage_helper = ManageHelper(logger=self.logger)
        am_reservation, status = manage_helper.do_get_reservations(actor_name=self.am_name,
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
            if new_time is not None:
                lease_end = ActorClock.from_milliseconds(milli_seconds=am_reservation[0].requested_end)
                lease_end_str = lease_end.strftime(self.TIME_FORMAT_IN_SECONDS)
                self.assertEqual(new_time, lease_end_str)

        broker_reservation, status = manage_helper.do_get_reservations(actor_name=self.broker_name,
                                                                       callback_topic=KafkaProcessorSingleton.get().kafka_topic,
                                                                       rid=res_id)
        self.assertEqual(0, status.status.code)
        self.assertEqual(1, len(broker_reservation))
        self.assertEqual(slice_id, broker_reservation[0].slice_id)
        self.assertEqual(res_id, broker_reservation[0].reservation_id)
        self.assertEqual(broker_res_state, broker_reservation[0].state)
        if new_time is not None:
            lease_end = ActorClock.from_milliseconds(milli_seconds=broker_reservation[0].requested_end)
            lease_end_str = lease_end.strftime(self.TIME_FORMAT_IN_SECONDS)
            self.assertEqual(new_time, lease_end_str)

        KafkaProcessorSingleton.get().stop()

    def test_d_create_delete_slice_two_vms_with_components(self):

        # Create Slice
        slice_graph = self.build_slice_with_compute_only(include_components=True)
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
        slice_graph = self.build_slice_with_compute_only()
        oh = OrchestratorHelper()
        status, response = oh.create(slice_graph=slice_graph, slice_name=self.TEST_SLICE_NAME)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(response, list))
        self.assertEqual(len(response), 2)
        self.slice_id = response[0].slice_id

        # Attempt creating the slice again with same name and verify it fails
        status, response = oh.create(slice_graph=slice_graph, slice_name=self.TEST_SLICE_NAME)
        self.assertEqual(Status.FAILURE, status)
        self.assertEqual(f"Slice {self.TEST_SLICE_NAME} already exists", response.content)

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

    def test_f_create_delete_slice_with_instance_type(self):
        # Create Slice
        slice_graph = self.build_slice_with_compute_only(no_cap=True)
        oh = OrchestratorHelper()

        # Create Slice with no capacities and hints
        status, response = oh.create(slice_graph=slice_graph, slice_name=self.TEST_SLICE_NAME)
        self.assertEqual(Status.FAILURE, status)
        print(response)
        self.assertEqual(BAD_REQUEST, response.status_code)

        # Create Slice with exceeding capacities
        slice_graph = self.build_slice_with_compute_only(use_hints=True, instance_type="fabric.c64.m384.d4000")
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
        error_messages = "Insufficient resources : ['core']"
        for s in slivers:
            self.assertEqual(s.get_state(), ReservationStates.Closed.name)
            self.assertIsNone(s.management_ip)
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)
            self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
                                               am_res_state=-1,
                                               broker_res_state=ReservationStates.Closed.value)
            status, sliver_status = oh.sliver_status(slice_id=self.slice_id, sliver_id=s.reservation_id)

            self.assertEqual(Status.OK, status)
            self.assertTrue(sliver_status.get_notices().__contains__(error_messages))

        # Create Slice with capacities and hints
        slice_graph = self.build_slice_with_compute_only(use_hints=True)
        status, response = oh.create(slice_graph=slice_graph, slice_name=self.TEST_SLICE_NAME)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(response, list))
        self.assertEqual(len(response), 2)
        self.slice_id = response[0].slice_id

        # Wait for the Slice to be Stable
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

        # Verify delete slice fails as slices is already closed
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

        # Check Slices API
        status, slices = oh.slices()
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(slices, list))
        self.assertEqual(0, len(slices))

        status, slices = oh.slices(state="All")
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(slices, list))
        self.assertTrue(len(slices) > 0)

    def test_g_create_delete_slice_two_vms_with_components_not_available(self):
        # Create Slice
        slice_graph = self.build_slice_with_compute_only(exceed_components=True, include_components=True)
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
        error_messages = [Constants.CLOSURE_BY_TICKET_REVIEW_POLICY,
                          "Insufficient resources: No candidates nodes found to serve res"]
        i = 0
        for s in slivers:
            self.assertEqual(s.get_state(), ReservationStates.Closed.name)
            self.assertIsNone(s.management_ip)
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)
            self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
                                               am_res_state=-1,
                                               broker_res_state=ReservationStates.Closed.value)
            status, sliver_status = oh.sliver_status(slice_id=self.slice_id, sliver_id=s.reservation_id)
            self.assertEqual(Status.OK, status)
            self.assertTrue(sliver_status.get_notices().__contains__(error_messages[i]))
            i += 1

        # Verify delete slice fails as slices is already closed
        status, response = oh.delete(self.slice_id)
        self.assertEqual(status, Status.FAILURE)
        self.assertEqual(f"Slice# {self.slice_id} already closed", response.content)

    def test_h_create_slice_with_lease_end_and_renew_slice(self):
        # Create Slice
        slice_graph = self.build_slice_with_compute_only(include_components=True)
        oh = OrchestratorHelper()
        now = datetime.utcnow()
        new_time = now + timedelta(days=2)
        new_time_str = new_time.strftime(Constants.RENEW_TIME_FORMAT)
        status, response = oh.create(slice_graph=slice_graph, slice_name=self.TEST_SLICE_NAME,
                                     lease_end_time=new_time_str)
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
        new_time_str_without_seconds = new_time.strftime(self.TIME_FORMAT_IN_SECONDS)

        status, slivers = oh.slivers(slice_id=self.slice_id)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(slivers, list))
        for s in slivers:
            self.assertEqual(ReservationStates.Active.name, s.get_state())
            self.assertIsNotNone(s.management_ip)
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)
            lease_end = datetime.strptime(s.lease_end, Constants.RENEW_TIME_FORMAT)
            lease_end_without_seconds = lease_end.strftime(self.TIME_FORMAT_IN_SECONDS)
            self.assertEqual(new_time_str_without_seconds, lease_end_without_seconds)

            self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
                                               am_res_state=ReservationStates.Active.value,
                                               broker_res_state=ReservationStates.Ticketed.value,
                                               new_time=new_time_str_without_seconds)

        # Renew Slice
        now = datetime.utcnow()
        new_time = now + timedelta(days=14)

        new_time_str = new_time.strftime(self.TIME_FORMAT_IN_SECONDS)
        status, response = oh.renew(slice_id=self.slice_id, new_lease_end_time=new_time_str)
        self.assertEqual(Status.FAILURE, status)
        self.assertEqual(BAD_REQUEST, response.status_code)

        new_time_str = new_time.strftime(Constants.RENEW_TIME_FORMAT)

        status, response = oh.renew(slice_id="Slice_not-exists", new_lease_end_time=new_time_str)
        self.assertEqual(Status.FAILURE, status)
        self.assertEqual(NOT_FOUND, response.status_code)

        status, response = oh.renew(slice_id=self.slice_id, new_lease_end_time=new_time_str)
        self.assertEqual(Status.OK, status)

        time.sleep(10)

        # check Slivers and verify there states at all 3 actors
        status, slivers = oh.slivers(slice_id=self.slice_id)
        self.assertEqual(Status.OK, status)
        self.assertTrue(isinstance(slivers, list))
        new_time_str_without_seconds = new_time.strftime(self.TIME_FORMAT_IN_SECONDS)
        for s in slivers:
            self.assertEqual(ReservationStates.Active.name, s.get_state())
            self.assertIsNotNone(s.management_ip)
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)

            lease_end = datetime.strptime(s.lease_end, Constants.RENEW_TIME_FORMAT)
            lease_end_without_seconds = lease_end.strftime(self.TIME_FORMAT_IN_SECONDS)
            self.assertEqual(new_time_str_without_seconds, lease_end_without_seconds)

            self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
                                               am_res_state=ReservationStates.Active.value,
                                               broker_res_state=ReservationStates.Ticketed.value,
                                               new_time=new_time_str_without_seconds)

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

    def test_i_create_delete_slice_network_service(self):

        # Create Slice
        slice_graph = self.build_slice()
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
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)

            #self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
            #                                   am_res_state=ReservationStates.Active.value,
            #                                   broker_res_state=ReservationStates.Ticketed.value)

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

            #self.assert_am_broker_reservations(slice_id=self.slice_id, res_id=s.reservation_id,
            #                                   am_res_state=ReservationStates.Closed.value,
            #                                   broker_res_state=ReservationStates.Closed.value)

    def test_create_delete_slice_ptp_network_service(self):

        # Create Slice
        slice_graph = self.build_2_site_ptp_slice()
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
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)

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

    def test_create_delete_slice_ptp_sriov_network_service(self):

        # Create Slice
        slice_graph = self.build_2_site_ptp_slice_sriov()
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
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)

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

    def test_create_delete_slice_sts_network_service(self):

        # Create Slice
        slice_graph = self.build_2_site_sts_slice()
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
            self.assertIsNotNone(s.graph_node_id)
            self.assertEqual(self.slice_id, s.slice_id)

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
