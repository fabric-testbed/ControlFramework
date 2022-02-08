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
from datetime import datetime

from fim.pluggable import PluggableRegistry
from fim.slivers.attached_components import AttachedComponentsInfo, ComponentSliver, ComponentType
from fim.slivers.capacities_labels import Capacities, CapacityHints
from fim.slivers.instance_catalog import InstanceCatalog
from fim.slivers.network_node import NodeSliver, NodeType

from fabric_cf.actor.core.apis.abc_authority_proxy import ABCAuthorityProxy
from fabric_cf.actor.core.apis.abc_broker_mixin import ABCBrokerMixin
from fabric_cf.actor.core.apis.abc_broker_policy_mixin import ABCBrokerPolicyMixin
from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.apis.abc_slice import ABCSlice
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.broker_policy import BrokerPolicy
from fabric_cf.actor.core.delegation.broker_delegation_factory import BrokerDelegationFactory
from fabric_cf.actor.core.kernel.broker_reservation import BrokerReservationFactory
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.policy.broker_simpler_units_policy import BrokerSimplerUnitsPolicy
from fabric_cf.actor.core.policy.network_node_inventory import NetworkNodeInventory
from fabric_cf.actor.core.proxies.kafka.kafka_authority_proxy import KafkaAuthorityProxy
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.security.auth_token import AuthToken
from fabric_cf.actor.test.base_test_case import BaseTestCase
from fabric_cf.actor.test.client_callback_helper import ClientCallbackHelper
from fabric_cf.actor.test.fim_test_helper import FimTestHelper
from fabric_cf.actor.test.dummy_authority_proxy import DummyAuthorityProxy


class BrokerSimplerUnitsPolicyTest(BaseTestCase, unittest.TestCase):
    DonateStartCycle = 10
    DonateEndCycle = 100
    logger = logging.getLogger('BrokerSimplerUnitsPolicyTest')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="actor.log")
    logger.setLevel(logging.INFO)

    from fabric_cf.actor.core.container.globals import Globals
    Globals.config_file = "./config/config.test.yaml"
    Constants.SUPERBLOCK_LOCATION = './state_recovery.lock'

    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().start(force_fresh=True)
    while not GlobalsSingleton.get().start_completed:
        time.sleep(0.0001)

    adms = None
    broker = None

    def setUp(self) -> None:
        self.adms = FimTestHelper.generate_adms()
        PluggableRegistry.instance.clear()

    def tearDown(self) -> None:
        FimTestHelper.n4j_imp.delete_all_graphs()

    def get_broker(self, *, name: str = BaseTestCase.broker_name, guid: ID = BaseTestCase.broker_guid) -> ABCBrokerMixin:
        db = self.get_container_database()
        db.reset_db()
        self.broker = super().get_broker()
        self.broker.set_recovered(value=True)
        Term.set_clock(self.broker.get_actor_clock())
        return self.broker

    def get_broker_policy(self) -> ABCBrokerPolicyMixin:
        policy = BrokerSimplerUnitsPolicy()
        resource_type = ResourceType(resource_type=NodeType.VM.name)
        inventory = NetworkNodeInventory()
        policy.register_inventory(resource_type=resource_type, inventory=inventory)
        return policy

    def get_request_from_request(self, request: ABCBrokerReservation, units: int, rtype: ResourceType,
                                 start: datetime, end: datetime):
        rset = ResourceSet(units=units, rtype=rtype)

        term = Term(start=request.get_term().get_start_time(), end=end, new_start=start)

        result = BrokerReservationFactory.create(rid=request.get_reservation_id(), resources=rset, term=term,
                                                 slice_obj=request.get_slice())

        result.set_sequence_in(sequence=request.get_sequence_in() + 1)
        return result

    def build_sliver(self) -> NodeSliver:
        node_sliver = NodeSliver()
        node_sliver.resource_type = NodeType.VM
        node_sliver.node_id = "test-slice-node-1"
        cap = Capacities(core=4, ram=64, disk=500)
        catalog = InstanceCatalog()
        instance_type = catalog.map_capacities_to_instance(cap=cap)
        cap_hints = CapacityHints(instance_type=instance_type)
        node_sliver.set_properties(name="node-1", type=NodeType.VM, site="RENC",
                                   capacities=cap, image_type='qcow2', image_ref='default_centos_8',
                                   capacity_hints=cap_hints)
        return node_sliver

    def build_sliver_with_components(self) -> NodeSliver:
        node_sliver = NodeSliver()
        node_sliver.resource_type = NodeType.VM
        node_sliver.node_id = "test-slice-node-1"
        cap = Capacities(core=4, ram=64, disk=500)
        catalog = InstanceCatalog()
        instance_type = catalog.map_capacities_to_instance(cap=cap)
        cap_hints = CapacityHints(instance_type=instance_type)
        node_sliver.set_properties(name="node-1", type=NodeType.VM, site="RENC",
                                   capacities=cap, image_type='qcow2', image_ref='default_centos_8',
                                   capacity_hints=cap_hints)
        component_sliver = ComponentSliver()
        component_sliver.set_properties(type=ComponentType.SmartNIC, model='ConnectX-6', name='nic1')
        node_sliver.attached_components_info = AttachedComponentsInfo()
        node_sliver.attached_components_info.add_device(device_info=component_sliver)

        return node_sliver

    def get_reservation_for_network_node(self, start: datetime, end: datetime, sliver: NodeSliver):
        slice_obj = SliceFactory.create(slice_id=ID(), name="test-slice")
        rtype = ResourceType(resource_type=sliver.resource_type.name)
        rset = ResourceSet(units=1, rtype=rtype, sliver=sliver)
        term = Term(start=start, end=end)
        request = BrokerReservationFactory.create(rid=ID(), resources=rset, term=term, slice_obj=slice_obj)
        return request

    def get_authority_proxy(self) -> ABCAuthorityProxy:
        auth = AuthToken(name="mysite", guid=ID())
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        proxy = KafkaAuthorityProxy(kafka_topic="test-topic", identity=auth, logger=GlobalsSingleton.get().get_logger())

        if ActorRegistrySingleton.get().get_proxy(Constants.PROTOCOL_LOCAL, "mysite") is None:
            dummy = DummyAuthorityProxy(auth=auth)
            ActorRegistrySingleton.get().register_proxy(dummy)
        return proxy

    def check_query_response(self, response: dict, count: int):
        temp = response.get(Constants.QUERY_RESPONSE_STATUS, None)
        self.assertIsNotNone(temp)
        bqm = response.get(Constants.BROKER_QUERY_MODEL, None)
        if count == 0:
            self.assertEqual("", bqm)
        else:
            self.assertIsNotNone(bqm)

    def assert_ticketed(self, r: ABCReservationMixin, units: int, rtype: ResourceType, start: datetime, end: datetime):
        self.assertIsNotNone(r)
        self.assertFalse(r.is_failed())
        self.assertEqual(units, r.get_resources().get_units())
        self.assertEqual(rtype, r.get_resources().get_type())
        self.assertIsNotNone(r.get_term())
        self.assertEqual(start, r.get_term().get_new_start_time())
        self.assertEqual(end, r.get_term().get_end_time())

    def assert_failed(self, r: ABCReservationMixin, update_data: UpdateData = None):
        self.assertIsNotNone(r)
        if update_data is None:
            self.assertTrue(r.is_failed())
        else:
            self.assertTrue(update_data.failed)
        self.assertIsNotNone(r.get_notices())

    def get_source_delegation(self, broker: ABCBrokerMixin, slice_obj: ABCSlice, adm_index: int = 0):
        adm = self.adms[adm_index]
        broker_delegation = BrokerDelegationFactory.create(adm.graph_id, slice_id=slice_obj.get_slice_id(),
                                                           broker=broker)
        broker_delegation.set_graph(graph=adm)
        broker_delegation.set_slice_object(slice_object=slice_obj)
        return broker_delegation

    def test_a_create(self):
        """
        Tests if the actor and the policy can be instantiated.
        """
        self.get_broker()
        self.assertIsNotNone(self.broker)

    def test_b_allocate_ticket(self):
        """
        Requests a ticket for all resources. Checks if the ticket is
        allocated for what was asked. Checks the term. Checks whether the
        reservation is closed when it expires.
        """
        self.broker = self.get_broker()
        controller = self.get_controller()
        clock = self.broker.get_actor_clock()

        proxy = ClientCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        broker_callback = ClientCallbackHelper(name=self.broker.get_name(), guid=self.broker.get_guid())
        ActorRegistrySingleton.get().register_callback(callback=proxy)
        ActorRegistrySingleton.get().register_callback(callback=broker_callback)
        last_called = proxy.get_called()

        inv_slice = SliceFactory.create(slice_id=ID(), name="inventory-slice")
        inv_slice.set_inventory(value=True)
        source = self.get_source_delegation(self.broker, inv_slice)

        self.broker.register_slice(slice_object=inv_slice)
        self.broker.register_delegation(delegation=source)
        self.broker.donate_delegation(delegation=source)

        cycle = 1
        self.broker.external_tick(cycle=cycle)
        cycle += 1

        units = 1
        start = clock.cycle_start_date(cycle=self.DonateStartCycle)
        end = clock.cycle_end_date(cycle=self.DonateEndCycle - 1)

        sliver = self.build_sliver()
        request = self.get_reservation_for_network_node(start, end, sliver)

        self.broker.ticket(reservation=request, callback=proxy, caller=proxy.get_identity())

        for c in range(cycle, self.DonateEndCycle):
            self.broker.external_tick(cycle=c)
            while self.broker.get_current_cycle() != c:
                time.sleep(0.001)

            if last_called < proxy.get_called():
                self.assert_ticketed(proxy.get_reservation(), units, request.get_type(), start, end)
                last_called = proxy.get_called()

        self.broker.await_no_pending_reservations()

        self.assertEqual(1, proxy.get_called())
        self.assertTrue(request.is_closed())

    def test_c_allocate_ticket2(self):
        """
        Requests a ticket for all resources. Checks if the ticket is
        allocated for what was asked. Checks the term. Checks whether the
        reservation is closed when it expires. Repeat one more time.
        """
        broker = self.get_broker()
        controller = self.get_controller()
        clock = broker.get_actor_clock()

        proxy = ClientCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        broker_callback = ClientCallbackHelper(name=broker.get_name(), guid=broker.get_guid())
        ActorRegistrySingleton.get().register_callback(callback=proxy)
        ActorRegistrySingleton.get().register_callback(callback=broker_callback)
        last_called = proxy.get_called()

        inv_slice = SliceFactory.create(slice_id=ID(), name="inventory-slice")
        inv_slice.set_inventory(value=True)
        source = self.get_source_delegation(self.broker, inv_slice)

        self.broker.register_slice(slice_object=inv_slice)
        self.broker.register_delegation(delegation=source)
        self.broker.donate_delegation(delegation=source)

        cycle = 1
        broker.external_tick(cycle=cycle)
        cycle += 1

        units = 1
        start = clock.cycle_start_date(cycle=self.DonateStartCycle)
        cycle_end = self.DonateEndCycle - 50
        end = clock.cycle_end_date(cycle=cycle_end)

        sliver = self.build_sliver()
        request = self.get_reservation_for_network_node(start, end, sliver)

        broker.ticket(reservation=request, callback=proxy, caller=proxy.get_identity())

        for c in range(cycle, self.DonateEndCycle):
            broker.external_tick(cycle=c)
            while broker.get_current_cycle() != c:
                time.sleep(0.001)

            if last_called < proxy.get_called():
                self.assert_ticketed(proxy.get_reservation(), units, request.get_type(), start, end)
                last_called = proxy.get_called()

            if c == cycle_end:
                broker.await_no_pending_reservations()
                self.assertTrue(request.is_closed())
                self.assertEqual(1, proxy.get_called())
                start = clock.cycle_start_date(cycle=cycle_end + 1)
                end = clock.cycle_end_date(cycle=self.DonateEndCycle - 1)
                request = self.get_reservation_for_network_node(start, end, sliver=sliver)
                broker.ticket(reservation=request, callback=proxy, caller=proxy.get_identity())

        broker.await_no_pending_reservations()

        self.assertEqual(2, proxy.get_called())
        self.assertTrue(request.is_closed())

    def test_d_extend_ticket(self):
        """
        Requests a ticket for all resources. Checks if the ticket is
        allocated for what was asked. Checks the term. Checks whether the
        reservation is closed when it expires. Repeat one more time.
        """
        broker = self.get_broker()
        controller = self.get_controller()
        clock = broker.get_actor_clock()

        proxy = ClientCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        broker_callback = ClientCallbackHelper(name=broker.get_name(), guid=broker.get_guid())
        ActorRegistrySingleton.get().register_callback(callback=proxy)
        ActorRegistrySingleton.get().register_callback(callback=broker_callback)
        last_called = proxy.get_called()

        inv_slice = SliceFactory.create(slice_id=ID(), name="inventory-slice")
        inv_slice.set_inventory(value=True)
        source = self.get_source_delegation(self.broker, inv_slice)

        self.broker.register_slice(slice_object=inv_slice)
        self.broker.register_delegation(delegation=source)
        self.broker.donate_delegation(delegation=source)

        cycle = 1
        broker.external_tick(cycle=cycle)
        cycle += 1

        units = 1
        start = clock.cycle_start_date(cycle=self.DonateStartCycle)
        cycle_end = self.DonateEndCycle - 50
        end = clock.cycle_end_date(cycle=cycle_end)

        sliver = self.build_sliver()
        request = self.get_reservation_for_network_node(start, end, sliver)
        reservation = request

        broker.ticket(reservation=request, callback=proxy, caller=proxy.get_identity())

        for c in range(cycle, self.DonateEndCycle):
            broker.external_tick(cycle=c)
            while broker.get_current_cycle() != c:
                time.sleep(0.001)

            if last_called < proxy.get_called():
                self.assert_ticketed(proxy.get_reservation(), units, request.get_type(), start, end)
                last_called = proxy.get_called()

            if c == cycle_end - 10:
                self.assertEqual(1, proxy.get_called())
                start = ActorClock.from_milliseconds(milli_seconds=ActorClock.to_milliseconds(when=end) + 1)
                end = clock.cycle_end_date(cycle=self.DonateEndCycle - 1)
                request = self.get_request_from_request(request, units, request.get_type(), start, end)
                broker.extend_ticket(reservation=request, caller=proxy.get_identity())
                print("Extend done")

        broker.await_no_pending_reservations()

        self.assertEqual(2, proxy.get_called())
        self.assertTrue(reservation.is_closed())

    def test_e_donate(self):
        broker = self.get_broker()
        policy = broker.get_policy()

        slice_obj = SliceFactory.create(slice_id=ID(), name="inventory_slice")
        slice_obj.set_inventory(value=True)

        adm_list_len = len(self.adms)
        adm_list_len -= 1

        self.broker.register_slice(slice_object=slice_obj)
        for i in range(0, adm_list_len):
            source = self.get_source_delegation(self.broker, slice_obj, adm_index=i)
            self.broker.register_delegation(delegation=source)
            self.broker.donate_delegation(delegation=source)

            self.assertEqual(i + 1, len(policy.delegations))

    def test_f_query(self):
        broker = self.get_broker()
        policy = broker.get_policy()

        request = BrokerPolicy.get_broker_query_model_query(level=1)
        response = policy.query(p=request)

        print(response)

        self.check_query_response(response, 0)

        slice_obj = SliceFactory.create(slice_id=ID(), name="inventory_slice")
        slice_obj.set_inventory(value=True)

        adm_list_len = len(self.adms)
        adm_list_len -= 1

        self.broker.register_slice(slice_object=slice_obj)
        for i in range(0, adm_list_len):
            request = BrokerPolicy.get_broker_query_model_query(level=i + 1)
            source = self.get_source_delegation(self.broker, slice_obj, adm_index=i)
            self.broker.register_delegation(delegation=source)
            self.broker.donate_delegation(delegation=source)

            response = policy.query(p=request)
            print(response)
            self.check_query_response(response, i + 1)

    def test_g_advanced_request(self):
        broker = self.get_broker()
        controller = self.get_controller()
        policy = broker.get_policy()
        policy.allocation_horizon = 10

        clock = broker.get_actor_clock()

        proxy = ClientCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        broker_callback = ClientCallbackHelper(name=broker.get_name(), guid=broker.get_guid())
        ActorRegistrySingleton.get().register_callback(callback=proxy)
        ActorRegistrySingleton.get().register_callback(callback=broker_callback)

        slice_obj = SliceFactory.create(slice_id=ID(), name="inventory_slice")
        slice_obj.set_inventory(value=True)

        source = self.get_source_delegation(self.broker, slice_obj)
        self.broker.register_slice(slice_object=slice_obj)
        self.broker.register_delegation(delegation=source)
        self.broker.donate_delegation(delegation=source)

        cycle = 1
        broker.external_tick(cycle=cycle)
        cycle += 1

        units = 1
        start = clock.cycle_start_date(cycle=self.DonateStartCycle)
        end = clock.cycle_end_date(cycle=self.DonateEndCycle - 1)
        sliver = self.build_sliver_with_components()
        request = self.get_reservation_for_network_node(start, end, sliver=sliver)
        broker.ticket(reservation=request, callback=proxy, caller=proxy.get_identity())

        self.assertEqual(proxy.prepared, 0)
        self.assertEqual(proxy.called, 0)

        for c in range(cycle, self.DonateEndCycle):
            broker.external_tick(cycle=c)
            while broker.get_current_cycle() != c:
                time.sleep(0.001)

            while proxy.prepared != 1:
                time.sleep(0.001)

            self.assert_ticketed(request, units, request.get_type(), start, end)
            self.assertEqual(1, proxy.prepared)

            if proxy.called > 0:
                self.assert_ticketed(proxy.get_reservation(), units, request.get_type(), start, end)

        broker.await_no_pending_reservations()

        self.assertEqual(1, proxy.get_called())
        self.assertTrue(request.is_closed())

    def _test_h_request_insufficient_cores(self):
        broker = self.get_broker()
        controller = self.get_controller()
        policy = broker.get_policy()
        policy.allocation_horizon = 10

        clock = broker.get_actor_clock()

        proxy = ClientCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        broker_callback = ClientCallbackHelper(name=broker.get_name(), guid=broker.get_guid())
        ActorRegistrySingleton.get().register_callback(callback=proxy)
        ActorRegistrySingleton.get().register_callback(callback=broker_callback)

        slice_obj = SliceFactory.create(slice_id=ID(), name="inventory_slice")
        slice_obj.set_inventory(value=True)

        source = self.get_source_delegation(self.broker, slice_obj)
        self.broker.register_slice(slice_object=slice_obj)
        self.broker.register_delegation(delegation=source)
        self.broker.donate_delegation(delegation=source)

        cycle = 1
        broker.external_tick(cycle=cycle)
        cycle += 1

        start = clock.cycle_start_date(cycle=self.DonateStartCycle)
        end = clock.cycle_end_date(cycle=self.DonateEndCycle - 1)
        sliver = self.build_sliver_with_components()
        caphint = CapacityHints(instance_type="fabric.c64.m384.d4000")
        sliver.set_capacity_hints(caphint=caphint)

        request = self.get_reservation_for_network_node(start, end, sliver=sliver)
        broker.ticket(reservation=request, callback=proxy, caller=proxy.get_identity())

        self.assertEqual(proxy.prepared, 0)
        self.assertEqual(proxy.called, 0)

        for c in range(cycle, self.DonateEndCycle):
            broker.external_tick(cycle=c)
            while broker.get_current_cycle() != c:
                time.sleep(0.001)

            while proxy.prepared != 1:
                time.sleep(0.001)

            self.assert_failed(request)
            self.assertEqual(1, proxy.prepared)

            if proxy.called > 0:
                print("Proxy Called")
            #    self.assert_failed(proxy.get_reservation(), proxy.update_data)

        broker.await_no_pending_reservations()

        self.assertEqual(1, proxy.get_called())
        self.assertTrue(request.is_closed())