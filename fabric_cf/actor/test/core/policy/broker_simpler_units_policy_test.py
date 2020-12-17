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
import time
import unittest
from datetime import datetime

from fabric_cf.actor.core.apis.i_broker import IBroker
from fabric_cf.actor.core.apis.i_broker_policy import IBrokerPolicy
from fabric_cf.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric_cf.actor.core.apis.i_slice import ISlice
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.resource_pool_attribute_descriptor import ResourcePoolAttributeDescriptor, \
    ResourcePoolAttributeType
from fabric_cf.actor.core.common.resource_pool_descriptor import ResourcePoolDescriptor
from fabric_cf.actor.core.core.broker_policy import BrokerPolicy
from fabric_cf.actor.core.core.ticket import Ticket
from fabric_cf.actor.core.kernel.broker_reservation_factory import BrokerReservationFactory
from fabric_cf.actor.core.kernel.client_reservation_factory import ClientReservationFactory
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.kernel.slice_factory import SliceFactory
from fabric_cf.actor.core.policy.broker_simpler_units_policy import BrokerSimplerUnitsPolicy
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_data import ResourceData
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.test.client_callback_helper import ClientCallbackHelper
from fabric_cf.actor.test.core.policy.broker_policy_test import BrokerPolicyTest


class BrokerSimplerUnitsPolicyTest(BrokerPolicyTest, unittest.TestCase):
    from fabric_cf.actor.core.container.globals import Globals
    Globals.config_file = Constants.test_broker_configuration_file

    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().start(force_fresh=True)
    while not GlobalsSingleton.get().start_completed:
        time.sleep(0.0001)

    def get_broker_policy(self) -> IBrokerPolicy:
        policy = BrokerSimplerUnitsPolicy()
        return policy

    def get_request_from_request(self, request: IBrokerReservation, units: int, rtype: ResourceType, start: datetime, end: datetime):
        rset = ResourceSet(units=units, rtype=rtype, rdata=ResourceData())

        term = Term(start=request.get_term().get_start_time(), end=end, new_start=start)

        result = BrokerReservationFactory.create(rid=request.get_reservation_id(), resources=rset, term=term,
                                                 slice_obj=request.get_slice())

        result.set_sequence_in(sequence=request.get_sequence_in() + 1)
        return result

    def get_request(self, units: int, rtype: ResourceType, start: datetime, end: datetime):
        slice_obj = SliceFactory.create(slice_id=ID(), name="test-slice")
        rset = ResourceSet(units=units, rtype=rtype, rdata=ResourceData())
        term = Term(start=start, end=end)
        request = BrokerReservationFactory.create(rid=ID(), resources=rset, term=term, slice_obj=slice_obj)
        return request

    def get_pool_descriptor(self, rtype: ResourceType) -> ResourcePoolDescriptor:
        rd = ResourcePoolDescriptor()
        rd.set_resource_type(rtype=rtype)
        rd.set_resource_type_label(rtype_label="Pool label: {}".format(rtype))
        ad = ResourcePoolAttributeDescriptor()
        ad.set_key(value=Constants.resource_memory)
        ad.set_label(label="Memory")
        ad.set_unit(unit="MB")
        ad.set_type(rtype=ResourcePoolAttributeType.INTEGER)
        ad.set_value(value=str(1024))
        rd.add_attribute(attribute=ad)
        return rd

    def get_source(self, units: int, rtype: ResourceType, broker: IBroker, slice_obj: ISlice):
        start = broker.get_actor_clock().cycle_start_date(cycle=self.DonateStartCycle)
        end = broker.get_actor_clock().cycle_end_date(cycle=self.DonateEndCycle)
        term = Term(start=start, end=end)

        resources = ResourceSet(units=units, rtype=rtype, rdata=ResourceData())

        properties = resources.get_resource_properties()
        rd = self.get_pool_descriptor(rtype)
        resources.set_resource_properties(p=rd.save(properties=properties, prefix=None))

        delegation = broker.get_plugin().get_ticket_factory().make_delegation(units=units, term=term, rtype=rtype,
                                                                              vector=None)
        ticket = broker.get_plugin().get_ticket_factory().make_ticket(delegation=delegation, source=None)
        cs = Ticket(ticket=ticket, plugin=broker.get_plugin())
        resources.set_resources(cset=cs)

        source = ClientReservationFactory.create(rid=ID(), resources=resources, term=term, slice_object=slice_obj)

        return source

    def check_query_response(self, response: dict, count: int):
        temp = response.get(Constants.query_response, None)
        self.assertIsNotNone(temp)
        self.assertEqual(temp, Constants.query_action_discover_pools)
        temp = response.get(Constants.pools_count, None)
        self.assertIsNotNone(temp)
        self.assertEqual(count, int(temp), count)
        result = BrokerPolicy.get_resource_pools(response)
        self.assertEqual(count, len(result))
        return result

    def test_e_donate(self):
        broker = self.get_broker()
        policy = broker.get_policy()

        slice_obj = SliceFactory.create(slice_id=ID(), name="inventory_slice")
        slice_obj.set_inventory(value=True)

        for i in range(1, 10):
            rtype = ResourceType(resource_type=str(i))
            source = self.get_source(i, rtype, broker, slice_obj)
            broker.donate_reservation(reservation=source)

            self.assertEqual(i, len(policy.inventory.get_inventory()))
            pool = policy.inventory.get(resource_type=rtype)
            self.assertIsNotNone(pool)
            self.assertEqual(i, pool.get_free())
            self.assertEqual(0, pool.get_allocated())

    def test_f_query(self):
        broker = self.get_broker()
        policy = broker.get_policy()

        request = {Constants.query_action:Constants.query_action_discover_pools}
        response = policy.query(p=request)

        print(response)

        self.check_query_response(response, 0)

        slice_obj = SliceFactory.create(slice_id=ID(), name="inventory_slice")
        slice_obj.set_inventory(value=True)

        for i in range(1, 10):
            rtype = ResourceType(resource_type=str(i))
            source = self.get_source(i, rtype, broker, slice_obj)
            broker.donate_reservation(reservation=source)

            response = policy.query(p=request)
            print(response)
            self.check_query_response(response, i)

    def test_g_advanced_request(self):
        broker = self.get_broker()
        controller = self.get_controller()
        policy = broker.get_policy()
        policy.allocation_horizon = 10

        clock = broker.get_actor_clock()

        rtype = ResourceType(resource_type="1")
        proxy = ClientCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        broker_callback = ClientCallbackHelper(name=broker.get_name(), guid=broker.get_guid())
        ActorRegistrySingleton.get().register_callback(callback=proxy)
        ActorRegistrySingleton.get().register_callback(callback=broker_callback)

        slice_obj = SliceFactory.create(slice_id=ID(), name="inventory_slice")
        slice_obj.set_inventory(value=True)

        source = self.get_source(1, rtype, broker, slice_obj)
        broker.donate_reservation(reservation=source)

        cycle = 1
        broker.external_tick(cycle=cycle)
        cycle += 1

        units = 1
        start = clock.cycle_start_date(cycle=self.DonateStartCycle)
        end = clock.cycle_end_date(cycle=self.DonateEndCycle - 1)

        request = self.get_request(units, rtype, start, end)
        broker.ticket(reservation=request, callback=proxy, caller=proxy.get_identity())

        self.assertEqual(proxy.prepared, 0)
        self.assertEqual(proxy.called, 0)

        for c in range(cycle, self.DonateEndCycle):
            broker.external_tick(cycle=c)
            while broker.get_current_cycle() != c:
                time.sleep(0.001)

            while proxy.prepared != 1:
                time.sleep(0.001)

            self.assert_ticketed(request, units, rtype, start, end)
            self.assertEqual(1, proxy.prepared)

            if proxy.called > 0:
                self.assert_ticketed(proxy.get_reservation(), units, rtype, start, end)

        broker.await_no_pending_reservations()

        self.assertEqual(1, proxy.get_called())
        self.assertTrue(request.is_closed())

