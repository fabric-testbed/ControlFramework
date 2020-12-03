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
import traceback
import unittest
from datetime import datetime
import time

from fabric.actor.core.apis.i_authority_proxy import IAuthorityProxy
from fabric.actor.core.apis.i_broker import IBroker
from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.apis.i_slice import ISlice
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.common.exceptions import BrokerException
from fabric.actor.core.kernel.slice_factory import SliceFactory
from fabric.actor.core.proxies.kafka.kafka_authority_proxy import KafkaAuthorityProxy
from fabric.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric.actor.core.time.actor_clock import ActorClock
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_type import ResourceType
from fabric.actor.security.auth_token import AuthToken
from fabric.actor.test.base_test_case import BaseTestCase
from fabric.actor.test.client_callback_helper import ClientCallbackHelper
from fabric.actor.test.dummy_authority_proxy import DummyAuthorityProxy


class BrokerPolicyTest(BaseTestCase):
    DonateStartCycle = 10
    DonateEndCycle = 100
    logger = logging.getLogger('BrokerPolicyTest')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="actor.log")
    logger.setLevel(logging.INFO)

    def get_broker(self, *, name: str = BaseTestCase.broker_name, guid: ID = BaseTestCase.broker_guid) -> IBroker:
        db = self.get_container_database()
        db.reset_db()
        broker = super().get_broker()
        broker.set_recovered(value=True)
        Term.set_clock(broker.get_actor_clock())
        return broker

    def get_source(self, units: int, rtype: ResourceType, broker: IBroker, slice_obj: ISlice):
        raise BrokerException("not implemented")

    def get_request(self, units: int, rtype: ResourceType, start: datetime, end: datetime):
        raise BrokerException("not implemented")

    def get_request_from_request(self, request: IBrokerReservation, units: int, rtype: ResourceType, start: datetime, end: datetime):
        raise BrokerException("not implemented")

    def assert_ticketed(self, r: IReservation, units: int, rtype: ResourceType, start: datetime, end: datetime):
        self.assertIsNotNone(r)
        self.assertFalse(r.is_failed())
        self.assertEqual(units, r.get_resources().get_units())
        self.assertEqual(rtype, r.get_resources().get_type())
        self.assertIsNotNone(r.get_term())
        self.assertEqual(start, r.get_term().get_new_start_time())
        self.assertEqual(end, r.get_term().get_end_time())

    def get_authority_proxy(self) -> IAuthorityProxy:
        auth = AuthToken(name="mysite", guid=ID())
        from fabric.actor.core.container.globals import GlobalsSingleton
        proxy = KafkaAuthorityProxy(kafka_topic="test-topic", identity=auth, logger=GlobalsSingleton.get().get_logger())

        if ActorRegistrySingleton.get().get_proxy(Constants.protocol_local, "mysite") is None:
            dummy = DummyAuthorityProxy(auth=auth)
            ActorRegistrySingleton.get().register_proxy(dummy)
        return proxy

    def _test_a_create(self):
        """
        Tests if the actor and the policy can be instantiated.
        """
        broker = self.get_broker()
        self.assertIsNotNone(broker)

    def _test_b_allocate_ticket(self):
        """
        Requests a ticket for all resources. Checks if the ticket is
        allocated for what was asked. Checks the term. Checks whether the
        reservation is closed when it expires.
        """
        broker = self.get_broker()
        controller = self.get_controller()
        clock = broker.get_actor_clock()
        rtype = ResourceType(resource_type="1")

        proxy = ClientCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        broker_callback = ClientCallbackHelper(name=broker.get_name(), guid=broker.get_guid())
        ActorRegistrySingleton.get().register_callback(proxy)
        ActorRegistrySingleton.get().register_callback(broker_callback)
        last_called = proxy.get_called()

        inv_slice = SliceFactory.create(slice_id=ID(), name="inventory-slice")
        inv_slice.set_inventory(value=True)
        source = self.get_source(1, rtype, broker, inv_slice)

        broker.donate_reservation(reservation=source)

        cycle = 1
        broker.external_tick(cycle=cycle)
        cycle += 1

        units = 1
        start = clock.cycle_start_date(cycle=self.DonateStartCycle)
        end = clock.cycle_end_date(cycle=self.DonateEndCycle - 1)

        request = self.get_request(units, rtype, start, end)

        broker.ticket(reservation=request, callback=proxy, caller=proxy.get_identity())

        for c in range(cycle, self.DonateEndCycle):
            broker.external_tick(cycle=c)
            while broker.get_current_cycle() != c:
                time.sleep(0.001)

            if last_called < proxy.get_called():
                self.assert_ticketed(proxy.get_reservation(), units, rtype, start, end)
                last_called = proxy.get_called()

        broker.await_no_pending_reservations()

        self.assertEqual(1, proxy.get_called())
        self.assertTrue(request.is_closed())

    def _test_c_allocate_ticket2(self):
        """
        Requests a ticket for all resources. Checks if the ticket is
        allocated for what was asked. Checks the term. Checks whether the
        reservation is closed when it expires. Repeat one more time.
        """
        broker = self.get_broker()
        controller = self.get_controller()
        clock = broker.get_actor_clock()
        rtype = ResourceType(resource_type="1")

        proxy = ClientCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        broker_callback = ClientCallbackHelper(name=broker.get_name(), guid=broker.get_guid())
        ActorRegistrySingleton.get().register_callback(proxy)
        ActorRegistrySingleton.get().register_callback(broker_callback)
        last_called = proxy.get_called()

        inv_slice = SliceFactory.create(slice_id=ID(), name="inventory-slice")
        inv_slice.set_inventory(value=True)
        source = self.get_source(1, rtype, broker, inv_slice)

        broker.donate_reservation(reservation=source)

        cycle = 1
        broker.external_tick(cycle=cycle)
        cycle += 1

        units = 1
        start = clock.cycle_start_date(cycle=self.DonateStartCycle)
        cycle_end = self.DonateEndCycle - 50
        end = clock.cycle_end_date(cycle=cycle_end)

        request = self.get_request(units, rtype, start, end)

        broker.ticket(reservation=request, callback=proxy, caller=proxy.get_identity())

        for c in range(cycle, self.DonateEndCycle):
            broker.external_tick(cycle=c)
            while broker.get_current_cycle() != c:
                time.sleep(0.001)

            if last_called < proxy.get_called():
                self.assert_ticketed(proxy.get_reservation(), units, rtype, start, end)
                last_called = proxy.get_called()

            if c == cycle_end:
                broker.await_no_pending_reservations()
                self.assertTrue(request.is_closed())
                self.assertEqual(1, proxy.get_called())
                start = clock.cycle_start_date(cycle=cycle_end + 1)
                end = clock.cycle_end_date(cycle=self.DonateEndCycle - 1)
                request = self.get_request(units, rtype, start, end)
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
        rtype = ResourceType(resource_type="1")

        proxy = ClientCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        broker_callback = ClientCallbackHelper(name=broker.get_name(), guid=broker.get_guid())
        ActorRegistrySingleton.get().register_callback(callback=proxy)
        ActorRegistrySingleton.get().register_callback(callback=broker_callback)
        last_called = proxy.get_called()

        inv_slice = SliceFactory.create(slice_id=ID(), name="inventory-slice")
        inv_slice.set_inventory(value=True)
        source = self.get_source(1, rtype, broker, inv_slice)

        broker.donate_reservation(reservation=source)

        cycle = 1
        broker.external_tick(cycle=cycle)
        cycle += 1

        units = 1
        start = clock.cycle_start_date(cycle=self.DonateStartCycle)
        cycle_end = self.DonateEndCycle - 50
        end = clock.cycle_end_date(cycle=cycle_end)

        request = self.get_request(units, rtype, start, end)
        reservation = request

        broker.ticket(reservation=request, callback=proxy, caller=proxy.get_identity())

        for c in range(cycle, self.DonateEndCycle):
            broker.external_tick(cycle=c)
            while broker.get_current_cycle() != c:
                time.sleep(0.001)

            if last_called < proxy.get_called():
                self.assert_ticketed(proxy.get_reservation(), units, rtype, start, end)
                last_called = proxy.get_called()

            if c == cycle_end - 10:
                self.assertEqual(1, proxy.get_called())
                start = ActorClock.from_milliseconds(milli_seconds=ActorClock.to_milliseconds(when=end) + 1)
                end = clock.cycle_end_date(cycle=self.DonateEndCycle - 1)
                request = self.get_request_from_request(request, units, rtype, start, end)
                broker.extend_ticket(reservation=request, caller=proxy.get_identity())
                print("Extend done")

        broker.await_no_pending_reservations()

        self.assertEqual(2, proxy.get_called())
        self.assertTrue(reservation.is_closed())