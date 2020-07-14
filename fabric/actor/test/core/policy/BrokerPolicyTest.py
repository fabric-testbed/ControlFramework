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
from datetime import datetime
import time

from fabric.actor.core.apis.IAuthorityProxy import IAuthorityProxy
from fabric.actor.core.apis.IBroker import IBroker
from fabric.actor.core.apis.IBrokerReservation import IBrokerReservation
from fabric.actor.core.apis.IReservation import IReservation
from fabric.actor.core.apis.ISlice import ISlice
from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.kernel.SliceFactory import SliceFactory
from fabric.actor.core.proxies.kafka.KafkaAuthorityProxy import KafkaAuthorityProxy
from fabric.actor.core.registry.ActorRegistry import ActorRegistrySingleton
from fabric.actor.core.time.Term import Term
from fabric.actor.core.util.ID import ID
from fabric.actor.core.util.ResourceType import ResourceType
from fabric.actor.db.PsqlDatabase import PsqlDatabase
from fabric.actor.security.AuthToken import AuthToken
from fabric.actor.test.BaseTestCase import BaseTestCase
from fabric.actor.test.ClientCallbackHelper import ClientCallbackHelper
from fabric.actor.test.DummyAuthorityProxy import DummyAuthorityProxy


class BrokerPolicyTest(BaseTestCase, unittest.TestCase):
    DonateStartCycle = 10
    DonateEndCycle = 100
    Logger = logging.getLogger('BrokerPolicyTest')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="actor.log")
    Logger.setLevel(logging.INFO)

    from fabric.actor.core.container import Globals
    Globals.ConfigFile = Constants.TestBrokerConfigurationFile
    from fabric.actor.core.container.Globals import GlobalsSingleton
    GlobalsSingleton.get().start(True)

    def setUp(self):
        PsqlDatabase(BaseTestCase.DbUser, BaseTestCase.DbPwd, BaseTestCase.DbName, BaseTestCase.DbHost,
        self.Logger).reset_db()
        time.sleep(1)

    def get_broker(self) -> IBroker:
        broker = super().get_broker()
        broker.set_recovered(True)
        Term.set_clock(broker.get_actor_clock())
        return broker

    def get_source(self, units: int, rtype: ResourceType, broker: IBroker, slice_obj: ISlice):
        raise Exception("not implemented")

    def get_request(self, units: int, rtype: ResourceType, start: datetime, end: datetime):
        raise Exception("not implemented")

    def get_request_from_request(self, request: IBrokerReservation, units: int, rtype: ResourceType, start: datetime, end: datetime):
        raise Exception("not implemented")

    def assert_ticketed(self, r: IReservation, units: int, rtype: ResourceType, start: datetime, end: datetime):
        self.assertIsNotNone(r)
        self.assertFalse(r.is_failed())
        self.assertEqual(units, r.get_resources().get_units())
        self.assertEqual(rtype, r.get_resources().get_type())
        self.assertIsNotNone(r.get_term())
        self.assertEqual(start, r.get_term().get_new_start_time())
        self.assertEqual(end, r.get_term().get_end_time())

    def get_authority_proxy(self) -> IAuthorityProxy:
        auth = AuthToken("mysite", ID())
        from fabric.actor.core.container.Globals import GlobalsSingleton
        proxy = KafkaAuthorityProxy("test-topic", auth, GlobalsSingleton.get().get_logger())

        try:
            if ActorRegistrySingleton.get().get_proxy(Constants.ProtocolLocal, "mysite") is None:
                dummy = DummyAuthorityProxy(auth)
                ActorRegistrySingleton.get().register_proxy(dummy)
        except Exception as e:
            raise e
        return proxy

    def test_create(self):
        self.get_broker()

    def test_allocate_ticket(self):
        broker = self.get_broker()
        controller = self.get_controller()

        clock = broker.get_actor_clock()
        rtype = ResourceType("1")

        proxy = ClientCallbackHelper(controller.get_name(), controller.get_guid())
        broker_callback = ClientCallbackHelper(broker.get_name(), broker.get_guid())
        ActorRegistrySingleton.get().register_callback(proxy)
        ActorRegistrySingleton.get().register_callback(broker_callback)
        last_called = proxy.get_called()

        inv_slice = SliceFactory.create("inventory-slice")
        inv_slice.set_inventory(True)
        source = self.get_source(1, rtype, broker, inv_slice)

        broker.donate_reservation(source)

        cycle = 1
        broker.external_tick(cycle)
        cycle += 1

        units = 1
        start = clock.cycle_start_date(self.DonateStartCycle)
        end = clock.cycle_end_date(self.DonateEndCycle - 1)

        request = self.get_request(units, rtype, start, end)

        broker.ticket(request, proxy, proxy.get_identity())

        while cycle < self.DonateEndCycle:
            broker.external_tick(cycle)
            while broker.get_current_cycle() != cycle:
                time.sleep(0.001)
            cycle += 1

            if last_called < proxy.get_called():
                self.assert_ticketed(proxy.get_reservation(), units, rtype, start, end)
                last_called = proxy.get_called()

        broker.await_no_pending_reservations()

        self.assertEqual(1, proxy.get_called())
        self.assertTrue(request.is_closed())
