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

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.unit import Unit, UnitState
from fabric_cf.actor.core.kernel.reservation_client import ClientReservationFactory
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.plugins.substrate.db.substrate_actor_database import SubstrateActorDatabase
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.test.base_test_case import BaseTestCase


class UnitTest(BaseTestCase, unittest.TestCase):
    from fabric_cf.actor.core.container.globals import Globals
    Globals.config_file = "./config/config.test.yaml"
    Constants.SUPERBLOCK_LOCATION = './state_recovery.lock'

    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().start(force_fresh=True)
    while not GlobalsSingleton.get().start_completed:
        time.sleep(0.0001)

    def make_actor_database(self) -> SubstrateActorDatabase:
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        return GlobalsSingleton.get().get_container().get_actor().get_plugin().get_database()

    def test_unit(self):
        rid = ID()
        u1 = Unit(rid=rid)
        self.assertIsNotNone(u1.get_id())
        self.assertEqual(UnitState.DEFAULT, u1.get_state())
        self.assertIsNone(u1.get_property(name="foo"))
        self.assertIsNone(u1.get_parent_id())
        self.assertIsNotNone(u1.get_reservation_id())
        self.assertIsNone(u1.get_slice_id())
        self.assertIsNone(u1.get_actor_id())

        self.assertEqual(0, u1.get_sequence())
        u1.increment_sequence()
        self.assertEqual(1, u1.get_sequence())
        u1.decrement_sequence()
        self.assertEqual(0, u1.get_sequence())

        db = self.make_actor_database()

        slice_id = ID()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        actor_id = GlobalsSingleton.get().get_container().get_actor().get_guid()

        slice_obj = SliceFactory.create(slice_id=slice_id, name="test_slice")
        db.add_slice(slice_object=slice_obj)

        reservation = ClientReservationFactory.create(rid=rid, slice_object=slice_obj)
        u1.set_actor_id(actor_id=actor_id)
        u1.set_reservation(reservation=reservation)
        u1.set_slice_id(slice_id=slice_id)

        db.add_reservation(reservation=reservation)

        u1.start_prime()
        self.assertEqual(UnitState.PRIMING, u1.get_state())
        u1.set_property(name="foo", value="bar")
        u1.increment_sequence()
        u1.increment_sequence()
        resource_type = ResourceType(resource_type="1")
        u1.set_resource_type(rtype=resource_type)
        self.assertEqual(2, u1.get_sequence())

        db.add_unit(u=u1)

        self.assertIsNotNone(db.get_unit(uid=rid))
