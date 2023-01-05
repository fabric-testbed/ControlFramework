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
import unittest

import time
from fabric_cf.actor.core.apis.abc_database import ABCDatabase
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.test.base_test_case import BaseTestCase


class ActorDatabaseTest(BaseTestCase, unittest.TestCase):
    from fabric_cf.actor.core.container.globals import Globals
    Globals.config_file = "./config/config.test.yaml"
    Constants.SUPERBLOCK_LOCATION = './state_recovery.lock'

    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().start(force_fresh=True)
    while not GlobalsSingleton.get().start_completed:
        time.sleep(0.0001)

    def get_clean_database(self) -> ABCDatabase:
        db = self.get_actor_database()
        db.set_actor_name(name=self.actor_name)
        db.set_reset_state(state=True)
        db.initialize()
        return db

    def test_a_create(self):
        self.get_clean_database()

    def prepare_actor_database(self):
        container_db = self.get_container_database()
        actor = self.get_actor()
        container_db.remove_actor(actor_name=actor.get_name())
        container_db.add_actor(actor=actor)
        actor.actor_added(config=None)
        return actor

    def test_b_create_2(self):
        self.prepare_actor_database()

    def get_database_to_test(self):
        actor = self.prepare_actor_database()
        return actor.get_plugin().get_database()

    def test_c_add_slice(self):
        db = self.get_database_to_test()
        slice_obj = SliceFactory.create(slice_id=ID(), name="slice_to_add")
        self.assertEqual("slice_to_add", slice_obj.get_name())
        db.add_slice(slice_object=slice_obj)
        slice2 = db.get_slices(slice_id=slice_obj.get_slice_id())
        self.assertIsNotNone(slice2)
        self.assertEqual(len(slice2), 1)
        db.remove_slice(slice_id=slice_obj.get_slice_id())
