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

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.kernel_tick import KernelTick


class TickTest(unittest.TestCase):
    from fabric_cf.actor.core.container.globals import Globals
    Globals.config_file = "./config/config.test.yaml"
    Constants.SUPERBLOCK_LOCATION = './state_recovery.lock'

    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().initialize()

    def get_tick(self):
        return KernelTick()

    def test_a_create(self):
        tick = self.get_tick()
        tick.initialize()

        self.assertEqual(0, tick.get_beginning_of_time())
        self.assertEqual(1, tick.get_cycle_millis())
        self.assertEqual(False, tick.is_manual())
        self.assertEqual(True, tick.stopped)
        self.assertIsNotNone(tick.clock)
        self.assertIsNotNone(tick.logger)
        self.assertIsNotNone(tick.subscribers)

    def test_b_properties(self):
        beginning = 1000
        cycle_length = 234

        tick = self.get_tick()
        tick.set_beginning_of_time(value=beginning)
        tick.set_cycle_millis(cycle_millis=cycle_length)
        tick.initialize()

        self.assertEqual(beginning, tick.get_beginning_of_time())
        self.assertEqual(cycle_length, tick.get_cycle_millis())

        failed = False

        try:
            tick.set_cycle_millis(cycle_millis=cycle_length + 10)
        except Exception:
            failed = True

        if not failed:
            self.fail()

        self.assertEqual(beginning, tick.get_beginning_of_time())

    def test_c_start_stop(self):
        tick = self.get_tick()
        tick.initialize()

        failed = False

        try:
            tick.tick()
        except Exception:
            failed = True

        self.assertTrue(failed)

        tick.start()

        failed = False

        try:
            tick.start()
        except Exception:
            failed = True

        self.assertTrue(failed)

        tick.stop()

        failed = False

        try:
            tick.stop()
        except Exception:
            failed = True

        self.assertTrue(failed)

        tick.start()
