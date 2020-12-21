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
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.time.calendar.controller_calendar import ControllerCalendar
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.test.core.time.calendar.client_calendar_test import ClientCalendarTest


class ControllerCalendarTest(ClientCalendarTest):
    def _get_calendar(self):
        clock = ActorClock(beginning_of_time=self.Offset, cycle_millis=self.Length)
        return ControllerCalendar(clock=clock)

    def test_create2(self):
        cal = self._get_calendar()
        self.assertIsNotNone(cal.closing)
        self.assertIsNotNone(cal.redeeming)

        self.assertIsNotNone(cal.get_closing(cycle=1000))
        self.assertIsNotNone(cal.get_redeeming(cycle=1000))

    def check_set(self, rset: ReservationSet, check: ReservationSet):
        self.assertIsNotNone(check)
        self.assertEqual(rset.size(), check.size())

        for res in rset.values():
            self.assertTrue(check.contains(reservation=res))
