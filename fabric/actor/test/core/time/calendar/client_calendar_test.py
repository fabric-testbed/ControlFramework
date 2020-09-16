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

from fabric.actor.core.kernel.client_reservation_factory import ClientReservationFactory
from fabric.actor.core.time.actor_clock import ActorClock
from fabric.actor.core.time.calendar.client_calendar import ClientCalendar
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.reservation_set import ReservationSet


class ClientCalendarTest(unittest.TestCase):
    Offset = 1000
    Length = 10

    def _get_calendar(self):
        clock = ActorClock(self.Offset, self.Length)
        return ClientCalendar(clock)

    def _make_reservation(self, id: str):
        return ClientReservationFactory.create(ID(id))

    def test_create(self):
        cal = self._get_calendar()
        self.assertIsNotNone(cal.clock)
        self.assertIsNotNone(cal.demand)
        self.assertIsNotNone(cal.holdings)
        self.assertIsNotNone(cal.pending)
        self.assertIsNotNone(cal.renewing)
        self.assertIsNotNone(cal.get_demand())
        self.assertIsNotNone(cal.get_holdings())
        self.assertIsNotNone(cal.get_pending())
        self.assertIsNotNone(cal.get_renewing(1000))

    def check_set(self, rset: ReservationSet, check: ReservationSet):
        self.assertIsNotNone(check)
        self.assertEqual(rset.size(), check.size())
        for r in rset.values():
            self.assertTrue(check.contains(r))

    def test_demand(self):
        cal = self._get_calendar()
        rset = ReservationSet()

        for i in range(5):
            r = self._make_reservation(str(i))
            # add to the list
            rset.add(r)
            cal.add_demand(r)
            # get the list and check it
            temp = cal.get_demand()
            self.check_set(rset, temp)
            # remove from the returned set
            temp.remove(r)
            # make sure this did not affect the parent data structure
            temp = cal.get_demand()
            self.check_set(rset, temp)

        for i in range(5):
            r = self._make_reservation(str(i))
            # add to the list
            rset.remove(r)
            cal.remove_demand(r)

            # get the list and check it
            temp = cal.get_demand()

            self.check_set(rset, temp)

    def test_pending(self):
        cal = self._get_calendar()
        rset = ReservationSet()

        for i in range(5):
            r = self._make_reservation(str(i))
            rset.add(r)
            cal.add_pending(r)
            temp = cal.get_pending()
            self.check_set(rset, temp)
            temp.remove(r)
            temp = cal.get_pending()
            self.check_set(rset, temp)

        for i in range(5):
            r = self._make_reservation(str(i))
            rset.remove(r)
            cal.remove_pending(r)
            temp = cal.get_pending()
            self.check_set(rset, temp)
