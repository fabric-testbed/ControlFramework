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

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.kernel.reservation_client import ClientReservationFactory
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reservation_list import ReservationList


class ReservationListTest(unittest.TestCase):
    def test_create(self):
        r_list = ReservationList()
        self.assertEqual(r_list.size(), 0)
        self.assertIsNotNone(r_list.rset_wrapper_list)
        self.assertIsNotNone(r_list.cycle_to_rset)
        self.assertEqual(r_list.count, 0)

    def make_reservation(self, rid: str) -> ABCReservationMixin:
        return ClientReservationFactory.create(rid=ID(uid=rid))

    def test_add_reservation(self):
        r_list = ReservationList()
        for i in range(10):
            for j in range(10):
                c = (i*10) + j
                res = self.make_reservation(str(c))
                r_list.add_reservation(reservation=res, cycle=i)
                self.assertEqual(c+1, r_list.size())

    def test_get_reservations(self):
        r_list = ReservationList()
        for i in range(10):
            for j in range(10):
                c = (i * 10) + j
                res = self.make_reservation(str(c))
                r_list.add_reservation(reservation=res, cycle=i)
                self.assertEqual(c + 1, r_list.size())

        for i in range(10):
            low = 10 * i
            high = 10 * (i + 1)

            rset = r_list.get_reservations(cycle=i)
            self.assertIsNotNone(rset)
            self.assertEqual(10, rset.size())

            for r in rset.values():
                value = int(str(r.get_reservation_id()))
                self.assertTrue(low <= value < high)

        rset = r_list.get_reservations(cycle=10)
        self.assertIsNotNone(rset)
        self.assertEqual(0, rset.size())

    def test_tick(self):
        r_list = ReservationList()
        for i in range(10):
            for j in range(10):
                c = (i*10) + j
                res = self.make_reservation(str(c))
                r_list.add_reservation(reservation=res, cycle=i)
                self.assertEqual(c+1, r_list.size())
                self.assertEqual(c+1, len(r_list.reservation_id_to_cycle))

        for i in range(10):
            rset = r_list.get_reservations(cycle=i)
            self.assertIsNotNone(rset)
            self.assertEqual(10, rset.size())

            expected_size = (10 - i) * 10
            self.assertEqual(expected_size, r_list.size())
            r_list.tick(cycle=i)

            rset = r_list.get_reservations(cycle=i)

            self.assertIsNotNone(rset)
            self.assertEqual(0, rset.size())

        self.assertEqual(0, r_list.size())
        self.assertEqual(0, len(r_list.reservation_id_to_cycle))

    def check_exists(self, holdings: ReservationList, reservation: ABCReservationMixin, cycle: int):
        rset = holdings.get_reservations(cycle=cycle)
        return rset.contains(reservation=reservation)

    def test_remove_reservation(self):
        r_list = ReservationList()
        for i in range(10):
            for j in range(10):
                c = (i*10) + j
                res = self.make_reservation(str(c))
                r_list.add_reservation(reservation=res, cycle=i)
                self.assertEqual(c+1, r_list.size())
                self.assertEqual(c+1, len(r_list.reservation_id_to_cycle))

        for i in range(10):
            for j in range(10):
                c = (i * 10) + j
                res = self.make_reservation(str(c))
                exist = self.check_exists(r_list, res, i)
                self.assertTrue(exist)
                r_list.remove_reservation(reservation=res)
                exist = self.check_exists(holdings=r_list, reservation=res, cycle=i)
                self.assertTrue(not exist)
