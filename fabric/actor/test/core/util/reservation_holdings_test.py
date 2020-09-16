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

from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.kernel.client_reservation_factory import ClientReservationFactory
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.reservation_holdings import ReservationHoldings


class ReservationHoldingsTest(unittest.TestCase):
    def test_create(self):
        holdings = ReservationHoldings()
        self.assertIsNotNone(holdings.list)
        self.assertIsNotNone(holdings.map)
        self.assertIsNotNone(holdings.reservation_set)

    def print_list(self, holdings: ReservationHoldings):
        output = ""
        for r in holdings.list:
            output += str(r)
            output += ","
        print(output)

    def make_reservation(self, rid: str) -> IReservation:
        return ClientReservationFactory.create(ID(rid))

    def check_exists(self, holdings: ReservationHoldings, reservation: IReservation):
        rset = holdings.get_reservations()
        return rset.contains(reservation=reservation)

    def test_add_reservation(self):
        holdings = ReservationHoldings()
        length = 5
        i = 0
        while i <= 5:
            self.assertEqual(i, holdings.size())
            res = self.make_reservation(str(i))
            exist = self.check_exists(holdings, res)
            self.assertFalse(exist)
            holdings.add_reservation(res, 5-i, 5-i + length)
            exist = self.check_exists(holdings, res)
            self.assertTrue(exist)
            self.assertEqual(i + 1, len(holdings.list))
            self.assertEqual(i + 1, len(holdings.map))
            self.assertEqual(i + 1, holdings.reservation_set.size())
            i += 1

        res = self.make_reservation("100")
        holdings.add_reservation(res, 0, 8)

    def test_remove_reservation(self):
        holdings = ReservationHoldings()
        length =5
        i = 0
        while i <= 5:
            self.assertEqual(i, holdings.size())
            res = self.make_reservation(str(i))
            exist = self.check_exists(holdings, res)
            self.assertFalse(exist)
            holdings.add_reservation(res, 5-i, 5-i + length)
            exist = self.check_exists(holdings, res)
            self.assertTrue(exist)
            self.assertEqual(i + 1, len(holdings.list))
            self.assertEqual(i + 1, len(holdings.map))
            self.assertEqual(i + 1, holdings.reservation_set.size())
            i += 1
            self.print_list(holdings)

        res = self.make_reservation("100")
        holdings.add_reservation(res, 0, 8)
        self.print_list(holdings)

        exist = self.check_exists(holdings, res)
        self.assertTrue(exist)
        self.assertEqual(i + 1, len(holdings.list))
        self.assertEqual(i + 1, len(holdings.map))
        self.assertEqual(i + 1, holdings.reservation_set.size())

        holdings.remove_reservation(res)
        exist = self.check_exists(holdings, res)
        self.assertFalse(exist)
        self.assertEqual(i, len(holdings.list))
        self.assertEqual(i, len(holdings.map))
        self.assertEqual(i, holdings.reservation_set.size())

        i = 0
        while i <= 5 :
            res = self.make_reservation(str(i))
            exist = self.check_exists(holdings, res)
            self.assertTrue(exist)
            self.assertEqual(6 - i, len(holdings.list))
            self.assertEqual(6 - i, len(holdings.map))
            self.assertEqual(6 - i, holdings.reservation_set.size())

            holdings.remove_reservation(res)

            exist = self.check_exists(holdings, res)
            self.assertFalse(exist)
            self.assertEqual(5 - i, len(holdings.list))
            self.assertEqual(5 - i, len(holdings.map))
            self.assertEqual(5 - i, holdings.reservation_set.size())

            i += 1

    def test_tick(self):
        holdings = ReservationHoldings()
        length = 5
        i = 0
        while i <= 5:
            self.assertEqual(i, holdings.size())
            res = self.make_reservation(str(i))
            exist = self.check_exists(holdings, res)
            self.assertFalse(exist)
            holdings.add_reservation(res, 5-i, 5-i + length)
            exist = self.check_exists(holdings, res)
            self.assertTrue(exist)
            self.assertEqual(i + 1, len(holdings.list))
            self.assertEqual(i + 1, len(holdings.map))
            self.assertEqual(i + 1, holdings.reservation_set.size())
            i += 1
            self.print_list(holdings)

        res = self.make_reservation("100")
        holdings.add_reservation(res, 0, 8)
        self.print_list(holdings)

        for i in range(12):
            holdings.tick(i)
            size = 0
            if i < 5:
                size = 7
            else:
                size = 0

                if i == 5:
                    size = 6
                elif i == 6:
                    size = 5
                elif i == 7:
                    size = 4
                elif i == 8:
                    size = 2
                elif i == 9:
                    size = 1
            self.assertEqual(size, len(holdings.list))
            self.assertEqual(size, len(holdings.map))
            self.assertEqual(size, holdings.reservation_set.size())

    def test_intersection(self):
        holdings = ReservationHoldings()
        length = 5
        i = 0
        while i <= 5:
            self.assertEqual(i, holdings.size())
            res = self.make_reservation(str(i))
            exist = self.check_exists(holdings, res)
            self.assertFalse(exist)
            holdings.add_reservation(res, 5-i, 5-i + length)
            exist = self.check_exists(holdings, res)
            self.assertTrue(exist)
            self.assertEqual(i + 1, len(holdings.list))
            self.assertEqual(i + 1, len(holdings.map))
            self.assertEqual(i + 1, holdings.reservation_set.size())
            i += 1
            self.print_list(holdings)

        results = [1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 0, 0]
        points = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

        for i in range(len(points)):
            rset = holdings.get_reservations(points[i])
            self.assertIsNotNone(rset)
            self.assertEqual(results[i], rset.size())