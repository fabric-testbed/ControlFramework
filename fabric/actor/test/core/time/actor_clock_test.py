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
from datetime import datetime

from fabric.actor.core.time.actor_clock import ActorClock


class ActorClockTest(unittest.TestCase):
    def _get_clock(self, offset: int, length: int):
        return ActorClock(offset, length)

    def test_create(self):
        clock = self._get_clock(0, 1)
        self.assertEqual(0, clock.beginning_of_time)
        self.assertEqual(1, clock.cycle_millis)
        self.assertEqual(0, clock.get_beginning_of_time())
        self.assertEqual(1, clock.get_cycle_millis())

        clock = self._get_clock(1000, 10)
        self.assertEqual(1000, clock.beginning_of_time)
        self.assertEqual(10, clock.cycle_millis)
        self.assertEqual(1000, clock.get_beginning_of_time())
        self.assertEqual(10, clock.get_cycle_millis())

    def test_cycle(self):
        offset = 1000
        length = 10
        clock = self._get_clock(1000, 10)

        ms = offset
        for i in range(100):
            exp = i
            self.assertEqual(exp, clock.cycle(millis=ms))
            self.assertEqual(exp, clock.cycle(when=datetime.fromtimestamp(ms*1000)))
            ms += length

        ms = offset
        for i in range(100):
            exp = i
            for j in range(length):
                self.assertEqual(exp, clock.cycle(millis=ms))
                self.assertEqual(exp, clock.cycle(when=datetime.fromtimestamp(ms*1000)))
                ms += 1

    def test_date(self):
        offset = 1000
        length = 10
        clock = self._get_clock(1000, 10)

        ms = offset
        for i in range(100):
            self.assertEqual(datetime.fromtimestamp(ms * 1000), clock.date(i))
            ms += length

    def test_cycle_start_end_date(self):
        offset = 1000
        length = 10
        clock = self._get_clock(1000, 10)

        start = offset
        end = offset + length - 1

        for i in range(100):
            self.assertEqual(datetime.fromtimestamp(start * 1000), clock.cycle_start_date(i))
            self.assertEqual(datetime.fromtimestamp(end * 1000), clock.cycle_end_date(i))
            self.assertEqual(start, clock.cycle_start_in_millis(i))
            self.assertEqual(end, clock.cycle_end_in_millis(i))
            start += length
            end += length

    def test_misc(self):
        offset = 1000
        length = 10
        clock = self._get_clock(1000, 10)

        ms = 0
        for i in range(100):
            temp = clock.get_millis(i)
            self.assertEqual(ms, temp)
            self.assertEqual(i, clock.convert_millis(temp))
            ms += length


if __name__ == '__main__':
    unittest.main()