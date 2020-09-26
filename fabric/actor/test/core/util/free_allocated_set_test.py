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

from fabric.actor.core.policy.free_allocated_set import FreeAllocatedSet


class FreeAllocatedSetTest(unittest.TestCase):
    def test_it(self):
        free_set = FreeAllocatedSet()
        self.assertEqual(0, len(free_set.free_set))
        self.assertEqual(0, len(free_set.allocated))

        for i in range(50):
            self.assertEqual(i, free_set.get_free())
            self.assertEqual(0, free_set.get_allocated())

            self.assertFalse(free_set.free_set.__contains__(i))
            self.assertFalse(free_set.allocated.__contains__(i))
            free_set.add_inventory(item=i)
            self.assertTrue(free_set.free_set.__contains__(i))
            self.assertFalse(free_set.allocated.__contains__(i))

        i = free_set.allocate()
        self.assertIsNotNone(i)
        self.assertEqual(1, free_set.get_allocated())
        self.assertEqual(49, free_set.get_free())
        self.assertTrue(free_set.allocated.__contains__(i))
        self.assertFalse(free_set.free_set.__contains__(i))

        free_set.free()
        self.assertEqual(0, free_set.get_allocated())
        self.assertEqual(50, free_set.get_free())
        self.assertFalse(free_set.allocated.__contains__(i))
        self.assertTrue(free_set.free_set.__contains__(i))

        ll = free_set.allocate_count(count=25)
        self.assertEqual(25, len(ll))

        for i in ll:
            self.assertEqual(25, free_set.get_allocated())
            self.assertEqual(25, free_set.get_free())
            self.assertTrue(free_set.allocated.__contains__(i))
            self.assertFalse(free_set.free_set.__contains__(i))

        sub1 = ll[0:10]
        sub2 = ll[10:25]
        free_set.free_list(items=sub1)

        self.assertEqual(35, free_set.get_free())
        self.assertEqual(15, free_set.get_allocated())

        for i in sub1:
            self.assertTrue(free_set.free_set.__contains__(i))
            self.assertFalse(free_set.allocated.__contains__(i))

        for i in sub2:
            self.assertFalse(free_set.free_set.__contains__(i))
            self.assertTrue(free_set.allocated.__contains__(i))

        free_set.free_list(items=sub2)

        self.assertEqual(50, free_set.get_free())
        self.assertEqual(0, free_set.get_allocated())

        for i in ll:
            self.assertTrue(free_set.free_set.__contains__(i))
            self.assertFalse(free_set.allocated.__contains__(i))

