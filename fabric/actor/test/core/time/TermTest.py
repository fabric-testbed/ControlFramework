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

from fabric.actor.core.time.Term import Term


class TermTest(unittest.TestCase):
    def setUp(self):
        Term.set_cycles = False

    def check(self, term: Term, start: int = None, new_start: int = None, end: int = None):
        if start is not None and new_start is not None and end is not None:
            self.assertIsNotNone(term.start_time)
            self.assertIsNotNone(term.new_start_time)
            self.assertIsNotNone(term.end_time)
            self.assertIsNotNone(term.get_start_time())
            self.assertIsNotNone(term.get_new_start_time())
            self.assertIsNotNone(term.get_end_time())
            self.assertEqual(term.get_start_time().timestamp(), start / 1000)
            self.assertEqual(term.get_new_start_time().timestamp(), new_start / 1000)
            self.assertEqual(term.get_end_time().timestamp(), end / 1000)
        else:
            self.assertIsNone(term.start_time)
            self.assertIsNone(term.new_start_time)
            self.assertIsNone(term.end_time)
            self.assertIsNone(term.get_start_time())
            self.assertIsNone(term.get_new_start_time())
            self.assertIsNone(term.get_end_time())

    def test_create(self):
        length = 500
        start = 1000
        new_start = 1500
        end = 1999
        old_end = 1499
        with self.assertRaises(Exception):
            Term()
        with self.assertRaises(Exception):
            t2 = Term(start=datetime.fromtimestamp(start / 1000))
        # Term(start, length)
        t3 = Term(start=datetime.fromtimestamp(start / 1000), length=length)
        self.check(t3, start=start, new_start= start, end=old_end)
        self.assertEqual(length, t3.get_length())

        # Term(start, end)
        t4 = Term(start=datetime.fromtimestamp(start / 1000), end=datetime.fromtimestamp(old_end / 1000))
        self.check(t4, start=start, new_start=start, end=old_end)
        self.assertEqual(length, t4.get_length())

        # Term(start, end, new_start)
        t5 = Term(start=datetime.fromtimestamp(start / 1000),
                  new_start=datetime.fromtimestamp(new_start / 1000),
                  end=datetime.fromtimestamp(end / 1000))
        self.check(t5, start=start, new_start=new_start, end=end)
        # NOTE: length is measured only relative to newStartTime!!!
        self.assertEqual(length, t5.get_length())

    def test_shift(self):
        start = 1000
        end = 1499
        length = 500
        t = Term(start=datetime.fromtimestamp(start / 1000), end=datetime.fromtimestamp(end / 1000))
        self.check(term=t, start=start, new_start=start, end=end)
        self.assertEqual(length, t.get_length())

        t1 = t.shift(datetime.fromtimestamp((start - 500) / 1000))
        self.check(term=t1, start=start-500, new_start=start-500, end=end-500)
        self.assertEqual(length, t1.get_length())

        t1 = t.shift(datetime.fromtimestamp((start) / 1000))
        self.check(term=t1, start=start, new_start=start, end=end)
        self.assertEqual(length, t1.get_length())

        t1 = t.shift(datetime.fromtimestamp((start + 500) / 1000))
        self.check(term=t1, start=start + 500, new_start=start + 500, end=end + 500)
        self.assertEqual(length, t1.get_length())

    def test_change(self):
        start = 1000
        end = 1499
        length = 500
        t = Term(start=datetime.fromtimestamp(start / 1000), end=datetime.fromtimestamp(end / 1000))
        self.check(term=t, start=start, new_start=start, end=end)
        self.assertEqual(length, t.get_length())

        t1 = t.change_length(2*length)
        self.check(term=t1, start=start, new_start=start, end=end+length)
        self.assertEqual(length*2, t1.get_length())

        t1 = t.change_length(length/2)
        self.check(term=t1, start=start, new_start=start, end=end - length/2)
        self.assertEqual(length / 2, t1.get_length())

    def test_extend(self):
        start = 1000
        end = 1499
        length = 500
        t = Term(start=datetime.fromtimestamp(start / 1000), end=datetime.fromtimestamp(end / 1000))
        self.check(term=t, start=start, new_start=start, end=end)
        self.assertEqual(length, t.get_length())

        # extend with same length
        t1 = t.extend()
        self.check(term=t1, start=start, new_start=end + 1, end=end + length)
        self.assertEqual(length, t1.get_length())
        self.assertEqual(2 * length, t1.get_full_length())
        self.assertTrue(t1.extends_term(t))

        # extend multiple times
        for i in range(10):
            t2 = t1.extend()
            self.check(start=t1.get_start_time().timestamp() * 1000,
                       new_start=t1.get_end_time().timestamp() * 1000 + 1,
                       end=t1.get_end_time().timestamp() * 1000 + length,
                       term=t2)
            self.assertEqual(length, t2.get_length())
            self.assertEqual(t1.get_full_length() + length, t2.get_full_length())
            self.assertTrue(t2.extends_term(t1))
            t1 = t2

        # extend with 1000
        l = 1000
        t1 = t.extend(length=l)
        self.check(start=start, new_start=end+1, end=end+l, term=t1)
        self.assertEqual(l, t1.get_length())
        self.assertEqual(l + length, t1.get_full_length())

        # extend multiple times
        for i in range(10):
            t2 = t1.extend()
            self.check(start=t1.get_start_time().timestamp() * 1000,
                       new_start=t1.get_end_time().timestamp() * 1000 + 1,
                       end=t1.get_end_time().timestamp() * 1000 + l,
                       term=t2)
            self.assertEqual(l, t2.get_length())
            self.assertEqual(t1.get_full_length() + l, t2.get_full_length())
            self.assertTrue(t2.extends_term(t2))
            t1 = t2

    def test_contains(self):
        start = 1000
        end = 1499
        length = 500
        t = Term(start=datetime.fromtimestamp(start / 1000), end=datetime.fromtimestamp(end / 1000))
        self.check(term=t, start=start, new_start=start, end=end)
        self.assertEqual(length, t.get_length())
        # self -> true
        self.assertTrue(t.contains(term=t))
        # self - 1 from the right
        self.assertTrue(t.contains(term=Term(start=datetime.fromtimestamp(start / 1000), length=length-1)))
        # self + 1 from the left
        self.assertTrue(t.contains(term=Term(start=datetime.fromtimestamp(start / 1000),
                                             end=datetime.fromtimestamp(end / 1000))))
        # self +1 from the left, -1 from the right
        self.assertTrue(t.contains(term=Term(start=datetime.fromtimestamp(start / 1000),
                                             end=datetime.fromtimestamp((end-1)/1000))))

        self.assertFalse(t.contains(term=Term(start=datetime.fromtimestamp((start-1)/1000),
                                              end=datetime.fromtimestamp(end/1000))))
        self.assertFalse(t.contains(term=Term(start=datetime.fromtimestamp((start-100)/1000),
                                              end=datetime.fromtimestamp(start/1000))))

        self.assertFalse(t.contains(term=Term(start=datetime.fromtimestamp(start/1000),
                                              end=datetime.fromtimestamp((end+1)/1000))))
        self.assertFalse(t.contains(term=Term(start=datetime.fromtimestamp(start/1000),
                                              end=datetime.fromtimestamp((end+100)/1000))))

        self.assertFalse(t.contains(term=Term(start=datetime.fromtimestamp((start - 100) / 1000),
                                              end=datetime.fromtimestamp((end + 100) / 1000))))

        self.assertTrue(t.contains(date=datetime.fromtimestamp(start/1000)))
        self.assertTrue(t.contains(date=datetime.fromtimestamp(end / 1000)))
        self.assertTrue(t.contains(date=datetime.fromtimestamp(((start+end)/2) / 1000)))

        self.assertFalse(t.contains(date=datetime.fromtimestamp((start - 1)/1000)))
        self.assertFalse(t.contains(date=datetime.fromtimestamp((end + 1) / 1000)))

    def test_ends(self):
        start = 1000
        end = 1499
        length = 500
        t = Term(start=datetime.fromtimestamp(start / 1000), end=datetime.fromtimestamp(end / 1000))
        self.check(term=t, start=start, new_start=start, end=end)
        self.assertEqual(length, t.get_length())
        # term cannot end before it started
        self.assertFalse(t.ends_before(t.get_start_time()))
        self.assertFalse(t.expired(t.get_start_time()))
        self.assertTrue(t.ends_after(t.get_start_time()))

        self.assertFalse(t.ends_before(t.get_end_time()))
        self.assertFalse(t.ends_after(t.get_end_time()))
        self.assertFalse(t.expired(t.get_end_time()))

        self.assertFalse(t.ends_before(datetime.fromtimestamp((start - 100) / 1000)))
        self.assertTrue(t.ends_after(datetime.fromtimestamp((start - 100) / 1000)))
        self.assertFalse(t.expired(datetime.fromtimestamp((start - 100) / 1000)))

        self.assertTrue(t.ends_before(datetime.fromtimestamp((end+1)/1000)))
        self.assertTrue(t.expired(datetime.fromtimestamp((end + 1) / 1000)))
        self.assertFalse(t.ends_after(datetime.fromtimestamp((end + 1) / 1000)))

    def test_equals(self):
        start = 1000
        end = 1499
        length = 500
        t = Term(start=datetime.fromtimestamp(start / 1000), end=datetime.fromtimestamp(end / 1000))
        self.check(term=t, start=start, new_start=start, end=end)
        self.assertEqual(length, t.get_length())

        self.assertTrue(t == t)

        t1 = Term(start=datetime.fromtimestamp(start / 1000), end=datetime.fromtimestamp(end / 1000))
        self.check(term=t1, start=start, new_start=start, end=end)
        self.assertEqual(length, t1.get_length())

        t2 = Term(start=datetime.fromtimestamp((start + 100) / 1000), end=datetime.fromtimestamp(end / 1000))
        self.check(term=t2, start=start + 100, new_start=start + 100, end=end)
        self.assertEqual(length - 100, t2.get_length())

        self.assertTrue(t == t1)
        self.assertFalse(t == t2)
        self.assertFalse(t == 11)

        class Abc:
            def __init__(self):
                self.num = 1

        self.assertFalse(t == Abc())

    @staticmethod
    def check_valid(term: Term):
        term.validate()

    def check_not_valid(self, term: Term):
        failed = False

        try:
            term.validate()
        except Exception as e:
            failed = True

        self.assertTrue(failed)

    def test_validate(self):
        with self.assertRaises(Exception):
            self.check_valid(Term(start=datetime.fromtimestamp(10/1000), new_start=datetime.fromtimestamp(100/1000),
                                  end=datetime.fromtimestamp(10/1000)))
            self.check_valid(Term(start=datetime.fromtimestamp(10 / 1000), new_start=datetime.fromtimestamp(100 / 1000),
                                  end=datetime.fromtimestamp(99 / 1000)))
            self.check_valid(Term(start=datetime.fromtimestamp(10 / 1000), new_start=datetime.fromtimestamp(1000 / 1000),
                                  end=datetime.fromtimestamp(1000 / 1000)))

        self.check_not_valid(Term(start=datetime.fromtimestamp(100/1000), end=datetime.fromtimestamp(10/1000)))
        self.check_not_valid(Term(start=datetime.fromtimestamp(100 / 1000), end=datetime.fromtimestamp(100 / 1000)))
        self.check_not_valid(Term(start=datetime.fromtimestamp(100 / 1000), end=datetime.fromtimestamp(1000 / 1000),
                                  new_start=datetime.fromtimestamp(10/1000)))
        self.check_not_valid(Term(start=datetime.fromtimestamp(100 / 1000), end=datetime.fromtimestamp(10 / 1000),
                                  new_start=datetime.fromtimestamp(10000/1000)))


if __name__ == '__main__':
    unittest.main()
