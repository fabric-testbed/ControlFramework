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

from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.time.term import Term


class TermTest(unittest.TestCase):
    def setUp(self):
        Term.set_cycles = False

    def check(self, *, term: Term, start: int = None, new_start: int = None, end: int = None):
        if start is not None and new_start is not None and end is not None:
            self.assertIsNotNone(term.start_time)
            self.assertIsNotNone(term.new_start_time)
            self.assertIsNotNone(term.end_time)
            self.assertIsNotNone(term.get_start_time())
            self.assertIsNotNone(term.get_new_start_time())
            self.assertIsNotNone(term.get_end_time())

            self.assertEqual(start, ActorClock.to_milliseconds(when=term.get_start_time()))
            self.assertEqual(new_start, ActorClock.to_milliseconds(when=term.get_new_start_time()))
            self.assertEqual(end, ActorClock.to_milliseconds(when=term.get_end_time()))
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
            t2 = Term(start=ActorClock.from_milliseconds(milli_seconds=start))

        # Term(start, length)
        t3 = Term(start=ActorClock.from_milliseconds(milli_seconds=start), length=length)
        self.check(term=t3, start=start, new_start=start, end=old_end)
        self.assertEqual(length, t3.get_length())

        # Term(start, end)
        t4 = Term(start=ActorClock.from_milliseconds(milli_seconds=start),
                  end=ActorClock.from_milliseconds(milli_seconds=old_end))
        self.check(term=t4, start=start, new_start=start, end=old_end)
        self.assertEqual(length, t4.get_length())

        # Term(start, end, new_start)
        t5 = Term(start=ActorClock.from_milliseconds(milli_seconds=start),
                  new_start=ActorClock.from_milliseconds(milli_seconds=new_start),
                  end=ActorClock.from_milliseconds(milli_seconds=end))
        self.check(term=t5, start=start, new_start=new_start, end=end)
        # NOTE: length is measured only relative to newStartTime!!!
        self.assertEqual(length, t5.get_length())

    def test_shift(self):
        start = 1000
        end = 1499
        length = 500
        t = Term(start=ActorClock.from_milliseconds(milli_seconds=start),
                 end=ActorClock.from_milliseconds(milli_seconds=end))
        self.check(term=t, start=start, new_start=start, end=end)
        self.assertEqual(length, t.get_length())

        t1 = t.shift(date=ActorClock.from_milliseconds(milli_seconds=start-500))
        self.check(term=t1, start=start-500, new_start=start-500, end=end-500)
        self.assertEqual(length, t1.get_length())

        t1 = t.shift(date=ActorClock.from_milliseconds(milli_seconds=start))
        self.check(term=t1, start=start, new_start=start, end=end)
        self.assertEqual(length, t1.get_length())

        t1 = t.shift(date=ActorClock.from_milliseconds(milli_seconds=start+500))
        self.check(term=t1, start=start + 500, new_start=start + 500, end=end + 500)
        self.assertEqual(length, t1.get_length())

    def test_change(self):
        start = 1000
        end = 1499
        length = 500
        t = Term(start=ActorClock.from_milliseconds(milli_seconds=start),
                 end=ActorClock.from_milliseconds(milli_seconds=end))
        self.check(term=t, start=start, new_start=start, end=end)
        self.assertEqual(length, t.get_length())

        t1 = t.change_length(length=2 * length)
        self.check(term=t1, start=start, new_start=start, end=end+length)
        self.assertEqual(length * 2, t1.get_length())

        t1 = t.change_length(length=int(length/2))
        self.check(term=t1, start=start, new_start=start, end=int(end - length/2))
        self.assertEqual(length / 2, t1.get_length())

    def test_extend(self):
        start_ms = 1000
        end_ms = 1499
        length_ms = 500
        t = Term(start=ActorClock.from_milliseconds(milli_seconds=start_ms),
                 end=ActorClock.from_milliseconds(milli_seconds=end_ms))
        self.assertEqual(length_ms, t.get_length())
        self.check(term=t, start=start_ms, new_start=start_ms, end=end_ms)

        # extend with same length
        t1 = t.extend()
        self.check(term=t1, start=start_ms, new_start=end_ms + 1, end=end_ms + length_ms)
        self.assertEqual(length_ms, t1.get_length())
        self.assertEqual(2 * length_ms, t1.get_full_length())
        self.assertTrue(t1.extends_term(old_term=t))

        # extend multiple times
        for i in range(10):
            t2 = t1.extend()
            self.check(start=ActorClock.to_milliseconds(when=t1.get_start_time()),
                       new_start=ActorClock.to_milliseconds(when=t1.get_end_time()) + 1,
                       end=ActorClock.to_milliseconds(when=t1.get_end_time()) + length_ms,
                       term=t2)
            self.assertEqual(length_ms, t2.get_length())
            self.assertEqual(t1.get_full_length() + length_ms, t2.get_full_length())
            self.assertTrue(t2.extends_term(old_term=t1))
            t1 = t2

        # extend with 1000
        l = 1000
        t1 = t.extend(length=l)
        self.check(start=start_ms, new_start=end_ms+1, end=end_ms+l, term=t1)
        self.assertEqual(l, t1.get_length())
        self.assertEqual(l + length_ms, t1.get_full_length())

        # extend multiple times
        for i in range(10):
            t2 = t1.extend()
            self.check(start=ActorClock.to_milliseconds(when=t1.get_start_time()),
                       new_start=ActorClock.to_milliseconds(when=t1.get_end_time()) + 1,
                       end=ActorClock.to_milliseconds(when=t1.get_end_time()) + l,
                       term=t2)
            self.assertEqual(l, t2.get_length())
            self.assertEqual(t1.get_full_length() + l, t2.get_full_length())
            self.assertTrue(t2.extends_term(old_term=t2))
            t1 = t2

    def test_contains(self):
        start = 1000
        end = 1499
        length = 500
        t = Term(start=ActorClock.from_milliseconds(milli_seconds=start), end=ActorClock.from_milliseconds(milli_seconds=end))
        self.check(term=t, start=start, new_start=start, end=end)
        self.assertEqual(length, t.get_length())
        # self -> true
        self.assertTrue(t.contains(term=t))
        # self - 1 from the right
        self.assertTrue(t.contains(term=Term(start=ActorClock.from_milliseconds(milli_seconds=start), length=length-1)))
        # self + 1 from the left
        self.assertTrue(t.contains(term=Term(start=ActorClock.from_milliseconds(milli_seconds=start),
                                             end=ActorClock.from_milliseconds(milli_seconds=end))))
        # self +1 from the left, -1 from the right
        self.assertTrue(t.contains(term=Term(start=ActorClock.from_milliseconds(milli_seconds=start),
                                             end=ActorClock.from_milliseconds(milli_seconds=end - 1))))

        self.assertFalse(t.contains(term=Term(start=ActorClock.from_milliseconds(milli_seconds=start -1),
                                              end=ActorClock.from_milliseconds(milli_seconds=end))))
        self.assertFalse(t.contains(term=Term(start=ActorClock.from_milliseconds(milli_seconds=start -100),
                                              end=ActorClock.from_milliseconds(milli_seconds=start))))

        self.assertFalse(t.contains(term=Term(start=ActorClock.from_milliseconds(milli_seconds=start),
                                              end=ActorClock.from_milliseconds(milli_seconds=end + 1))))
        self.assertFalse(t.contains(term=Term(start=ActorClock.from_milliseconds(milli_seconds=start),
                                              end=ActorClock.from_milliseconds(milli_seconds=end + 100))))

        self.assertFalse(t.contains(term=Term(start=ActorClock.from_milliseconds(milli_seconds=start - 100),
                                              end=ActorClock.from_milliseconds(milli_seconds=end + 100))))

        self.assertTrue(t.contains(date=ActorClock.from_milliseconds(milli_seconds=start)))
        self.assertTrue(t.contains(date=ActorClock.from_milliseconds(milli_seconds=end)))
        self.assertTrue(t.contains(date=ActorClock.from_milliseconds(milli_seconds=int((start + end)/2))))

        self.assertFalse(t.contains(date=ActorClock.from_milliseconds(milli_seconds=start -1)))
        self.assertFalse(t.contains(date=ActorClock.from_milliseconds(milli_seconds=end + 1)))

    def test_ends(self):
        start = 1000
        end = 1499
        length = 500
        t = Term(start=ActorClock.from_milliseconds(milli_seconds=start),
                 end=ActorClock.from_milliseconds(milli_seconds=end))
        self.check(term=t, start=start, new_start=start, end=end)
        self.assertEqual(length, t.get_length())
        # term cannot end before it started
        self.assertFalse(t.ends_before(date=t.get_start_time()))
        self.assertFalse(t.expired(date=t.get_start_time()))
        self.assertTrue(t.ends_after(date=t.get_start_time()))

        self.assertFalse(t.ends_before(date=t.get_end_time()))
        self.assertFalse(t.ends_after(date=t.get_end_time()))
        self.assertFalse(t.expired(date=t.get_end_time()))

        self.assertFalse(t.ends_before(date=ActorClock.from_milliseconds(milli_seconds=start - 100)))
        self.assertTrue(t.ends_after(date=ActorClock.from_milliseconds(milli_seconds=start - 100)))
        self.assertFalse(t.expired(date=ActorClock.from_milliseconds(milli_seconds=start - 100)))

        self.assertTrue(t.ends_before(date=ActorClock.from_milliseconds(milli_seconds=end + 1)))
        self.assertTrue(t.expired(date=ActorClock.from_milliseconds(milli_seconds=end + 1)))
        self.assertFalse(t.ends_after(date=ActorClock.from_milliseconds(milli_seconds=end + 1)))

    def test_equals(self):
        start = 1000
        end = 1499
        length = 500
        t = Term(start=ActorClock.from_milliseconds(milli_seconds=start),
                 end=ActorClock.from_milliseconds(milli_seconds=end))
        self.check(term=t, start=start, new_start=start, end=end)
        self.assertEqual(length, t.get_length())

        self.assertTrue(t == t)

        t1 = Term(start=ActorClock.from_milliseconds(milli_seconds=start),
                  end=ActorClock.from_milliseconds(milli_seconds=end))
        self.check(term=t1, start=start, new_start=start, end=end)
        self.assertEqual(length, t1.get_length())

        t2 = Term(start=ActorClock.from_milliseconds(milli_seconds=start + 100),
                  end=ActorClock.from_milliseconds(milli_seconds=end))
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

    def check_not_valid(self, *, term: Term):
        failed = False

        try:
            term.validate()
        except Exception:
            failed = True

        self.assertTrue(failed)

    def test_validate(self):
        with self.assertRaises(Exception):
            self.check_valid(term=Term(start=ActorClock.from_milliseconds(milli_seconds=10),
                                  new_start=ActorClock.from_milliseconds(milli_seconds=100),
                                  end=ActorClock.from_milliseconds(milli_seconds=10)))
            self.check_valid(term=Term(start=ActorClock.from_milliseconds(milli_seconds=10),
                                  new_start=ActorClock.from_milliseconds(milli_seconds=100),
                                  end=ActorClock.from_milliseconds(milli_seconds=99)))
            self.check_valid(term=Term(start=ActorClock.from_milliseconds(milli_seconds=10),
                                  new_start=ActorClock.from_milliseconds(milli_seconds=1000),
                                  end=ActorClock.from_milliseconds(milli_seconds=1000)))

        self.check_not_valid(term=Term(start=ActorClock.from_milliseconds(milli_seconds=100),
                                  end=ActorClock.from_milliseconds(milli_seconds=10)))
        self.check_not_valid(term=Term(start=ActorClock.from_milliseconds(milli_seconds=100),
                                  end=ActorClock.from_milliseconds(milli_seconds=100)))
        self.check_not_valid(term=Term(start=ActorClock.from_milliseconds(milli_seconds=100),
                                  end=ActorClock.from_milliseconds(milli_seconds=1000),
                                  new_start=ActorClock.from_milliseconds(milli_seconds=10)))
        self.check_not_valid(term=Term(start=ActorClock.from_milliseconds(milli_seconds=100),
                                  end=ActorClock.from_milliseconds(milli_seconds=10),
                                  new_start=ActorClock.from_milliseconds(milli_seconds=10000)))


if __name__ == '__main__':
    unittest.main()
