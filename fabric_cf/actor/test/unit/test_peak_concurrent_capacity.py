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
import logging
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from fim.slivers.capacities_labels import Capacities
from fim.slivers.delegations import Delegation, Delegations, DelegationType
from fim.slivers.network_node import NodeSliver

from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.policy.network_node_inventory import NetworkNodeInventory
from fabric_cf.actor.core.util.id import ID


class TestPeakConcurrentCapacity(unittest.TestCase):
    """
    Capacities allocated to existing reservations on a worker must only be accounted for the reservations
    which are held concurrently within the requested window.

    Summing every reservation overlapping the requested window over-counts reservations which do not overlap
    each other (e.g. two 32 core reservations on consecutive days on a 64 core worker), failing requests
    (including renewals) that could be satisfied.

    No infrastructure (Neo4j/Postgres/Kafka) required.
    """
    T0 = datetime(2026, 11, 1, tzinfo=timezone.utc)
    logger = logging.getLogger("test-peak-concurrent-capacity")

    @classmethod
    def day(cls, n: float) -> datetime:
        return cls.T0 + timedelta(days=n)

    @staticmethod
    def term(start, end):
        if start is None and end is None:
            return None
        t = MagicMock()
        t.get_start_time.return_value = start
        t.get_end_time.return_value = end
        return t

    def reservation(self, rid: str, core: int, start, end, requested_end=None):
        r = MagicMock()
        r.get_reservation_id.return_value = ID(uid=rid)
        r.is_ticketing.return_value = False
        r.is_active.return_value = True
        sliver = NodeSliver()
        sliver.set_capacity_allocations(cap=Capacities(core=core, ram=8, disk=10))
        r.get_resources.return_value.get_sliver.return_value = sliver
        r.get_term.return_value = self.term(start, end)
        r.get_approved_term.return_value = None
        r.get_requested_term.return_value = self.term(start, requested_end) if requested_end else None
        return r

    @staticmethod
    def delegations(core: int = 64) -> Delegations:
        delegation = Delegation(atype=DelegationType.CAPACITY, delegation_id="del1")
        delegation.set_details(Capacities(core=core, ram=512, disk=2000))
        delegations = Delegations(atype=DelegationType.CAPACITY)
        delegations.add_delegations(delegation)
        return delegations

    def check(self, existing, start, end, core: int = 16) -> str:
        return NetworkNodeInventory.check_capacities(rid=ID(uid="me"),
                                                     requested_capacities=Capacities(core=core, ram=8, disk=10),
                                                     delegated=self.delegations(), existing_reservations=existing,
                                                     logger=self.logger, start=start, end=end)

    def assert_insufficient(self, existing, start, end, core: int = 16):
        with self.assertRaises(BrokerException) as ctx:
            self.check(existing, start, end, core=core)
        self.assertEqual(ctx.exception.error_code, ExceptionErrorCode.INSUFFICIENT_RESOURCES)
        self.assertIn("core", ctx.exception.msg)

    def test_non_overlapping_reservations_are_not_summed(self):
        x = self.reservation("x", 32, self.day(1), self.day(3))
        y = self.reservation("y", 32, self.day(5), self.day(7))
        self.assertEqual(self.check([x, y], self.day(0), self.day(10)), "del1")

    def test_overlapping_reservations_are_summed(self):
        x = self.reservation("x", 32, self.day(1), self.day(3))
        z = self.reservation("z", 32, self.day(2), self.day(4))
        self.assert_insufficient([x, z], self.day(0), self.day(10))

    def test_peak_is_used_among_mixed_reservations(self):
        x = self.reservation("x", 32, self.day(1), self.day(3))
        y = self.reservation("y", 32, self.day(5), self.day(7))
        z = self.reservation("z", 32, self.day(2), self.day(4))
        self.assert_insufficient([x, y, z], self.day(0), self.day(10))
        # Peak is 32 within a window covering only z
        self.assertEqual(self.check([x, z], self.day(3.5), self.day(10)), "del1")

    def test_reservation_ending_at_window_start_is_not_counted(self):
        a = self.reservation("a", 64, self.day(-2), self.day(0))
        self.assertEqual(self.check([a], self.day(0), self.day(2)), "del1")

    def test_reservation_active_at_window_start_is_counted(self):
        b = self.reservation("b", 64, self.day(-2), self.day(1))
        self.assert_insufficient([b], self.day(0), self.day(2))

    def test_extending_reservation_counted_till_requested_end(self):
        c = self.reservation("c", 64, self.day(-2), self.day(0), requested_end=self.day(5))
        self.assert_insufficient([c], self.day(1), self.day(2))

    def test_naive_datetimes_are_treated_as_utc(self):
        n = self.reservation("n", 64, datetime(2026, 11, 2), datetime(2026, 11, 3))
        self.assert_insufficient([n], self.day(0), self.day(10))

    def test_reservation_with_unknown_window_is_held_throughout(self):
        u = self.reservation("u", 64, None, None)
        self.assert_insufficient([u], self.day(0), self.day(10))

    def test_without_requested_window_all_reservations_are_summed(self):
        x = self.reservation("x", 32, self.day(1), self.day(3))
        y = self.reservation("y", 32, self.day(5), self.day(7))
        self.assert_insufficient([x, y], None, None)

    def test_own_reservation_is_excluded(self):
        me = self.reservation("me", 64, self.day(0), self.day(10))
        self.assertEqual(self.check([me], self.day(0), self.day(10)), "del1")


if __name__ == '__main__':
    unittest.main()
