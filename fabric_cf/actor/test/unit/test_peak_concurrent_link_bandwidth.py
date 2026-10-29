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
from types import SimpleNamespace
from unittest.mock import MagicMock

from fabric_cf.actor.core.util.time_window import usage_at_peaks
from fabric_cf.actor.db.psql_database import PsqlDatabase


class TestPeakConcurrentLinkBandwidth(unittest.TestCase):
    """
    Bandwidth allocated on a link must only be accounted for the reservations which are held concurrently
    within the requested window; reservations which do not overlap each other must not be summed.

    No infrastructure (Neo4j/Postgres/Kafka) required.
    """
    T0 = datetime(2026, 11, 1, tzinfo=timezone.utc)
    LINK = "link:local-port+renc-data-sw:HundredGigE0/0/0/5:remote-port+uky-data-sw:HundredGigE0/0/0/5"

    @classmethod
    def day(cls, n: float) -> datetime:
        return cls.T0 + timedelta(days=n)

    def test_usage_at_peaks_scalar(self):
        allocations = [((self.day(1), self.day(3)), 40), ((self.day(5), self.day(7)), 40),
                       ((self.day(2), self.day(4)), 30)]
        self.assertEqual(max(usage_at_peaks(allocations=allocations, start=self.day(0), end=self.day(10), zero=0)), 70)
        self.assertEqual(max(usage_at_peaks(allocations=allocations, start=self.day(4), end=self.day(10), zero=0)), 40)
        # Without a window, everything is summed
        self.assertEqual(max(usage_at_peaks(allocations=allocations, start=None, end=None, zero=0)), 110)

    def test_usage_at_peaks_empty(self):
        self.assertEqual(max(usage_at_peaks(allocations=[], start=self.day(0), end=self.day(1), zero=0)), 0)

    def db_with_rows(self, rows):
        db = PsqlDatabase.__new__(PsqlDatabase)
        db.logger = logging.getLogger("test-peak-link-bw")
        query = MagicMock()
        for m in ["join", "filter", "options"]:
            getattr(query, m).return_value = query
        query.all.return_value = rows
        session = MagicMock()
        session.query.return_value = query
        db.get_session = lambda: session
        return db

    def row(self, bw, start, end):
        return SimpleNamespace(node_id=self.LINK, bw=bw,
                               reservation=SimpleNamespace(lease_start=start, lease_end=end))

    def test_get_links_peak(self):
        rows = [self.row(40, self.day(1), self.day(3)), self.row(40, self.day(5), self.day(7))]
        db = self.db_with_rows(rows)
        self.assertEqual(db.get_links(node_id=self.LINK, states=[], rsv_type=[], start=self.day(0), end=self.day(10),
                                      peak=True), {self.LINK: 40})

    def test_get_links_sum_by_default(self):
        rows = [self.row(40, self.day(1), self.day(3)), self.row(40, self.day(5), self.day(7))]
        db = self.db_with_rows(rows)
        self.assertEqual(db.get_links(node_id=self.LINK, states=[], rsv_type=[], start=self.day(0), end=self.day(10)),
                         {self.LINK: 80})


if __name__ == '__main__':
    unittest.main()
