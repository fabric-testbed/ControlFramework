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
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from fabric_cf.actor.core.kernel.predecessor_state import PredecessorState
from fabric_cf.actor.core.kernel.reservation import Reservation
from fabric_cf.actor.core.kernel.reservation_client import ReservationClient
from fabric_cf.actor.core.kernel.reservation_states import JoinState
from fabric_cf.actor.core.util.id import ID


class TestRenewPredecessorCoverage(unittest.TestCase):
    """
    A network service renew waits until its VM predecessors are extended to cover the new end. The VMs' terms
    come back from the broker/AM in milliseconds while a REST renew computes the new end with microseconds;
    a VM extended to the same end must count as covering it
    """
    NEW_END = datetime(2027, 5, 3, 16, 17, 27, 8417, tzinfo=timezone.utc)
    BLOCKED_AT = datetime(2026, 11, 2, 16, 17, 27, tzinfo=timezone.utc)

    @staticmethod
    def _term(*, end: datetime):
        # Term() needs the actor clock; only the end time matters here
        return SimpleNamespace(get_end_time=lambda: end, ends_after=lambda date: date < end)

    def _network_service(self, *, pred: MagicMock) -> ReservationClient:
        ns = ReservationClient.__new__(ReservationClient)
        ns.rid = ID(uid="ns")
        ns.approved_term = self._term(end=self.NEW_END)
        ns.joinstate = JoinState.BlockedExtendTicket
        ns.last_transition_time = self.BLOCKED_AT
        ns.error_message = ""
        ns.logger = MagicMock()
        ns.redeem_predecessors = {pred.get_reservation_id(): PredecessorState(reservation=pred)}
        return ns

    def _vm(self, *, end: datetime) -> MagicMock:
        vm = MagicMock(spec=Reservation)
        vm.get_reservation_id.return_value = ID(uid="vm")
        vm.get_term.return_value = self._term(end=end)
        vm.is_failed.return_value = False
        vm.is_closed.return_value = False
        vm.is_closing.return_value = False
        vm.is_ticketed.return_value = False
        vm.is_active.return_value = True
        vm.is_extending_ticket.return_value = False
        # The VM concluded its extend after the network service blocked on it
        vm.last_transition_time = self.BLOCKED_AT + timedelta(seconds=11)
        return vm

    def test_predecessor_extended_to_same_end_in_milliseconds_covers(self):
        vm = self._vm(end=self.NEW_END.replace(microsecond=8000))
        approved, rollback, failed = self._network_service(pred=vm).approve_extend_ticket()
        self.assertTrue(approved)
        self.assertFalse(rollback)
        self.assertEqual(failed, [])

    def test_predecessor_not_extended_rolls_back(self):
        vm = self._vm(end=self.NEW_END - timedelta(days=180))
        ns = self._network_service(pred=vm)
        approved, rollback, _ = ns.approve_extend_ticket()
        self.assertTrue(rollback)
        self.assertIn("could not be extended", ns.error_message)


if __name__ == '__main__':
    unittest.main()
