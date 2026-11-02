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
from types import SimpleNamespace
from unittest.mock import MagicMock

from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
from fabric_cf.actor.core.core.actor import ActorMixin
from fabric_cf.actor.core.kernel.predecessor_state import PredecessorState
from fabric_cf.actor.core.util.id import ID


class TestRebindPredecessors(unittest.TestCase):
    """
    Predecessors of reservations recovered from the database are bound to separate copies loaded from the
    database; recovery must re-point them at the reservations registered with the kernel so that predecessor
    checks (e.g. a network service renew waiting on its VMs) see live state and term
    """

    @staticmethod
    def _reservation(rid: str):
        return SimpleNamespace(get_reservation_id=lambda: ID(uid=rid))

    @staticmethod
    def _controller_reservation(redeem: list, join: list = None):
        r = MagicMock(spec=ABCControllerReservation)
        r.get_redeem_predecessors.return_value = redeem
        r.get_join_predecessors.return_value = join or []
        return r

    def _rebind(self, reservations: list, live: dict):
        actor = SimpleNamespace(get_reservation=lambda rid: live.get(str(rid)))
        ActorMixin.rebind_predecessors(actor, reservations=reservations)

    def test_predecessors_rebound_to_live_reservations(self):
        stale_vm = self._reservation("vm")
        stale_switch = self._reservation("switch")
        live_vm = self._reservation("vm")
        live_switch = self._reservation("switch")
        redeem = PredecessorState(reservation=stale_vm)
        join = PredecessorState(reservation=stale_switch)
        ns = self._controller_reservation(redeem=[redeem], join=[join])

        self._rebind(reservations=[ns], live={"vm": live_vm, "switch": live_switch})

        self.assertIs(redeem.get_reservation(), live_vm)
        self.assertIs(join.get_reservation(), live_switch)

    def test_unregistered_predecessor_left_untouched(self):
        # Closed reservations are not registered with the kernel on recovery
        stale_vm = self._reservation("vm")
        redeem = PredecessorState(reservation=stale_vm)
        ns = self._controller_reservation(redeem=[redeem])

        self._rebind(reservations=[ns], live={})

        self.assertIs(redeem.get_reservation(), stale_vm)

    def test_non_controller_reservations_ignored(self):
        other = MagicMock()
        self._rebind(reservations=[other], live={})
        other.get_redeem_predecessors.assert_not_called()


if __name__ == '__main__':
    unittest.main()
