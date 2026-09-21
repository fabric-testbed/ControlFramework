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

from fim.slivers.capacities_labels import Capacities
from fim.slivers.network_link import NetworkLinkSliver

from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.policy.broker_simpler_units_policy import BrokerSimplerUnitsPolicy


class TestEroLinkBandwidth(unittest.TestCase):
    """
    An inter-site Link node in the model is not guaranteed to advertise bandwidth: the ABQM only
    copies Capacities/CapacityAllocations onto the link when the CBM has them, and the network ARM
    leaves them unset on some links.

    Reading `link_sliver.capacities.bw` unconditionally therefore raised
    `AttributeError: 'NoneType' object has no attribute 'bw'`, which propagated out of
    ticket_inventory and failed the whole L2PTP ticket with a message naming neither the link nor
    the cause. The check must instead name the offending link, and must disqualify only the
    candidate path it appears on.

    No infrastructure (Neo4j/Postgres/Kafka) required.
    """
    LINK_ID = ("link:local-port+cape-data-sw:Genericintf1:"
               "remote-port+star-data-sw:FourHundredGigE0/0/0/60.1400")
    LINK_NAME = "port+cape-data-sw:Genericintf1 to port+star-data-sw:FourHundredGigE0/0/0/60.1400"

    logger = logging.getLogger("test-ero-link-bandwidth")

    # ── builders ──────────────────────────────────────────────────────────

    @staticmethod
    def _link_sliver(*, capacities: Capacities = None, capacity_allocations: Capacities = None,
                     name: str = LINK_NAME) -> NetworkLinkSliver:
        sliver = NetworkLinkSliver()
        sliver.node_id = TestEroLinkBandwidth.LINK_ID
        sliver.set_properties(name=name, type=sliver.type_from_str("L1Path"),
                              capacities=capacities, capacity_allocations=capacity_allocations)
        return sliver

    def _policy(self, *, link_sliver: NetworkLinkSliver, existing: dict = None) -> BrokerSimplerUnitsPolicy:
        """
        A policy wired up with just enough state for _is_link_allowed: a stub ABQM returning the
        given link sliver, and existing link usage served without touching the database.
        """
        policy = BrokerSimplerUnitsPolicy(actor=None)
        policy.logger = self.logger

        class StubAbqm:
            @staticmethod
            def build_deep_link_sliver(*, node_id: str) -> NetworkLinkSliver:
                return link_sliver

        policy.abqm = StubAbqm()
        policy.get_existing_links = lambda **kwargs: existing or {}
        return policy

    def _is_link_allowed(self, policy: BrokerSimplerUnitsPolicy, requested_bw: int) -> bool:
        now = datetime.now(timezone.utc)
        return policy._is_link_allowed(link_id=self.LINK_ID, requested_bw=requested_bw,
                                       reservation_id="rid-1", start=now, end=now + timedelta(days=1),
                                       node_id_to_reservations={})

    # ── tests ─────────────────────────────────────────────────────────────

    def test_link_without_bandwidth_names_the_link(self):
        """A link advertising no bandwidth must raise a BrokerException naming it, not AttributeError."""
        policy = self._policy(link_sliver=self._link_sliver())

        with self.assertRaises(BrokerException) as ctx:
            self._is_link_allowed(policy, requested_bw=8)

        self.assertEqual(ExceptionErrorCode.INVALID_ARGUMENT, ctx.exception.error_code)
        self.assertIn(self.LINK_ID, ctx.exception.msg)
        self.assertIn(self.LINK_NAME, ctx.exception.msg)
        self.assertIn("L1Path", ctx.exception.msg)

    def test_link_with_zero_bandwidth_names_the_link(self):
        """An empty Capacities object is truthy with bw == 0; it carries no more information than None."""
        policy = self._policy(link_sliver=self._link_sliver(capacities=Capacities()))

        with self.assertRaises(BrokerException) as ctx:
            self._is_link_allowed(policy, requested_bw=8)

        self.assertIn(self.LINK_ID, ctx.exception.msg)

    def test_capacities_used_when_no_allocations_advertised(self):
        policy = self._policy(link_sliver=self._link_sliver(capacities=Capacities(bw=100)))

        self.assertTrue(self._is_link_allowed(policy, requested_bw=100))
        self.assertFalse(self._is_link_allowed(policy, requested_bw=101))

    def test_allocations_take_precedence_as_the_usable_budget(self):
        """CapacityAllocations on a link is the administratively usable share, not consumed bandwidth."""
        policy = self._policy(link_sliver=self._link_sliver(capacities=Capacities(bw=100),
                                                            capacity_allocations=Capacities(bw=80)))

        self.assertTrue(self._is_link_allowed(policy, requested_bw=80))
        self.assertFalse(self._is_link_allowed(policy, requested_bw=81))

    def test_existing_reservations_are_deducted(self):
        policy = self._policy(link_sliver=self._link_sliver(capacities=Capacities(bw=100)),
                              existing={self.LINK_ID: 70})

        self.assertTrue(self._is_link_allowed(policy, requested_bw=30))
        self.assertFalse(self._is_link_allowed(policy, requested_bw=31))


if __name__ == "__main__":
    unittest.main()
