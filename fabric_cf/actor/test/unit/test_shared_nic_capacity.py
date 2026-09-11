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

from fim.slivers.attached_components import ComponentSliver, ComponentType, AttachedComponentsInfo
from fim.slivers.capacities_labels import Capacities, Labels
from fim.slivers.delegations import Delegation, Delegations, DelegationType
from fim.slivers.interface_info import InterfaceInfo, InterfaceSliver
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceInfo, NetworkServiceSliver

from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.policy.network_node_inventory import NetworkNodeInventory


class TestSharedNicCapacityTracking(unittest.TestCase):
    """
    The delegated capacity of a Shared NIC must stay in step with the PCI addresses left in its
    label delegation.

    Capacities defines __sub__ but not __isub__, so decrementing it with `-=` built a new object
    and rebound the local, leaving the delegation holding its original unit count - while the BDF
    list beside it was mutated in place. A fully allocated card was therefore never excluded from
    the candidate node, and the next allocation indexed an empty BDF list.

    No infrastructure (Neo4j/Postgres/Kafka) required.
    """
    DELEGATION_ID = "del1"
    NIC_NAME = "renc-w1-slot6"
    MODEL = "ConnectX-6"
    VF_BDFS = ["0000:41:00.2", "0000:41:00.3", "0000:41:00.4"]
    VF_VLANS = ["2112", "2118", "2120"]
    VF_MACS = ["0C:42:A1:EA:C7:52", "0C:42:A1:EA:C7:53", "0C:42:A1:EA:C7:54"]
    NUMA = "1"

    logger = logging.getLogger("test-shared-nic-capacity")

    # ── builders ──────────────────────────────────────────────────────────

    @classmethod
    def _delegations(cls, details, atype: DelegationType) -> Delegations:
        delegation = Delegation(atype=atype, delegation_id=cls.DELEGATION_ID)
        delegation.set_details(details)
        delegations = Delegations(atype=atype)
        delegations.add_delegations(delegation)
        return delegations

    @classmethod
    def _available_shared_nic(cls) -> ComponentSliver:
        component = ComponentSliver()
        component.set_type(ComponentType.SharedNIC)
        component.set_name(resource_name=cls.NIC_NAME)
        component.set_model(resource_model=cls.MODEL)
        component.set_capacity_delegations(
            cls._delegations(Capacities(unit=len(cls.VF_BDFS)), DelegationType.CAPACITY))
        component.set_label_delegations(cls._delegations(
            Labels(bdf=list(cls.VF_BDFS), numa=[cls.NUMA] * len(cls.VF_BDFS)),
            DelegationType.LABEL))

        ifs = InterfaceSliver()
        ifs.set_name(resource_name="p1")
        ifs.set_label_delegations(cls._delegations(
            Labels(bdf=list(cls.VF_BDFS), vlan=list(cls.VF_VLANS), mac=list(cls.VF_MACS),
                   local_name=["p1"] * len(cls.VF_BDFS)),
            DelegationType.LABEL))
        interface_info = InterfaceInfo()
        interface_info.add_interface(ifs)

        ns = NetworkServiceSliver()
        ns.set_name(resource_name=f"{cls.NIC_NAME}-ns")
        ns.interface_info = interface_info

        component.network_service_info = NetworkServiceInfo()
        component.network_service_info.add_network_service(ns_sliver=ns)
        return component

    @classmethod
    def _allocated_vf(cls, bdf: str) -> ComponentSliver:
        allocated = ComponentSliver()
        allocated.set_type(ComponentType.SharedNIC)
        allocated.set_name(resource_name="nic1")
        allocated.set_model(resource_model=cls.MODEL)
        allocated.set_labels(lab=Labels(bdf=bdf))
        allocated.capacity_allocations = Capacities(unit=1)
        return allocated

    # ── private broker entry points ───────────────────────────────────────

    def _exclude_vf(self, shared: ComponentSliver, bdf: str):
        return NetworkNodeInventory._NetworkNodeInventory__exclude_allocated_pci_device_from_shared_nic(
            shared=shared, allocated=self._allocated_vf(bdf), logger=self.logger)

    @staticmethod
    def _details(delegations):
        from fabric_cf.actor.fim.fim_helper import FimHelper
        return FimHelper.get_delegations(delegations=delegations)[1]

    @classmethod
    def _capacities(cls, component: ComponentSliver) -> Capacities:
        return cls._details(component.get_capacity_delegations())

    @classmethod
    def _labels(cls, component: ComponentSliver) -> Labels:
        return cls._details(component.get_label_delegations())

    # ── tests ─────────────────────────────────────────────────────────────

    def test_capacity_follows_remaining_bdfs(self):
        shared = self._available_shared_nic()
        for i, bdf in enumerate(self.VF_BDFS):
            _, exhausted = self._exclude_vf(shared, bdf)
            expected = len(self.VF_BDFS) - (i + 1)
            self.assertEqual(expected, len(self._labels(shared).bdf))
            self.assertEqual(expected, self._capacities(shared).unit,
                             "delegated capacity drifted from the remaining PCI addresses")
            self.assertEqual(expected < 1, exhausted)

    def test_last_bdf_reports_exhausted(self):
        shared = self._available_shared_nic()
        for bdf in self.VF_BDFS[:-1]:
            _, exhausted = self._exclude_vf(shared, bdf)
            self.assertFalse(exhausted)
        _, exhausted = self._exclude_vf(shared, self.VF_BDFS[-1])
        self.assertTrue(exhausted)

    def test_unknown_bdf_leaves_capacity_alone(self):
        shared = self._available_shared_nic()
        _, exhausted = self._exclude_vf(shared, "0000:99:00.9")
        self.assertFalse(exhausted)
        self.assertEqual(len(self.VF_BDFS), self._capacities(shared).unit)
        self.assertEqual(self.VF_BDFS, self._labels(shared).bdf)

    def test_exhausted_card_is_removed_from_candidate_node(self):
        node = NodeSliver()
        node.set_name(resource_name="renc-w1")
        node.attached_components_info = AttachedComponentsInfo()
        node.attached_components_info.add_device(self._available_shared_nic())

        for bdf in self.VF_BDFS:
            available = node.attached_components_info.devices.get(self.NIC_NAME)
            if available is None:
                break
            NetworkNodeInventory._NetworkNodeInventory__exclude_allocated_component(
                graph_node=node, available=available, allocated=self._allocated_vf(bdf),
                logger=self.logger)

        self.assertNotIn(self.NIC_NAME, node.attached_components_info.devices)

    def test_exhausted_card_raises_insufficient_resources_not_index_error(self):
        """
        The regression this guards: with capacity stuck at its original value the exhausted card
        stayed in the candidate node, and the next allocation reached usable_bdfs[0] on an empty
        list and raised IndexError instead of a broker error the caller can act on.
        """
        shared = self._available_shared_nic()
        for bdf in self.VF_BDFS:
            self._exclude_vf(shared, bdf)

        self.assertEqual(0, self._capacities(shared).unit)

        requested = ComponentSliver()
        requested.set_type(ComponentType.SharedNIC)
        requested.set_name(resource_name="nic1")
        requested.set_model(resource_model=self.MODEL)

        with self.assertRaises(BrokerException) as ctx:
            NetworkNodeInventory._NetworkNodeInventory__check_component_labels_and_capacities(
                available=shared, graph_id="graph-1", requested=requested, logger=self.logger)
        self.assertEqual(ExceptionErrorCode.INSUFFICIENT_RESOURCES, ctx.exception.error_code)

    def test_missing_label_delegation_is_reported_as_exhausted(self):
        """Previously raised AttributeError on delegated_label.bdf."""
        shared = self._available_shared_nic()
        shared.set_label_delegations(self._delegations(Labels(numa=[self.NUMA]),
                                                       DelegationType.LABEL))
        _, exhausted = self._exclude_vf(shared, self.VF_BDFS[0])
        self.assertTrue(exhausted)


if __name__ == '__main__':
    unittest.main()
