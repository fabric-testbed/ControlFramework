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
from fim.slivers.network_service import NetworkServiceInfo, NetworkServiceSliver, NSLayer

from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.policy.network_node_inventory import NetworkNodeInventory


class TestMixedModeNic(unittest.TestCase):
    """
    A worker with one ConnectX-6 whose PF0 is in SR-IOV/shared mode and whose PF1 is dedicated.
    The two halves are separate components and must be allocated independently.
    No infrastructure (Neo4j/Postgres/Kafka) required.
    """
    DELEGATION_ID = "del1"

    SLOT = "6"
    # PF0 serves the VFs, PF1 is passed through whole
    PF_DEDICATED = "0000:41:00.1"
    VF_BDFS = ["0000:41:00.2", "0000:41:00.3", "0000:41:00.4"]
    VF_VLANS = ["2112", "2118", "2120"]
    VF_MACS = ["0C:42:A1:EA:C7:52", "0C:42:A1:EA:C7:53", "0C:42:A1:EA:C7:54"]
    DEDICATED_MAC = "0C:42:A1:EA:C7:51"
    NUMA = "1"

    SHARED_NAME = "cape-w1-slot6"
    DEDICATED_NAME = "cape-w1-slot6-f1"
    SHARED_MODEL = "ConnectX-6"
    DEDICATED_MODEL = "ConnectX-6-1P"

    logger = logging.getLogger("test-mixed-mode-nic")

    # ── builders ──────────────────────────────────────────────────────────

    @classmethod
    def _delegations(cls, details, atype: DelegationType) -> Delegations:
        delegation = Delegation(atype=atype, delegation_id=cls.DELEGATION_ID)
        delegation.set_details(details)
        delegations = Delegations(atype=atype)
        delegations.add_delegations(delegation)
        return delegations

    @classmethod
    def _available_dedicated(cls) -> ComponentSliver:
        """The dedicated half of the card: one PF, one port, full VLAN range."""
        component = ComponentSliver()
        component.set_type(ComponentType.SmartNIC)
        component.set_name(resource_name=cls.DEDICATED_NAME)
        component.set_model(resource_model=cls.DEDICATED_MODEL)
        component.set_capacity_delegations(cls._delegations(Capacities(unit=1),
                                                            DelegationType.CAPACITY))
        component.set_label_delegations(cls._delegations(
            Labels(bdf=[cls.PF_DEDICATED], numa=[cls.NUMA]), DelegationType.LABEL))

        ifs = InterfaceSliver()
        ifs.set_name(resource_name="p1")
        ifs.set_label_delegations(cls._delegations(
            Labels(mac=cls.DEDICATED_MAC, local_name="p1", vlan_range="1-4096"),
            DelegationType.LABEL))
        interface_info = InterfaceInfo()
        interface_info.add_interface(ifs)

        ns = NetworkServiceSliver()
        ns.set_name(resource_name=f"{cls.DEDICATED_NAME}-ns")
        ns.interface_info = interface_info

        component.network_service_info = NetworkServiceInfo()
        component.network_service_info.add_network_service(ns_sliver=ns)
        return component

    @classmethod
    def _available_shared(cls) -> ComponentSliver:
        """The shared half of the same card: VFs of PF0."""
        component = ComponentSliver()
        component.set_type(ComponentType.SharedNIC)
        component.set_name(resource_name=cls.SHARED_NAME)
        component.set_model(resource_model=cls.SHARED_MODEL)
        component.set_capacity_delegations(cls._delegations(Capacities(unit=len(cls.VF_BDFS)),
                                                            DelegationType.CAPACITY))
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
        ns.set_name(resource_name=f"{cls.SHARED_NAME}-ns")
        ns.interface_info = interface_info

        component.network_service_info = NetworkServiceInfo()
        component.network_service_info.add_network_service(ns_sliver=ns)
        return component

    @classmethod
    def _requested_dedicated(cls, model: str = DEDICATED_MODEL) -> ComponentSliver:
        component = ComponentSliver()
        component.set_type(ComponentType.SmartNIC)
        component.set_name(resource_name="nic1")
        component.set_model(resource_model=model)

        ifs = InterfaceSliver()
        ifs.set_name(resource_name="nic1-p1")
        ifs.set_labels(lab=Labels(local_name="p1"))
        interface_info = InterfaceInfo()
        interface_info.add_interface(ifs)

        ns = NetworkServiceSliver()
        ns.set_name(resource_name="nic1-ns")
        ns.set_layer(layer=NSLayer.L2)
        ns.interface_info = interface_info

        component.network_service_info = NetworkServiceInfo()
        component.network_service_info.add_network_service(ns_sliver=ns)
        return component

    @classmethod
    def _worker(cls) -> NodeSliver:
        """A worker carrying both halves of the same physical card."""
        node = NodeSliver()
        node.set_name(resource_name="cape-w1")
        aci = AttachedComponentsInfo()
        aci.add_device(cls._available_shared())
        aci.add_device(cls._available_dedicated())
        node.attached_components_info = aci
        return node

    # ── private broker entry points ───────────────────────────────────────

    @staticmethod
    def _check(available: ComponentSliver, requested: ComponentSliver) -> ComponentSliver:
        return NetworkNodeInventory._NetworkNodeInventory__check_component_labels_and_capacities(
            available=available, graph_id="graph-1", requested=requested,
            logger=TestMixedModeNic.logger)

    @staticmethod
    def _exclude(graph_node: NodeSliver, available: ComponentSliver, allocated: ComponentSliver):
        NetworkNodeInventory._NetworkNodeInventory__exclude_allocated_component(
            graph_node=graph_node, available=available, allocated=allocated,
            logger=TestMixedModeNic.logger)

    @staticmethod
    def _allocated_ifs(component: ComponentSliver) -> InterfaceSliver:
        ns = next(iter(component.network_service_info.network_services.values()))
        return next(iter(ns.interface_info.interfaces.values()))

    # ── tests ─────────────────────────────────────────────────────────────

    def test_dedicated_half_allocates_its_single_port(self):
        allocated = self._check(self._available_dedicated(), self._requested_dedicated())
        self.assertEqual(1, allocated.capacity_allocations.unit)
        self.assertEqual([self.PF_DEDICATED], allocated.label_allocations.bdf)
        ifs_labels = self._allocated_ifs(allocated).get_label_allocations()
        self.assertEqual(self.DEDICATED_MAC, ifs_labels.mac)
        self.assertEqual("p1", ifs_labels.local_name)

    def test_two_port_request_does_not_match_single_port_card(self):
        """
        The single-port model exists precisely so a ConnectX-6 request, which carries p1 and p2,
        cannot be satisfied by a card offering one port.
        """
        requested = self._requested_dedicated(model=self.SHARED_MODEL)
        result = self._check(self._available_dedicated(), requested)
        self.assertIsNone(result.capacity_allocations)
        self.assertIsNone(result.label_allocations)

    def test_allocating_dedicated_half_leaves_shared_half_intact(self):
        node = self._worker()
        allocated = self._check(self._available_dedicated(), self._requested_dedicated())

        self._exclude(graph_node=node,
                      available=node.attached_components_info.get_device(self.DEDICATED_NAME),
                      allocated=allocated)

        remaining = node.attached_components_info.devices
        self.assertNotIn(self.DEDICATED_NAME, remaining)
        self.assertIn(self.SHARED_NAME, remaining)

        shared = node.attached_components_info.get_device(self.SHARED_NAME)
        _, labels = self._label_details(shared)
        self.assertEqual(self.VF_BDFS, labels.bdf)

    def test_allocating_a_vf_leaves_dedicated_half_intact(self):
        node = self._worker()
        allocated = ComponentSliver()
        allocated.set_type(ComponentType.SharedNIC)
        allocated.set_name(resource_name="nic1")
        allocated.set_model(resource_model=self.SHARED_MODEL)
        allocated.set_labels(lab=Labels(bdf=self.VF_BDFS[0]))
        allocated.capacity_allocations = Capacities(unit=1)

        self._exclude(graph_node=node,
                      available=node.attached_components_info.get_device(self.SHARED_NAME),
                      allocated=allocated)

        remaining = node.attached_components_info.devices
        # VFs remain, so the shared component stays; the dedicated half is untouched either way
        self.assertIn(self.SHARED_NAME, remaining)
        self.assertIn(self.DEDICATED_NAME, remaining)

        shared = node.attached_components_info.get_device(self.SHARED_NAME)
        _, labels = self._label_details(shared)
        self.assertNotIn(self.VF_BDFS[0], labels.bdf)
        self.assertEqual(self.VF_BDFS[1:], labels.bdf)

        dedicated = node.attached_components_info.get_device(self.DEDICATED_NAME)
        _, ded_labels = self._label_details(dedicated)
        self.assertEqual([self.PF_DEDICATED], ded_labels.bdf)

    def test_exhausting_the_shared_half_never_touches_the_dedicated_half(self):
        node = self._worker()
        for bdf in self.VF_BDFS:
            allocated = ComponentSliver()
            allocated.set_type(ComponentType.SharedNIC)
            allocated.set_name(resource_name="nic1")
            allocated.set_model(resource_model=self.SHARED_MODEL)
            allocated.set_labels(lab=Labels(bdf=bdf))
            allocated.capacity_allocations = Capacities(unit=1)
            available = node.attached_components_info.devices.get(self.SHARED_NAME)
            if available is None:
                break
            self._exclude(graph_node=node, available=available, allocated=allocated)

        # every VF of PF0 is spoken for, so the shared half is gone from the candidate node
        self.assertNotIn(self.SHARED_NAME, node.attached_components_info.devices)

        # PF1 is a separate component and is completely unaffected
        self.assertIn(self.DEDICATED_NAME, node.attached_components_info.devices)
        dedicated = node.attached_components_info.get_device(self.DEDICATED_NAME)
        _, ded_labels = self._label_details(dedicated)
        self.assertEqual([self.PF_DEDICATED], ded_labels.bdf)
        _, ded_caps = self._capacity_details(dedicated)
        self.assertEqual(1, ded_caps.unit)

    @staticmethod
    def _label_details(component: ComponentSliver):
        from fabric_cf.actor.fim.fim_helper import FimHelper
        return FimHelper.get_delegations(delegations=component.get_label_delegations())

    @staticmethod
    def _capacity_details(component: ComponentSliver):
        from fabric_cf.actor.fim.fim_helper import FimHelper
        return FimHelper.get_delegations(delegations=component.get_capacity_delegations())


class TestSharedNicCapacityTracking(unittest.TestCase):
    """
    The delegated capacity of a Shared NIC must stay in step with the PCI addresses left in
    its label delegation. Capacities defines __sub__ but not __isub__, so decrementing it with
    `-=` used to rebind a local and leave the delegation untouched: the component was never
    excluded once fully allocated, and the next allocation indexed an empty BDF list.
    """
    logger = logging.getLogger("test-shared-nic-capacity")

    def _shared(self) -> ComponentSliver:
        return TestMixedModeNic._available_shared()

    def _allocated_vf(self, bdf: str) -> ComponentSliver:
        allocated = ComponentSliver()
        allocated.set_type(ComponentType.SharedNIC)
        allocated.set_name(resource_name="nic1")
        allocated.set_model(resource_model=TestMixedModeNic.SHARED_MODEL)
        allocated.set_labels(lab=Labels(bdf=bdf))
        allocated.capacity_allocations = Capacities(unit=1)
        return allocated

    def _exclude_vf(self, shared: ComponentSliver, bdf: str):
        return NetworkNodeInventory._NetworkNodeInventory__exclude_allocated_pci_device_from_shared_nic(
            shared=shared, allocated=self._allocated_vf(bdf), logger=self.logger)

    def test_capacity_follows_remaining_bdfs(self):
        shared = self._shared()
        for i, bdf in enumerate(TestMixedModeNic.VF_BDFS):
            _, exhausted = self._exclude_vf(shared, bdf)
            _, caps = TestMixedModeNic._capacity_details(shared)
            _, labels = TestMixedModeNic._label_details(shared)
            expected = len(TestMixedModeNic.VF_BDFS) - (i + 1)
            self.assertEqual(expected, len(labels.bdf))
            self.assertEqual(expected, caps.unit,
                             "delegated capacity drifted from the remaining PCI addresses")
            self.assertEqual(expected < 1, exhausted)

    def test_last_bdf_reports_exhausted(self):
        shared = self._shared()
        for bdf in TestMixedModeNic.VF_BDFS[:-1]:
            _, exhausted = self._exclude_vf(shared, bdf)
            self.assertFalse(exhausted)
        _, exhausted = self._exclude_vf(shared, TestMixedModeNic.VF_BDFS[-1])
        self.assertTrue(exhausted)

    def test_unknown_bdf_leaves_capacity_alone(self):
        shared = self._shared()
        _, exhausted = self._exclude_vf(shared, "0000:99:00.9")
        _, caps = TestMixedModeNic._capacity_details(shared)
        self.assertFalse(exhausted)
        self.assertEqual(len(TestMixedModeNic.VF_BDFS), caps.unit)

    def test_exhausted_card_is_not_offered_for_allocation(self):
        """
        The regression this guards: with capacity stuck at its original value the exhausted card
        stayed in the candidate node and the next allocation hit usable_bdfs[0] on an empty list.
        """
        shared = self._shared()
        for bdf in TestMixedModeNic.VF_BDFS:
            self._exclude_vf(shared, bdf)

        _, caps = TestMixedModeNic._capacity_details(shared)
        self.assertEqual(0, caps.unit)

        requested = ComponentSliver()
        requested.set_type(ComponentType.SharedNIC)
        requested.set_name(resource_name="nic1")
        requested.set_model(resource_model=TestMixedModeNic.SHARED_MODEL)

        with self.assertRaises(BrokerException) as ctx:
            NetworkNodeInventory._NetworkNodeInventory__check_component_labels_and_capacities(
                available=shared, graph_id="graph-1", requested=requested, logger=self.logger)
        self.assertEqual(ExceptionErrorCode.INSUFFICIENT_RESOURCES, ctx.exception.error_code)


if __name__ == '__main__':
    unittest.main()
