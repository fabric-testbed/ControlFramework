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

from fim.slivers.attached_components import ComponentSliver, ComponentType
from fim.slivers.capacities_labels import Labels
from fim.slivers.delegations import Delegation, Delegations, DelegationType
from fim.slivers.interface_info import InterfaceInfo, InterfaceSliver
from fim.slivers.network_service import NetworkServiceInfo, NetworkServiceSliver, NSLayer

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.container.maintenance import Maintenance, Site
from fabric_cf.actor.core.policy.network_node_inventory import NetworkNodeInventory


class TestSharedNicBlockedVlans(unittest.TestCase):
    """
    Shared NIC VLAN selection with operator blocked VLANs (e.g. stale switch config).
    No infrastructure (Neo4j/Postgres/Kafka) required.
    """
    DELEGATION_ID = "del1"
    BDFS = ["0000:e2:09.2", "0000:e2:09.3", "0000:e2:09.4"]
    NUMAS = ["1", "1", "1"]
    VLANS = ["2112", "2118", "2120"]
    MACS = ["0C:42:A1:EA:C7:51", "0C:42:A1:EA:C7:52", "0C:42:A1:EA:C7:53"]
    LOCAL_NAMES = ["p1", "p1", "p1"]
    MODEL = "ConnectX-6"

    logger = logging.getLogger("test-shared-nic-blocked-vlans")

    @classmethod
    def _label_delegations(cls, labels: Labels) -> Delegations:
        delegation = Delegation(atype=DelegationType.LABEL, delegation_id=cls.DELEGATION_ID)
        delegation.set_details(labels)
        delegations = Delegations(atype=DelegationType.LABEL)
        delegations.add_delegations(delegation)
        return delegations

    @classmethod
    def _available_shared_nic(cls) -> ComponentSliver:
        component = ComponentSliver()
        component.set_type(ComponentType.SharedNIC)
        component.set_name(resource_name="shared-nic-1")
        component.set_model(resource_model=cls.MODEL)
        component.set_label_delegations(cls._label_delegations(
            Labels(bdf=list(cls.BDFS), numa=list(cls.NUMAS))))

        ifs = InterfaceSliver()
        ifs.set_name(resource_name="p1")
        ifs.set_label_delegations(cls._label_delegations(
            Labels(bdf=list(cls.BDFS), vlan=list(cls.VLANS), mac=list(cls.MACS),
                   local_name=list(cls.LOCAL_NAMES))))
        interface_info = InterfaceInfo()
        interface_info.add_interface(ifs)

        ns = NetworkServiceSliver()
        ns.set_name(resource_name="shared-nic-1-ns")
        ns.interface_info = interface_info

        component.network_service_info = NetworkServiceInfo()
        component.network_service_info.add_network_service(ns_sliver=ns)
        return component

    @classmethod
    def _requested_shared_nic(cls, model: str = MODEL, vlan: str = None) -> ComponentSliver:
        component = ComponentSliver()
        component.set_type(ComponentType.SharedNIC)
        component.set_name(resource_name="nic1")
        component.set_model(resource_model=model)

        ifs = InterfaceSliver()
        ifs.set_name(resource_name="nic1-p1")
        if vlan is not None:
            ifs.set_labels(lab=Labels(vlan=vlan))
        interface_info = InterfaceInfo()
        interface_info.add_interface(ifs)

        ns = NetworkServiceSliver()
        ns.set_name(resource_name="nic1-ns")
        ns.set_layer(layer=NSLayer.L3)
        ns.interface_info = interface_info

        component.network_service_info = NetworkServiceInfo()
        component.network_service_info.add_network_service(ns_sliver=ns)
        return component

    @staticmethod
    def _allocate(available: ComponentSliver, requested: ComponentSliver,
                  blocked_vlans: set = None) -> ComponentSliver:
        return NetworkNodeInventory._NetworkNodeInventory__update_shared_nic_labels_and_capacities(
            available=available, requested=requested, logger=TestSharedNicBlockedVlans.logger,
            blocked_vlans=blocked_vlans)

    @staticmethod
    def _allocated_ifs(component: ComponentSliver) -> InterfaceSliver:
        ns = next(iter(component.network_service_info.network_services.values()))
        return next(iter(ns.interface_info.interfaces.values()))

    def test_no_blocked_vlans_assigns_first_bdf(self):
        requested = self._allocate(self._available_shared_nic(), self._requested_shared_nic())
        self.assertEqual(self.BDFS[0], requested.label_allocations.bdf)
        ifs_labels = self._allocated_ifs(requested).get_label_allocations()
        self.assertEqual(self.VLANS[0], ifs_labels.vlan)
        self.assertEqual(self.MACS[0], ifs_labels.mac)

    def test_blocked_vlan_skips_bound_bdf(self):
        requested = self._allocate(self._available_shared_nic(), self._requested_shared_nic(),
                                   blocked_vlans={self.VLANS[0]})
        self.assertEqual(self.BDFS[1], requested.label_allocations.bdf)
        ifs_labels = self._allocated_ifs(requested).get_label_allocations()
        self.assertEqual(self.VLANS[1], ifs_labels.vlan)
        self.assertEqual(self.MACS[1], ifs_labels.mac)

    def test_all_vlans_blocked_raises_insufficient_resources(self):
        with self.assertRaises(BrokerException) as ctx:
            self._allocate(self._available_shared_nic(), self._requested_shared_nic(),
                           blocked_vlans=set(self.VLANS))
        self.assertEqual(ExceptionErrorCode.INSUFFICIENT_RESOURCES, ctx.exception.error_code)

    def test_requested_vlan_blocked_raises_insufficient_resources(self):
        with self.assertRaises(BrokerException) as ctx:
            self._allocate(self._available_shared_nic(),
                           self._requested_shared_nic(vlan=self.VLANS[1]),
                           blocked_vlans={self.VLANS[1]})
        self.assertEqual(ExceptionErrorCode.INSUFFICIENT_RESOURCES, ctx.exception.error_code)

    def test_requested_vlan_honored_when_others_blocked(self):
        requested = self._allocate(self._available_shared_nic(),
                                   self._requested_shared_nic(vlan=self.VLANS[2]),
                                   blocked_vlans={self.VLANS[0]})
        self.assertEqual(self.BDFS[2], requested.label_allocations.bdf)
        self.assertEqual(self.VLANS[2], self._allocated_ifs(requested).get_label_allocations().vlan)

    def test_vnic_model_ignores_blocked_vlans(self):
        requested = self._allocate(self._available_shared_nic(),
                                   self._requested_shared_nic(model=Constants.OPENSTACK_VNIC_MODEL),
                                   blocked_vlans=set(self.VLANS))
        self.assertEqual(self.BDFS[0], requested.label_allocations.bdf)
        self.assertIsNone(self._allocated_ifs(requested).get_label_allocations().vlan)


class TestSiteBlockedVlans(unittest.TestCase):
    """
    Parsing of the blocked VLAN maintenance properties on a Site.
    """
    SITE = "UTAH"
    W1 = "utah-w1.fabric-testbed.net"
    W2 = "utah-w2.fabric-testbed.net"

    def _site(self, properties: dict = None) -> Site:
        site = Site(name=self.SITE, maint_info=None)
        if properties is not None:
            site.set_properties(properties=properties)
        return site

    def test_no_properties(self):
        self.assertEqual(set(), self._site().get_blocked_vlans(worker=self.W1))

    def test_site_wide_vlans(self):
        site = self._site({Constants.BLOCKED_VLANS: "2118, 2120"})
        self.assertEqual({"2118", "2120"}, site.get_blocked_vlans())
        self.assertEqual({"2118", "2120"}, site.get_blocked_vlans(worker=self.W1))

    def test_worker_vlans_combined_with_site_wide(self):
        site = self._site({Constants.BLOCKED_VLANS: "2118",
                           f"{Constants.BLOCKED_VLANS}.{self.W1}": "2200,2201"})
        self.assertEqual({"2118", "2200", "2201"}, site.get_blocked_vlans(worker=self.W1))
        self.assertEqual({"2118"}, site.get_blocked_vlans(worker=self.W2))
        self.assertEqual({"2118"}, site.get_blocked_vlans())

    def test_worker_only_vlans(self):
        site = self._site({f"{Constants.BLOCKED_VLANS}.{self.W1}": "2118"})
        self.assertEqual({"2118"}, site.get_blocked_vlans(worker=self.W1))
        self.assertEqual(set(), site.get_blocked_vlans(worker=self.W2))


class TestMergePropertiesForMaintenance(unittest.TestCase):
    """
    Per key merge of the site properties carried with a maintenance request.
    """
    W1 = "utah-w1.fabric-testbed.net"

    def test_untouched_keys_are_preserved(self):
        existing = {Constants.PROJECT_ID: "p1", f"{Constants.BLOCKED_VLANS}.{self.W1}": "2118"}
        merged = Maintenance.merge_properties(existing=existing,
                                              updates={Constants.BLOCKED_VLANS: "2120"})
        self.assertEqual({Constants.PROJECT_ID: "p1",
                          f"{Constants.BLOCKED_VLANS}.{self.W1}": "2118",
                          Constants.BLOCKED_VLANS: "2120"}, merged)

    def test_update_overwrites_key(self):
        merged = Maintenance.merge_properties(existing={Constants.BLOCKED_VLANS: "2118"},
                                              updates={Constants.BLOCKED_VLANS: "2120"})
        self.assertEqual({Constants.BLOCKED_VLANS: "2120"}, merged)

    def test_empty_value_removes_key(self):
        existing = {Constants.PROJECT_ID: "p1", Constants.BLOCKED_VLANS: "2118"}
        merged = Maintenance.merge_properties(existing=existing,
                                              updates={Constants.BLOCKED_VLANS: " "})
        self.assertEqual({Constants.PROJECT_ID: "p1"}, merged)

    def test_no_existing_properties(self):
        merged = Maintenance.merge_properties(existing=None,
                                              updates={Constants.BLOCKED_VLANS: "2118",
                                                       Constants.USERS: ""})
        self.assertEqual({Constants.BLOCKED_VLANS: "2118"}, merged)


if __name__ == '__main__':
    unittest.main()
