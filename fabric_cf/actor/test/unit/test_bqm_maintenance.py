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

from fim.graph.abc_property_graph import ABCPropertyGraph
from fim.slivers.attached_components import AttachedComponentsInfo, ComponentSliver, ComponentType
from fim.slivers.capacities_labels import Capacities, Flags
from fim.slivers.maintenance_mode import MaintenanceEntry, MaintenanceInfo, MaintenanceState
from fim.slivers.network_node import NodeSliver, NodeType

from fabric_cf.actor.core.container.maintenance import Maintenance
from fabric_cf.actor.fim.plugins.broker.aggregate_bqm_plugin import AggregatedBQMPlugin


class TestBqmWorkerMaintenanceState(unittest.TestCase):
    """
    Per-worker maintenance state resolution used when building the BQM/summary.
    No infrastructure (Neo4j/Postgres/Kafka) required.
    """
    SITE = "CERN"
    W1 = "cern-w1.fabric-testbed.net"
    W2 = "cern-w2.fabric-testbed.net"

    @staticmethod
    def _maint_info(entries: dict) -> MaintenanceInfo:
        info = MaintenanceInfo()
        for name, entry in entries.items():
            info.add(name, entry)
        info.finalize()
        return info

    def _entry(self, info: MaintenanceInfo, worker: str) -> MaintenanceEntry:
        return Maintenance.worker_maintenance_entry(maintenance_info=info, site_name=self.SITE,
                                                    worker_name=worker)

    def _state(self, info: MaintenanceInfo, worker: str) -> str:
        entry = self._entry(info, worker)
        return entry.state.name if entry is not None and entry.state is not None else None

    def test_worker_in_maintenance_at_active_site(self):
        # Site is Active, only w2 is in Maint - this is the shape written by worker level
        # maintenance updates
        info = self._maint_info({self.SITE: MaintenanceEntry(state=MaintenanceState.Active),
                                 self.W2: MaintenanceEntry(state=MaintenanceState.Maint)})
        self.assertEqual("Active", self._state(info, self.W1))
        self.assertEqual("Maint", self._state(info, self.W2))

    def test_site_in_maintenance_applies_to_all_workers(self):
        info = self._maint_info({self.SITE: MaintenanceEntry(state=MaintenanceState.Maint)})
        self.assertEqual("Maint", self._state(info, self.W1))
        self.assertEqual("Maint", self._state(info, self.W2))

    def test_site_maintenance_wins_over_worker_entry(self):
        info = self._maint_info({self.SITE: MaintenanceEntry(state=MaintenanceState.Maint),
                                 self.W1: MaintenanceEntry(state=MaintenanceState.Active)})
        self.assertEqual("Maint", self._state(info, self.W1))

    def test_future_site_pre_maint_does_not_mask_worker_maintenance(self):
        # A site scheduled for maintenance next week must not make a worker that is in
        # maintenance right now look schedulable
        info = self._maint_info({self.SITE: MaintenanceEntry(state=MaintenanceState.PreMaint,
                                                            deadline=datetime.now(timezone.utc) + timedelta(days=7)),
                                 self.W2: MaintenanceEntry(state=MaintenanceState.Maint)})
        self.assertEqual("Maint", self._state(info, self.W2))
        self.assertTrue(Maintenance.is_maintenance_blocking(entry=self._entry(info, self.W2)))
        # ... while its unaffected peers inherit the site's upcoming maintenance
        self.assertEqual("PreMaint", self._state(info, self.W1))
        self.assertFalse(Maintenance.is_maintenance_blocking(entry=self._entry(info, self.W1)))

    def test_active_site_pre_maint_worker_keeps_worker_deadline(self):
        # Two PreMaint entries: the one starting sooner is the effective one
        site_deadline = datetime.now(timezone.utc) + timedelta(days=7)
        worker_deadline = datetime.now(timezone.utc) + timedelta(hours=1)
        info = self._maint_info({self.SITE: MaintenanceEntry(state=MaintenanceState.PreMaint,
                                                            deadline=site_deadline),
                                 self.W2: MaintenanceEntry(state=MaintenanceState.PreMaint,
                                                           deadline=worker_deadline)})
        self.assertEqual(worker_deadline, self._entry(info, self.W2).deadline)
        self.assertEqual(site_deadline, self._entry(info, self.W1).deadline)

    def test_past_site_pre_maint_blocks_active_worker(self):
        info = self._maint_info({self.SITE: MaintenanceEntry(state=MaintenanceState.PreMaint,
                                                            deadline=datetime.now(timezone.utc) - timedelta(hours=1)),
                                 self.W1: MaintenanceEntry(state=MaintenanceState.Active)})
        self.assertEqual("PreMaint", self._state(info, self.W1))
        self.assertTrue(Maintenance.is_maintenance_blocking(entry=self._entry(info, self.W1)))

    def test_worker_pre_maint_is_reported(self):
        deadline = datetime.now(timezone.utc) + timedelta(days=1)
        info = self._maint_info({self.SITE: MaintenanceEntry(state=MaintenanceState.Active),
                                 self.W2: MaintenanceEntry(state=MaintenanceState.PreMaint,
                                                           deadline=deadline)})
        self.assertEqual("PreMaint", self._state(info, self.W2))
        self.assertEqual(deadline, self._entry(info, self.W2).deadline)

    def test_no_maintenance_info(self):
        self.assertIsNone(self._state(None, self.W1))
        self.assertIsNone(self._state(self._maint_info({}), self.W1))

    def test_blocking(self):
        blocking = Maintenance.is_maintenance_blocking
        now = datetime.now(timezone.utc)

        self.assertFalse(blocking(entry=None))
        self.assertFalse(blocking(entry=MaintenanceEntry(state=MaintenanceState.Active)))
        self.assertTrue(blocking(entry=MaintenanceEntry(state=MaintenanceState.Maint)))
        # PreMaint blocks only once the deadline has passed
        self.assertFalse(blocking(entry=MaintenanceEntry(state=MaintenanceState.PreMaint,
                                                         deadline=now + timedelta(hours=1))))
        self.assertTrue(blocking(entry=MaintenanceEntry(state=MaintenanceState.PreMaint,
                                                        deadline=now - timedelta(hours=1))))
        # PreMaint without a deadline must not raise
        self.assertFalse(blocking(entry=MaintenanceEntry(state=MaintenanceState.PreMaint)))
        # Naive deadlines are treated as UTC rather than raising on comparison
        self.assertTrue(blocking(entry=MaintenanceEntry(state=MaintenanceState.PreMaint,
                                                        deadline=(now - timedelta(hours=1)).replace(tzinfo=None))))


class FakeSite:
    def __init__(self, maint_info: MaintenanceInfo):
        self._info = maint_info

    def get_maintenance_info(self) -> MaintenanceInfo:
        return self._info


class FakeDatabase:
    def __init__(self, site_name: str, maint_info: MaintenanceInfo):
        self.site_name = site_name
        self.maint_info = maint_info

    def get_site(self, *, site_name: str):
        return FakeSite(self.maint_info) if site_name == self.site_name else None


class FakeActor:
    def __init__(self, db: FakeDatabase):
        self.db = db

    def get_plugin(self):
        return self

    def get_database(self):
        return self.db


class FakeCbm:
    """
    Minimal stand-in for a CBM holding one site worth of identical workers.
    """
    graph_id = "fake-cbm"

    def __init__(self, site: str, workers: dict):
        self.site = site
        self.workers = workers

    def get_all_nodes_by_class(self, *, label):
        return list(self.workers.keys())

    def build_deep_node_sliver(self, *, node_id):
        cores_cap, _ = self.workers[node_id]
        sliver = NodeSliver()
        sliver.node_id = node_id
        sliver.set_name(resource_name=node_id)
        sliver.set_type(NodeType.Server)
        sliver.set_site(self.site)
        sliver.set_capacities(Capacities(core=cores_cap, ram=512, disk=4800))
        sliver.set_flags(Flags(ptp=True))
        comp = ComponentSliver()
        comp.set_name(resource_name=f"{node_id}-gpu")
        comp.set_type(ComponentType.GPU)
        comp.set_model("RTX6000")
        comp.set_capacities(Capacities(unit=2))
        components = AttachedComponentsInfo()
        components.add_device(comp)
        sliver.attached_components_info = components
        return sliver

    def get_intersite_links(self):
        return []


class TestBqmSummaryMaintenanceAccounting(unittest.TestCase):
    """
    End-to-end accounting for plug_produce_bqm_summary/plug_produce_bqm with hosts in maintenance.
    Uses stub CBM/database objects, so no infrastructure is required.
    """
    SITE = "CERN"
    # name -> (cores capacity, cores allocated)
    WORKERS = {"cern-w1.fabric-testbed.net": (128, 96),
               "cern-w2.fabric-testbed.net": (128, 4),
               "cern-w3.fabric-testbed.net": (128, 16),
               "cern-w4.fabric-testbed.net": (128, 96),
               "cern-w5.fabric-testbed.net": (128, 0),
               "cern-w6.fabric-testbed.net": (128, 48)}
    MAINT = {"cern-w2.fabric-testbed.net", "cern-w5.fabric-testbed.net"}

    def setUp(self):
        info = MaintenanceInfo()
        for worker in sorted(self.MAINT):
            info.add(worker, MaintenanceEntry(state=MaintenanceState.Maint))
        info.finalize()

        workers = self.WORKERS

        class Plugin(AggregatedBQMPlugin):
            @staticmethod
            def occupied_node_capacity(*, db, node_id, start, end):
                _, allocated = workers[node_id]
                comps = {} if allocated == 0 else {ComponentType.GPU: {"RTX6000": Capacities(unit=1)}}
                return Capacities(core=allocated, ram=allocated * 4, disk=allocated * 10), comps

        self.plugin = Plugin(actor=FakeActor(FakeDatabase(self.SITE, info)), logger=None)
        self.cbm = FakeCbm(self.SITE, self.WORKERS)

    def test_summary_excludes_maintenance_hosts_from_availability(self):
        out = self.plugin.plug_produce_bqm_summary(cbm=self.cbm, query_level=2)
        site = out["sites"][0]

        expected_avail = sum(max(0, cap - alloc) for name, (cap, alloc) in self.WORKERS.items()
                             if name not in self.MAINT)
        # Capacity and allocations stay truthful for the whole site
        self.assertEqual(sum(cap for cap, _ in self.WORKERS.values()), site["cores_capacity"])
        self.assertEqual(sum(alloc for _, alloc in self.WORKERS.values()), site["cores_allocated"])
        # Availability counts only the hosts that can actually take a reservation
        self.assertEqual(expected_avail, site["cores_available"])
        self.assertEqual(len(self.MAINT), site["hosts_in_maintenance"])
        self.assertEqual(len(self.WORKERS), site["hosts_count"])

    def test_summary_reports_per_host_maintenance_state(self):
        out = self.plugin.plug_produce_bqm_summary(cbm=self.cbm, query_level=2)

        self.assertEqual(self.MAINT, {h["name"] for h in out["hosts"] if h["state"] == "Maint"})
        self.assertEqual(self.MAINT, {h["name"] for h in out["hosts"] if h["in_maintenance"]})
        # A host in maintenance advertises nothing as available, but still reports its capacity
        for host in out["hosts"]:
            if host["name"] in self.MAINT:
                self.assertEqual(0, host["cores_available"])
                self.assertEqual(0, host["ram_available"])
                self.assertEqual(0, host["disk_available"])
                self.assertEqual(128, host["cores_capacity"])
                self.assertTrue(all(c["available"] == 0 for c in host["components"].values()))

    def test_summary_host_records_reconcile_with_site_totals(self):
        out = self.plugin.plug_produce_bqm_summary(cbm=self.cbm, query_level=2)
        site = out["sites"][0]

        for dimension in ("cores", "ram", "disk"):
            for field in ("capacity", "allocated", "available"):
                key = f"{dimension}_{field}"
                self.assertEqual(site[key], sum(h[key] for h in out["hosts"]), key)

        for field in ("capacity", "allocated", "available"):
            expected = sum(c[field] for h in out["hosts"] for c in h["components"].values())
            self.assertEqual(expected, site["components"]["GPU-RTX6000"][field], field)

    def test_graph_site_node_shows_no_availability_for_maintenance_hosts(self):
        abqm = self.plugin.plug_produce_bqm(cbm=self.cbm, query_level=2)

        site_nodes = abqm.get_all_nodes_by_class(label=ABCPropertyGraph.CLASS_CompositeNode)
        self.assertEqual(1, len(site_nodes))
        _, site_props = abqm.get_node_properties(node_id=site_nodes[0])
        site_caps = Capacities.from_json(site_props.get(ABCPropertyGraph.PROP_CAPACITIES))
        site_allocs = Capacities.from_json(site_props.get(ABCPropertyGraph.PROP_CAPACITY_ALLOCATIONS))

        usable = {n: v for n, v in self.WORKERS.items() if n not in self.MAINT}
        # Capacity still describes all of the hardware at the site
        self.assertEqual(sum(cap for cap, _ in self.WORKERS.values()), site_caps.core)
        # Hosts in maintenance are counted as fully allocated, so a consumer deriving
        # capacity - allocations sees only what can actually be reserved, which is the same
        # number the JSON summary reports as cores_available
        self.assertEqual(sum(max(0, cap - alloc) for cap, alloc in usable.values()),
                         site_caps.core - site_allocs.core)

    def test_graph_worker_nodes_show_no_availability_when_in_maintenance(self):
        abqm = self.plugin.plug_produce_bqm(cbm=self.cbm, query_level=2)

        seen = set()
        for node_id in abqm.get_all_nodes_by_class(label=ABCPropertyGraph.CLASS_NetworkNode):
            _, props = abqm.get_node_properties(node_id=node_id)
            name = props.get(ABCPropertyGraph.PROP_NAME)
            caps = Capacities.from_json(props.get(ABCPropertyGraph.PROP_CAPACITIES))
            allocs = Capacities.from_json(props.get(ABCPropertyGraph.PROP_CAPACITY_ALLOCATIONS))
            seen.add(name)
            if name in self.MAINT:
                # capacity keeps describing the hardware, but nothing on it is free
                self.assertEqual(self.WORKERS[name][0], caps.core, name)
                self.assertEqual(0, caps.core - allocs.core, f"{name} advertises free cores")
            else:
                self.assertEqual(self.WORKERS[name][1], allocs.core, name)
        self.assertEqual(set(self.WORKERS), seen)

    def test_graph_component_availability_matches_summary(self):
        """
        fablib works out what components a site has free by summing the per worker component
        slivers rather than reading the aggregated site node, so worker components must not
        advertise anything on a host in maintenance.
        """
        abqm = self.plugin.plug_produce_bqm(cbm=self.cbm, query_level=2)
        summary = self.plugin.plug_produce_bqm_summary(cbm=self.cbm, query_level=2)

        capacity, allocated = 0, 0
        for node_id in abqm.get_all_nodes_by_class(label=ABCPropertyGraph.CLASS_Component):
            _, props = abqm.get_node_properties(node_id=node_id)
            caps = Capacities.from_json(props.get(ABCPropertyGraph.PROP_CAPACITIES))
            alloc_json = props.get(ABCPropertyGraph.PROP_CAPACITY_ALLOCATIONS)
            capacity += caps.unit
            allocated += Capacities.from_json(alloc_json).unit if alloc_json else 0

        expected_available = summary["sites"][0]["components"]["GPU-RTX6000"]["available"]
        self.assertEqual(expected_available, capacity - allocated)
        # guard against the fixture silently losing its maintenance hosts, which would make the
        # assertion above pass for the wrong reason
        self.assertLess(expected_available, capacity)

    def test_graph_worker_nodes_carry_own_maintenance_state(self):
        abqm = self.plugin.plug_produce_bqm(cbm=self.cbm, query_level=2)

        states = {}
        for node_id in abqm.get_all_nodes_by_class(label=ABCPropertyGraph.CLASS_NetworkNode):
            _, props = abqm.get_node_properties(node_id=node_id)
            name = props.get(ABCPropertyGraph.PROP_NAME)
            info = props.get(ABCPropertyGraph.PROP_MAINTENANCE_INFO)
            self.assertIsNotNone(info, f"{name} carries no maintenance info")
            states[name] = MaintenanceInfo.from_json(info).get(name).state

        self.assertEqual(len(self.WORKERS), len(states))
        self.assertEqual(self.MAINT, {n for n, st in states.items() if st == MaintenanceState.Maint})


if __name__ == '__main__':
    unittest.main()
