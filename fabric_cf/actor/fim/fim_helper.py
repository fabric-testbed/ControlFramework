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
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_database import ABCDatabase

import logging
import random
from datetime import datetime
from typing import Tuple, List, Union


from fabric_cf.actor.fim.plugins.broker.aggregate_bqm_plugin import AggregatedBQMPlugin
from fim.graph.abc_property_graph import ABCPropertyGraph, ABCGraphImporter
from fim.graph.neo4j_property_graph import Neo4jGraphImporter, Neo4jPropertyGraph
from fim.graph.networkx_property_graph import NetworkXGraphImporter
from fim.graph.resources.abc_arm import ABCARMPropertyGraph
from fim.graph.resources.abc_cbm import ABCCBMPropertyGraph
from fim.graph.resources.neo4j_arm import Neo4jARMGraph
from fim.graph.resources.neo4j_cbm import Neo4jCBMGraph, Neo4jCBMFactory
from fim.graph.resources.networkx_abqm import NetworkXABQMFactory
from fim.graph.slices.abc_asm import ABCASMPropertyGraph
from fim.graph.slices.neo4j_asm import Neo4jASMFactory
from fim.graph.slices.networkx_asm import NetworkxASM, NetworkXASMFactory
from fim.slivers.attached_components import ComponentSliver
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Capacities
from fim.slivers.delegations import Delegations
from fim.slivers.interface_info import InterfaceSliver, InterfaceType
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver, ServiceType
from fim.user import ExperimentTopology, NodeType, Component, ReservationInfo, Node, GraphFormat, Labels
from fim.user.composite_node import CompositeNode
from fim.user.interface import Interface
from fim.user.topology import AdvertizedTopology

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates


class InterfaceSliverMapping:
    def __init__(self):
        # Maps to Connection Point in the Graph
        self.peer_ifs = None
        # Maps to Network Service (Parent of Peer IFS) in the Graph
        self.peer_ns_id = None
        # Maps to Component (Parent of Network Service) in the Graph
        self.component_name = None
        # Maps to the Network Node (Parent of the Component or Parent of Connection) in the Graph
        self.node_id = None
        self.facility = False
        self.peer_site = None

    def get_peer_ifs(self) -> InterfaceSliver:
        return self.peer_ifs

    def get_peer_ns_id(self) -> str:
        return self.peer_ns_id

    def get_component_name(self) -> str:
        return self.component_name

    def get_node_id(self) -> str:
        return self.node_id

    def is_facility(self) -> bool:
        return self.facility

    def get_peer_site(self) -> str:
        return self.peer_site

    def is_peered(self) -> bool:
        return self.peer_site is not None

    def set_peer_ifs(self, peer_ifs: InterfaceSliver):
        self.peer_ifs = peer_ifs

    def set_peer_ns_id(self, peer_ns_id: str):
        self.peer_ns_id = peer_ns_id

    def set_component_name(self, component_name: str):
        self.component_name = component_name

    def set_node_id(self, node_id: str):
        self.node_id = node_id

    def set_facility(self, facility: bool):
        self.facility = facility

    def set_peer_site(self, peer_site: str):
        self.peer_site = peer_site

    def set_properties(self, **kwargs):
        """
        Lets you set multiple properties exposed via setter methods
        :param kwargs:
        :return:
        """
        # set any property on a sliver that has a setter
        for k, v in kwargs.items():
            try:
                # we can set anything the sliver model has a setter for
                self.__getattribute__('set_' + k)(v)
            except AttributeError:
                raise RuntimeError(f'Unable to set property {k} on the sliver - no such property available')


class FimHelper:
    """
    Provides methods to load Graph Models and perform various operations on them
    """
    _neo4j_graph_importer = None

    @staticmethod
    def get_neo4j_importer(neo4j_config: dict = None) -> ABCGraphImporter:
        """
        get fim graph importer
        :return: Neo4jGraphImporter
        """
        logger = None
        if FimHelper._neo4j_graph_importer is None:
            if neo4j_config is None:
                from fabric_cf.actor.core.container.globals import GlobalsSingleton
                neo4j_config = GlobalsSingleton.get().get_config().get_global_config().get_neo4j_config()
                logger = GlobalsSingleton.get().get_logger()

            FimHelper._neo4j_graph_importer = Neo4jGraphImporter(url=neo4j_config["url"], user=neo4j_config["user"],
                                                                 pswd=neo4j_config["pass"],
                                                                 import_host_dir=neo4j_config["import_host_dir"],
                                                                 import_dir=neo4j_config["import_dir"], logger=logger)
        return FimHelper._neo4j_graph_importer

    @staticmethod
    def get_networkx_importer(logger: logging.Logger = None) -> ABCGraphImporter:
        """
        get fim graph importer
        :return: Neo4jGraphImporter
        """
        if logger is None:
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            logger = GlobalsSingleton.get().get_logger()

        return NetworkXGraphImporter(logger=logger)

    @staticmethod
    def get_arm_graph_from_file(*, filename: str, graph_id: str = None, neo4j_config: dict = None) -> ABCARMPropertyGraph:
        """
        Load specified file directly with no manipulations or validation
        :param filename:
        :param graph_id:
        :param neo4j_config neo4j_config
        :return:
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer(neo4j_config=neo4j_config)
        neo4_graph = neo4j_graph_importer.import_graph_from_file(graph_file=filename, graph_id=graph_id)
        site_arm = Neo4jARMGraph(graph=Neo4jPropertyGraph(graph_id=neo4_graph.graph_id,
                                                          importer=neo4j_graph_importer))

        site_arm.validate_graph()

        return site_arm

    @staticmethod
    def get_arm_graph(*, graph_id: str, neo4j_config: dict = None) -> ABCARMPropertyGraph:
        """
        Load arm graph from fim
        :param graph_id: graph_id
        :param neo4j_config neo4j_config
        :return: Neo4jARMGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer(neo4j_config=neo4j_config)
        arm_graph = Neo4jARMGraph(graph=Neo4jPropertyGraph(graph_id=graph_id, importer=neo4j_graph_importer))
        if arm_graph.graph_exists():
            arm_graph.validate_graph()

        return arm_graph

    @staticmethod
    def get_experiment_topology(*, graph_id: str) -> ExperimentTopology:
        """
        Load Experiment Topology provide Graph Id
        :param graph_id Graph Id
        """
        graph = FimHelper.get_graph(graph_id=graph_id)
        asm_graph = Neo4jASMFactory.create(graph=graph)
        neo4j_topo = ExperimentTopology()
        neo4j_topo.cast(asm_graph=asm_graph)
        return neo4j_topo

    @staticmethod
    def get_graph(*, graph_id: str, neo4j_config: dict = None) -> ABCPropertyGraph:
        """
        Load arm graph from fim
        :param graph_id: graph_id.
        :param neo4j_config neo4j_config
        :return: Neo4jARMGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer(neo4j_config=neo4j_config)
        arm_graph = Neo4jPropertyGraph(graph_id=graph_id, importer=neo4j_graph_importer)

        return arm_graph

    @staticmethod
    def get_neo4j_cbm_graph(graph_id: str, neo4j_config: dict = None) -> ABCCBMPropertyGraph:
        """
        Load cbm graph from fim
        :param graph_id: graph_id
        :param neo4j_config neo4j_config
        :return: Neo4jCBMGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer(neo4j_config=neo4j_config)
        combined_broker_model = Neo4jCBMGraph(graph_id=graph_id,
                                              importer=neo4j_graph_importer,
                                              logger=neo4j_graph_importer.log)
        if combined_broker_model.graph_exists():
            combined_broker_model.validate_graph()
        return combined_broker_model

    @staticmethod
    def get_neo4j_cbm_graph_from_string_direct(*, graph_str: str, ignore_validation: bool = False) -> ABCCBMPropertyGraph:
        """
        Load Broker Query model graph from string
        :param graph_str: graph_str
        :param ignore_validation: ignore validation when set to true
        :return: Neo4jCBMGraph
        """
        neo4_graph = FimHelper.get_graph_from_string_direct(graph_str=graph_str)
        if neo4_graph.graph_exists() and not ignore_validation:
            neo4_graph.validate_graph()
        return Neo4jCBMFactory.create(neo4_graph)

    @staticmethod
    def get_graph_from_string_direct(*, graph_str: str, neo4j_config: dict = None) -> ABCPropertyGraph:
        """
        Load arm graph from fim
        :param graph_str: graph_str
        :param neo4j_config neo4j_config
        :return: Neo4jPropertyGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer(neo4j_config=neo4j_config)
        graph = neo4j_graph_importer.import_graph_from_string_direct(graph_string=graph_str)

        return graph

    @staticmethod
    def get_graph_from_string(*, graph_str: str, neo4j_config: dict = None) -> ABCPropertyGraph:
        """
        Load arm graph from fim
        :param graph_str: graph_str
        :param neo4j_config neo4j_config
        :return: Neo4jPropertyGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer(neo4j_config=neo4j_config)
        graph = neo4j_graph_importer.import_graph_from_string(graph_string=graph_str)

        return graph

    @staticmethod
    def get_networkx_graph_from_string(*, graph_str: str) -> ABCPropertyGraph:
        """
        Load arm graph from fim
        :param graph_str: graph_str
        :return: NetworkXPropertyGraph
        """
        networkx_graph_importer = FimHelper.get_networkx_importer()
        graph = networkx_graph_importer.import_graph_from_string(graph_string=graph_str)

        return graph

    @staticmethod
    def delete_graph(*, graph_id: str, neo4j_config: dict = None):
        """
        Delete a graph
        @param graph_id graph id
        @param neo4j_config neo4j_config
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer(neo4j_config=neo4j_config)
        neo4j_graph_importer.delete_graph(graph_id=graph_id)

    @staticmethod
    def get_delegation(delegated_capacities: Delegations, delegation_name: str) -> Capacities:
        """
        Get Delegated capacity given delegation name
        :param delegated_capacities: list of delegated capacities
        :param delegation_name: delegation name
        :return: capacity for specified delegation
        """
        delegation = delegated_capacities.get_by_delegation_id(delegation_name)
        return delegation.get_details() if delegation is not None else None

    @staticmethod
    def update_node(*, graph_id: str, sliver: BaseSliver):
        """
        Update Sliver Node in ASM
        :param graph_id:
        :param sliver:
        :return:
        """
        if sliver is None:
            return
        graph = FimHelper.get_graph(graph_id=graph_id)
        asm_graph = Neo4jASMFactory.create(graph=graph)
        neo4j_topo = ExperimentTopology()
        neo4j_topo.cast(asm_graph=asm_graph)

        node_name = sliver.get_name()
        if isinstance(sliver, NodeSliver) and node_name in neo4j_topo.nodes:
            node = neo4j_topo.nodes[node_name]
            node.set_properties(labels=sliver.labels,
                                label_allocations=sliver.label_allocations,
                                capacity_allocations=sliver.capacity_allocations,
                                reservation_info=sliver.reservation_info,
                                node_map=sliver.node_map,
                                management_ip=sliver.management_ip,
                                capacity_hints=sliver.capacity_hints)
            if sliver.attached_components_info is not None:
                graph_sliver = asm_graph.build_deep_node_sliver(node_id=sliver.node_id)
                diff = graph_sliver.diff(other_sliver=sliver)
                if diff is not None:
                    for cname in diff.removed.components:
                        reservation_info = ReservationInfo()
                        reservation_info.reservation_state = ReservationStates.Failed.name
                        node.components[cname].set_properties(reservation_info=reservation_info)

                topo_component_dict = node.components
                for component in sliver.attached_components_info.devices.values():
                    topo_component = topo_component_dict[component.get_name()]
                    topo_component.set_properties(labels=component.labels,
                                                  label_allocations=component.label_allocations,
                                                  capacity_allocations=component.capacity_allocations,
                                                  node_map=component.node_map)
                    # Update Mac address
                    if component.network_service_info is not None and \
                            component.network_service_info.network_services is not None:
                        topo_ifs_dict = topo_component.interfaces
                        for ns in component.network_service_info.network_services.values():
                            if ns.interface_info is None or ns.interface_info.interfaces is None:
                                continue
                            for ifs in ns.interface_info.interfaces.values():
                                topo_ifs = topo_ifs_dict[ifs.get_name()]
                                topo_ifs.set_properties(labels=ifs.labels,
                                                        label_allocations=ifs.label_allocations,
                                                        node_map=ifs.node_map)
                                if ifs.peer_labels is not None:
                                    topo_ifs.set_properties(peer_labels=ifs.peer_labels)

                                if ifs.capacities is not None:
                                    topo_ifs.set_properties(capacities=ifs.capacities)

        elif isinstance(sliver, NetworkServiceSliver) and node_name in neo4j_topo.network_services:
            node = neo4j_topo.network_services[node_name]
            node.set_properties(labels=sliver.labels,
                                label_allocations=sliver.label_allocations,
                                capacity_allocations=sliver.capacity_allocations,
                                reservation_info=sliver.reservation_info,
                                node_map=sliver.node_map,
                                gateway=sliver.gateway)
            if sliver.interface_info is not None:
                topo_ifs_dict = node.interfaces
                for ifs in sliver.interface_info.interfaces.values():
                    if ifs.get_name() not in node.interfaces:
                        continue
                    topo_ifs = topo_ifs_dict[ifs.get_name()]
                    topo_ifs.set_properties(labels=ifs.labels,
                                            label_allocations=ifs.label_allocations,
                                            node_map=ifs.node_map)

                    if ifs.peer_labels is not None:
                        topo_ifs.set_properties(peer_labels=ifs.peer_labels)

                    if ifs.capacities is not None:
                        topo_ifs.set_properties(capacities=ifs.capacities)

    @staticmethod
    def get_neo4j_asm_graph(*, slice_graph: str) -> ABCASMPropertyGraph:
        """
        Load Slice in Neo4j
        :param slice_graph: slice graph string
        :return: Neo4j ASM graph
        """
        neo4j_graph = FimHelper.get_graph_from_string(graph_str=slice_graph)
        asm = Neo4jASMFactory.create(graph=neo4j_graph)
        return asm

    @staticmethod
    def get_networkx_asm_graph(*, slice_graph: str) -> NetworkxASM:
        """
        Load Slice in NetworkX
        :param slice_graph: slice graph string
        :return: NetworkX ASM graph
        """
        networkx_graph = FimHelper.get_graph_from_string(graph_str=slice_graph)
        asm = NetworkXASMFactory.create(graph=networkx_graph)
        return asm

    @staticmethod
    def get_interface_sliver_mapping(ifs_node_id: str, slice_graph: ABCASMPropertyGraph) -> InterfaceSliverMapping:
        """
        Finds Peer Interface Sliver and parent information upto Network Node
        @param ifs_node_id node id of the Interface Sliver
        @param slice_graph Slice ASM
        @returns Interface Sliver Mapping
        """

        # Peer Connection point maps to Interface Sliver in ASM
        # This must always return only 1 IFS
        peer_interfaces = FimHelper.get_peer_interfaces(ifs_node_id=ifs_node_id, graph=slice_graph)
        if len(peer_interfaces) != 1:
            raise Exception(f"More than one Peer Interface Sliver found for IFS: {ifs_node_id}!")
        peer_ifs = next(iter(peer_interfaces))

        if peer_ifs.get_type() == InterfaceType.SubInterface:
            parent_cp_node_name, parent_cp_node_id = slice_graph.get_parent(node_id=peer_ifs.node_id,
                                                                            rel=ABCPropertyGraph.REL_CONNECTS,
                                                                            parent=ABCPropertyGraph.CLASS_ConnectionPoint)
            peer_ns_node_name, peer_ns_id = slice_graph.get_parent(node_id=parent_cp_node_id,
                                                                   rel=ABCPropertyGraph.REL_CONNECTS,
                                                                   parent=ABCPropertyGraph.CLASS_NetworkService)
        else:
            peer_ns_node_name, peer_ns_id = slice_graph.get_parent(node_id=peer_ifs.node_id,
                                                                   rel=ABCPropertyGraph.REL_CONNECTS,
                                                                   parent=ABCPropertyGraph.CLASS_NetworkService)

        component_name = None
        facility = False
        peer_site = None

        if peer_ifs.get_type() in [InterfaceType.DedicatedPort, InterfaceType.SharedPort, InterfaceType.SubInterface]:
            component_name, component_id = slice_graph.get_parent(node_id=peer_ns_id, rel=ABCPropertyGraph.REL_HAS,
                                                                  parent=ABCPropertyGraph.CLASS_Component)
            # Possibly P4 switch; parent will be a switch
            if not component_name:
                component_id = peer_ns_id
                component_name = str(NodeType.Switch)

            node_name, node_id = slice_graph.get_parent(node_id=component_id, rel=ABCPropertyGraph.REL_HAS,
                                                        parent=ABCPropertyGraph.CLASS_NetworkNode)
        elif peer_ifs.get_type() == InterfaceType.FacilityPort:
            node_name, node_id = slice_graph.get_parent(node_id=peer_ns_id, rel=ABCPropertyGraph.REL_HAS,
                                                        parent=ABCPropertyGraph.CLASS_NetworkNode)
            node_sliver = slice_graph.build_deep_node_sliver(node_id=node_id)
            # Passing Facility Name instead of Node ID
            node_id = f"{node_sliver.get_site()},{node_name}"
            facility = True
        else:
            node_id = None
            peer_ns = slice_graph.build_deep_ns_sliver(node_id=peer_ns_id)

            # Peer Network Service is FABRIC L3VPN connected to a FABRIC Site
            # Determine the site to which AL2S Peered Interface is connected to

            for ifs in peer_ns.interface_info.interfaces.values():
                # Skip the peered interface
                if ifs.node_id == peer_ifs.node_id:
                    continue
                # Grab the first interface connected to a VM
                peer_nic_ifs_list = FimHelper.get_peer_interfaces(ifs_node_id=ifs.node_id,
                                                                  graph=slice_graph)
                ovs_ns_name, ovs_ns_id = slice_graph.get_parent(node_id=peer_nic_ifs_list[0].node_id,
                                                                rel=ABCPropertyGraph.REL_CONNECTS,
                                                                parent=ABCPropertyGraph.CLASS_NetworkService)
                ovs_ns = slice_graph.build_deep_ns_sliver(node_id=ovs_ns_id)

                peer_site = ovs_ns.get_site()

                if ovs_ns.get_type() != ServiceType.OVS:
                    # Peer node i.e. Facility Port
                    peer_node_name, peer_node_id = slice_graph.get_parent(node_id=ovs_ns.node_id,
                                                                          rel=ABCPropertyGraph.REL_HAS,
                                                                          parent=ABCPropertyGraph.CLASS_NetworkNode)

                    peer_node = slice_graph.build_deep_node_sliver(node_id=peer_node_id)

                    peer_site = f'{ovs_ns.get_site()},{peer_node.get_type()},{peer_node.get_name()}'
                break

        ret_val = InterfaceSliverMapping()
        ret_val.set_properties(peer_ifs=peer_ifs, peer_ns_id=peer_ns_id, component_name=component_name,
                               node_id=node_id, facility=facility, peer_site=peer_site)
        return ret_val

    @staticmethod
    def get_peer_interfaces(ifs_node_id: str, graph: ABCPropertyGraph,
                            interface_type: InterfaceType = None) -> List[InterfaceSliver]:
        """
        Finds Peer Interface Sliver and parent information upto Network Node
        @param ifs_node_id node id of the Interface Sliver
        @param graph Slice ASM
        @param interface_type Interface Type
        @returns Interface Sliver Mapping
        """
        result = []
        candidates = graph.find_peer_connection_points(node_id=ifs_node_id)
        if candidates is None:
            return result
        for c in candidates:
            clazzes, node_props = graph.get_node_properties(node_id=c)

            # Peer Connection point maps to Interface Sliver
            # Build Interface Sliver
            peer_ifs = ABCPropertyGraph.interface_sliver_from_graph_properties_dict(d=node_props)
            if interface_type is not None and peer_ifs.get_type() == interface_type:
                result.append(peer_ifs)
                break
            else:
                result.append(peer_ifs)
        return result

    @staticmethod
    def get_site_interface_sliver(*, component: ComponentSliver or NodeSliver, local_name: str,
                                  region: str = None, device_name: str = None) -> InterfaceSliver or None:
        """
        Get Interface Sliver (child of Component Sliver) with a local name

        E.g: Component renc-w3-nic2 has two connection points; this function returns the connection point whose
        name matches the local name
        renc-w3         => renc-w3-nic2         => renc-w3-nic2-l2ovs   => renc-w3-nic2-p1
        [Network Node]     [Component]             [Network Service]       [Connection Point]
                                                                        => renc-w3-nic2-p2
                                                                           [Connection Point]

        @param component Component Sliver
        @param local_name Local Name
        @param region region
        @param device_name device name
        @return Interface sliver
        """
        result = None
        for ns in component.network_service_info.network_services.values():
            if not ns.interface_info:
                continue

            # Filter on region
            if region is not None:
                result = list(filter(lambda x: (region in x.labels.region), ns.interface_info.interfaces.values()))
            else:
                result = list(ns.interface_info.interfaces.values())

            # Filter on device name
            if device_name is not None:
                result = list(filter(lambda x: (device_name in x.labels.device_name), result))

            if local_name is not None:
                result = list(filter(lambda x: (local_name in x.labels.local_name), result))

            if result is not None:
                break

        if result is None or len(result) == 0:
            raise Exception(f"No interface found to service region {region}, device: {device_name} "
                            f"local_name: {local_name} in component: {component}")

        return random.choice(result)

    @staticmethod
    def get_owners(*, bqm: ABCCBMPropertyGraph, node_id: str,
                   ns_type: ServiceType) -> Tuple[NodeSliver, NetworkServiceSliver, NetworkServiceSliver]:
        """
        Get owner switch and network service of a Connection Point from BQM
        @param bqm BQM graph
        @param node_id Connection Point Node Id
        @param ns_type Network Service Type
        @return Owner Switch and Network Service
        """
        mpls_ns_name, mpls_ns_id = bqm.get_parent(node_id=node_id, rel=ABCPropertyGraph.REL_CONNECTS,
                                                  parent=ABCPropertyGraph.CLASS_NetworkService)

        mpls_ns = bqm.build_deep_ns_sliver(node_id=mpls_ns_id)

        sw_name, sw_id = bqm.get_parent(node_id=mpls_ns_id, rel=ABCPropertyGraph.REL_HAS,
                                        parent=ABCPropertyGraph.CLASS_NetworkNode)

        switch = bqm.build_deep_node_sliver(node_id=sw_id)

        requested_ns = mpls_ns
        if ns_type in Constants.L3_SERVICES:
            for ns in switch.network_service_info.network_services.values():
                if ns_type == ns.get_type():
                    requested_ns = ns
                    break

        return switch, mpls_ns, requested_ns

    @staticmethod
    def get_parent_node(*, graph_model: ABCPropertyGraph, component: Component = None, interface: Interface = None,
                        sliver: bool = True) -> Tuple[Union[NodeSliver, NetworkServiceSliver, None], str]:
        node = None
        if component is not None:
            node_name, node_id = graph_model.get_parent(node_id=component.node_id, rel=ABCPropertyGraph.REL_HAS,
                                                        parent=ABCPropertyGraph.CLASS_NetworkNode)
            if sliver:
                node = graph_model.build_deep_node_sliver(node_id=node_id)
        elif interface is not None:
            node_name, node_id = graph_model.get_parent(node_id=interface.node_id, rel=ABCPropertyGraph.REL_CONNECTS,
                                                        parent=ABCPropertyGraph.CLASS_NetworkService)
            if sliver:
                node = graph_model.build_deep_ns_sliver(node_id=node_id)
        else:
            raise Exception("Invalid Arguments - component/interface both are None")

        return node, node_id

    @staticmethod
    def prune_graph(*, graph_id: str) -> ExperimentTopology:
        """
        Load arm graph from fim and prune all nodes with reservation_state = reservation_state
        :param graph_id: graph_id
        :return: ExperimentTopology
        """
        slice_topology = FimHelper.get_experiment_topology(graph_id=graph_id)
        slice_topology.prune(reservation_state=ReservationStates.Failed.name)
        slice_topology.prune(reservation_state=ReservationStates.Closed.name)
        slice_topology.prune(reservation_state=ReservationStates.CloseFail.name)

        return slice_topology

    @staticmethod
    def get_workers(site: CompositeNode) -> dict:
        node_id_list = site.topo.graph_model.get_first_neighbor(
            node_id=site.node_id,
            rel=ABCPropertyGraph.REL_HAS,
            node_label=ABCPropertyGraph.CLASS_NetworkNode,
        )
        workers = dict()
        for nid in node_id_list:
            _, node_props = site.topo.graph_model.get_node_properties(node_id=nid)
            n = Node(
                name=node_props[ABCPropertyGraph.PROP_NAME],
                node_id=nid,
                topo=site.topo,
            )
            if n.type != NodeType.Facility:
                workers[n.name] = n
        return workers

    @staticmethod
    def build_broker_query_model(db: ABCDatabase, level_0_broker_query_model: str, level: int,
                                 graph_format: GraphFormat = GraphFormat.GRAPHML,
                                 start: datetime = None, end: datetime = None,
                                 includes: str = None, excludes: str = None) -> str:
        if level == 2:
            sites_to_include = [s.strip().upper() for s in includes.split(",")] if includes else []
            sites_to_exclude = [s.strip().upper() for s in excludes.split(",")] if excludes else []

            if level_0_broker_query_model and len(level_0_broker_query_model) > 0:
                topology = AdvertizedTopology()

                nx_pgraph = topology.graph_model.importer.import_graph_from_string(graph_string=level_0_broker_query_model)
                topology.graph_model = NetworkXABQMFactory.create(nx_pgraph)

                sites_to_remove = []

                for site_name, site in topology.sites.items():
                    if len(sites_to_include) and site_name not in sites_to_include:
                        sites_to_remove.append(site_name)
                        continue

                    if len(sites_to_exclude) and site_name in sites_to_exclude:
                        sites_to_remove.append(site_name)
                        continue

                    site_cap_alloc = Capacities()

                    for child_name, child in site.children.items():
                        allocated_caps, allocated_comp_caps = AggregatedBQMPlugin.occupied_node_capacity(db=db,
                                                                                                         node_id=child.node_id,
                                                                                                         start=start,
                                                                                                         end=end)
                        site_cap_alloc += allocated_caps
                        child.set_property(pname="capacity_allocations", pval=allocated_caps)

                        # merge allocated component capacities
                        for kt, v in allocated_comp_caps.items():
                            for km, vcap in v.items():
                                name = f"{kt}-{km}"
                                if child.components.get(name) is not None:
                                    capacity_allocations = Capacities()
                                    if child.components[name].capacity_allocations:
                                        capacity_allocations = child.components[name].capacity_allocations
                                    capacity_allocations += vcap
                                    child.components[name].set_property(pname="capacity_allocations",
                                                                        pval=capacity_allocations)

                for s in sites_to_remove:
                    topology.remove_node(s)

                for f_name, facility in topology.facilities.items():
                    for if_name, interface in facility.interfaces.items():
                        allocated_vlans = AggregatedBQMPlugin.occupied_vlans(db=db, node_id=f_name,
                                                                             component_name=interface.node_id,
                                                                             start=start, end=end)
                        if allocated_vlans and len(allocated_vlans):
                            label_allocations = Labels(vlan=allocated_vlans)
                            interface.set_property(pname="label_allocations", pval=label_allocations)

                return topology.serialize(fmt=graph_format)
