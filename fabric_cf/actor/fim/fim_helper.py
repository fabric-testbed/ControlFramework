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
from typing import Tuple

from fim.graph.abc_property_graph import ABCPropertyGraph, ABCGraphImporter
from fim.graph.neo4j_property_graph import Neo4jGraphImporter, Neo4jPropertyGraph
from fim.graph.networkx_property_graph import NetworkXGraphImporter
from fim.graph.resources.abc_arm import ABCARMPropertyGraph
from fim.graph.resources.abc_cbm import ABCCBMPropertyGraph
from fim.graph.resources.neo4j_arm import Neo4jARMGraph
from fim.graph.resources.neo4j_cbm import Neo4jCBMGraph, Neo4jCBMFactory
from fim.graph.slices.abc_asm import ABCASMPropertyGraph
from fim.graph.slices.neo4j_asm import Neo4jASMFactory, Neo4jASM
from fim.graph.slices.networkx_asm import NetworkxASM, NetworkXASMFactory
from fim.slivers.attached_components import ComponentSliver
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Capacities
from fim.slivers.delegations import Delegations
from fim.slivers.interface_info import InterfaceSliver
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver
from fim.user import ExperimentTopology, Labels


class InterfaceSliverMapping:
    def __init__(self):
        # Maps to Connection Point in the Graph
        self.peer_ifs = None
        # Maps to Network Service (Parent of Peer IFS) in the Graph
        self.peer_ns_id = None
        # Maps to Component (Parent of Network Service) in the Graph
        self.component_name = None
        # Maps to the Network Node (Parent of the Component) in the Graph
        self.node_id = None

    def get_peer_ifs(self) -> InterfaceSliver:
        return self.peer_ifs

    def get_peer_ns_id(self) -> str:
        return self.peer_ns_id

    def get_component_name(self) -> str:
        return self.component_name

    def get_node_id(self) -> str:
        return self.node_id

    def set_peer_ifs(self, peer_ifs: InterfaceSliver):
        self.peer_ifs = peer_ifs

    def set_peer_ns_id(self, peer_ns_id: str):
        self.peer_ns_id = peer_ns_id

    def set_component_name(self, component_name: str):
        self.component_name = component_name

    def set_node_id(self, node_id: str):
        self.node_id = node_id

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
    @staticmethod
    def get_neo4j_importer() -> ABCGraphImporter:
        """
        get fim graph importer
        :return: Neo4jGraphImporter
        """
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        neo4j_config = GlobalsSingleton.get().get_config().get_global_config().get_neo4j_config()
        logger = GlobalsSingleton.get().get_logger()

        neo4j_graph_importer = Neo4jGraphImporter(url=neo4j_config["url"], user=neo4j_config["user"],
                                                  pswd=neo4j_config["pass"],
                                                  import_host_dir=neo4j_config["import_host_dir"],
                                                  import_dir=neo4j_config["import_dir"], logger=logger)
        return neo4j_graph_importer

    @staticmethod
    def get_networkx_importer() -> ABCGraphImporter:
        """
        get fim graph importer
        :return: Neo4jGraphImporter
        """
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        logger = GlobalsSingleton.get().get_logger()

        return NetworkXGraphImporter(logger=logger)

    @staticmethod
    def get_arm_graph_from_file(*, filename: str) -> ABCARMPropertyGraph:
        """
        Load specified file directly with no manipulations or validation
        :param filename:
        :return:
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer()
        neo4_graph = neo4j_graph_importer.import_graph_from_file(graph_file=filename)
        site_arm = Neo4jARMGraph(graph=Neo4jPropertyGraph(graph_id=neo4_graph.graph_id,
                                                          importer=neo4j_graph_importer))

        site_arm.validate_graph()

        return site_arm

    @staticmethod
    def get_arm_graph(*, graph_id: str) -> ABCARMPropertyGraph:
        """
        Load arm graph from fim
        :param graph_id: graph_id
        :return: Neo4jARMGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer()
        arm_graph = Neo4jARMGraph(graph=Neo4jPropertyGraph(graph_id=graph_id, importer=neo4j_graph_importer))
        if arm_graph.graph_exists():
            arm_graph.validate_graph()

        return arm_graph

    @staticmethod
    def get_graph(*, graph_id: str) -> ABCPropertyGraph:
        """
        Load arm graph from fim
        :param graph_id: graph_id
        :return: Neo4jARMGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer()
        arm_graph = Neo4jPropertyGraph(graph_id=graph_id, importer=neo4j_graph_importer)

        return arm_graph

    @staticmethod
    def get_neo4j_cbm_graph(graph_id: str) -> ABCCBMPropertyGraph:
        """
        Load cbm graph from fim
        :param graph_id: graph_id
        :return: Neo4jCBMGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer()
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
    def get_graph_from_string_direct(*, graph_str: str) -> ABCPropertyGraph:
        """
        Load arm graph from fim
        :param graph_str: graph_str
        :return: Neo4jPropertyGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer()
        graph = neo4j_graph_importer.import_graph_from_string_direct(graph_string=graph_str)

        return graph

    @staticmethod
    def get_graph_from_string(*, graph_str: str) -> ABCPropertyGraph:
        """
        Load arm graph from fim
        :param graph_str: graph_str
        :return: Neo4jPropertyGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer()
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
    def delete_graph(*, graph_id: str):
        """
        Delete a graph
        @param graph_id graph id
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer()
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
        if sliver is not None:
            graph = FimHelper.get_graph(graph_id=graph_id)
            asm_graph = Neo4jASMFactory.create(graph=graph)
            neo4j_topo = ExperimentTopology()
            neo4j_topo.cast(asm_graph=asm_graph)

            node_name = sliver.get_name()
            if isinstance(sliver, NodeSliver):
                node = neo4j_topo.nodes[node_name]
                node.set_properties(label_allocations=sliver.label_allocations,
                                    capacity_allocations=sliver.capacity_allocations,
                                    reservation_info=sliver.reservation_info,
                                    node_map=sliver.node_map,
                                    management_ip=sliver.management_ip)
                if sliver.attached_components_info is not None:
                    for component in sliver.attached_components_info.devices.values():
                        cname = component.get_name()
                        node.components[cname].set_properties(label_allocations=component.label_allocations,
                                                              capacity_allocations=component.capacity_allocations,
                                                              node_map=component.node_map)
            elif isinstance(sliver, NetworkServiceSliver):
                node = neo4j_topo.network_services[node_name]
                node.set_properties(label_allocations=sliver.label_allocations,
                                    capacity_allocations=sliver.capacity_allocations,
                                    reservation_info=sliver.reservation_info,
                                    node_map=sliver.node_map)
                # FIXME Update IFS on ASM; list_interfaces currently returns a list as opposed to Dict
                #if sliver.interface_info is not None:
                #    for ids in sliver.interface_info.interfaces.values():


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

        # Peer Connection point maps to Interface Sliver
        peer_ifs = FimHelper.get_interface_sliver_by_id(ifs_node_id=ifs_node_id, graph=slice_graph)

        peer_ns_node_name, peer_ns_id = slice_graph.get_parent(node_id=peer_ifs.node_id,
                                                               rel=ABCPropertyGraph.REL_CONNECTS,
                                                               parent=ABCPropertyGraph.CLASS_NetworkService)

        component_name, component_id = slice_graph.get_parent(node_id=peer_ns_id, rel=ABCPropertyGraph.REL_HAS,
                                                              parent=ABCPropertyGraph.CLASS_Component)

        node_name, node_id = slice_graph.get_parent(node_id=component_id, rel=ABCPropertyGraph.REL_HAS,
                                                    parent=ABCPropertyGraph.CLASS_NetworkNode)

        ret_val = InterfaceSliverMapping()
        ret_val.set_properties(peer_ifs=peer_ifs, peer_ns_id=peer_ns_id, component_name=component_name, node_id=node_id)
        return ret_val

    @staticmethod
    def get_interface_sliver_by_id(ifs_node_id: str, graph: ABCPropertyGraph) -> InterfaceSliver or None:
        """
        Finds Peer Interface Sliver and parent information upto Network Node
        @param ifs_node_id node id of the Interface Sliver
        @param graph Slice ASM
        @returns Interface Sliver Mapping
        """
        peer_cp_node_id = None

        candidates = graph.get_first_and_second_neighbor(node_id=ifs_node_id, rel1=ABCPropertyGraph.REL_CONNECTS,
                                                         node1_label=ABCPropertyGraph.CLASS_Link,
                                                         rel2=ABCPropertyGraph.REL_CONNECTS,
                                                         node2_label=ABCPropertyGraph.CLASS_ConnectionPoint)

        if len(candidates) == 0:
            return None
        if len(candidates) != 1:
            raise Exception(f"Connection point is only expected to connect to one other"
                            f"connection point, instead connects to {len(candidates)}")
        peer_cp_node_id = candidates[0][1]

        clazzes, node_props = graph.get_node_properties(node_id=peer_cp_node_id)

        # Peer Connection point maps to Interface Sliver
        # Build Interface Sliver
        peer_ifs = FimHelper.build_ifs_from_props(node_props=node_props)

        return peer_ifs

    @staticmethod
    def get_interface_sliver_by_component_id_local_name(component_id: str, bqm: ABCCBMPropertyGraph,
                                                        local_name: str) -> InterfaceSliver or None:
        """
        Find Interface Sliver matching a local name when the parent component Id is provided
        @param component_id node id of the parent component
        @param bqm Broker Query Model
        @param local_name local Name of the connection point
        @returns Interface Sliver
        """
        cp_ids = bqm.get_first_and_second_neighbor(node_id=component_id,
                                                   rel1=ABCPropertyGraph.REL_HAS,
                                                   node1_label=ABCPropertyGraph.CLASS_NetworkService,
                                                   rel2=ABCPropertyGraph.REL_CONNECTS,
                                                   node2_label=ABCPropertyGraph.CLASS_ConnectionPoint)

        cp = None
        if len(cp_ids) == 0:
            return None
        elif len(cp_ids) == 1:
            cp_id = cp_ids[0][1]
            _, cp_props = bqm.get_node_properties(node_id=cp_id)
            cp = FimHelper.build_ifs_from_props(node_props=cp_props)
        else:
            for c in cp_ids:
                _, cp_props = bqm.get_node_properties(node_id=c[1])
                if local_name in cp_props[ABCPropertyGraph.PROP_NAME]:
                    cp = FimHelper.build_ifs_from_props(node_props=cp_props)
                    break

        return cp

    @staticmethod
    def build_ifs_from_props(node_props: dict) -> InterfaceSliver:
        """
        Build Interface Sliver from the node properties
        @param node_props Node properties
        @return Interface Sliver
        """
        ifs = InterfaceSliver()
        ifs.node_id = node_props[ABCPropertyGraph.NODE_ID]
        cap_json = node_props.get(ABCPropertyGraph.PROP_CAPACITIES, None)
        labels_json = node_props.get(ABCPropertyGraph.PROP_LABELS, None)
        ifs.set_properties(name=node_props[ABCPropertyGraph.PROP_NAME],
                           type=node_props[ABCPropertyGraph.PROP_TYPE])
        if cap_json is not None:
            ifs.set_capacities(cap=Capacities().from_json(cap_json))

        if labels_json is not None:
            ifs.set_labels(lab=Labels().from_json(labels_json))
        return ifs

    @staticmethod
    def get_site_interface_sliver(*, component: ComponentSliver, local_name: str) -> InterfaceSliver or None:
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
        @return Interface sliver
        """
        for ns in component.network_service_info.network_services.values():
            for ifs in ns.interface_info.interfaces.values():
                if local_name in ifs.get_name():
                    return ifs
        return None

    @staticmethod
    def get_owners(*, bqm: ABCCBMPropertyGraph, node_id: str) -> Tuple[NodeSliver, NetworkServiceSliver]:
        """
        Get owner switch and network service of a Connection Point from BQM
        @param bqm BQM graph
        @param node_id Connection Point Node Id
        @return Owner Switch and Network Service
        """
        ns_name, ns_id = bqm.get_parent(node_id=node_id, rel=ABCPropertyGraph.REL_CONNECTS,
                                        parent=ABCPropertyGraph.CLASS_NetworkService)

        ns = bqm.build_deep_ns_sliver(node_id=ns_id)

        sw_name, sw_id = bqm.get_parent(node_id=ns_id, rel=ABCPropertyGraph.REL_HAS,
                                        parent=ABCPropertyGraph.CLASS_NetworkNode)

        switch = bqm.build_deep_node_sliver(node_id=sw_id)
        return switch, ns
