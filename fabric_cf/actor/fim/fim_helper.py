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
import json

from fim.graph.abc_property_graph import ABCPropertyGraph
from fim.graph.neo4j_property_graph import Neo4jGraphImporter, Neo4jPropertyGraph
from fim.graph.resources.neo4j_arm import Neo4jARMGraph
from fim.graph.resources.neo4j_cbm import Neo4jCBMGraph, Neo4jCBMFactory
from fim.graph.slices.neo4j_asm import Neo4jASMFactory, Neo4jASM
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Capacities
from fim.slivers.network_node import NodeSliver
from fim.user import ExperimentTopology


class FimHelper:
    """
    Provides methods to load Graph Models and perform various operations on them
    """
    @staticmethod
    def get_neo4j_importer() -> Neo4jGraphImporter:
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
    def get_arm_graph_from_file(*, filename: str) -> Neo4jARMGraph:
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
    def get_arm_graph(*, graph_id: str) -> Neo4jARMGraph:
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
    def get_graph(*, graph_id: str) -> Neo4jPropertyGraph:
        """
        Load arm graph from fim
        :param graph_id: graph_id
        :return: Neo4jARMGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer()
        arm_graph = Neo4jPropertyGraph(graph_id=graph_id, importer=neo4j_graph_importer)

        return arm_graph

    @staticmethod
    def get_neo4j_cbm_graph(graph_id: str) -> Neo4jCBMGraph:
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
    def get_neo4j_cbm_graph_from_string_direct(*, graph_str: str, ignore_validation: bool = False) -> Neo4jCBMGraph:
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
    def get_graph_from_string_direct(*, graph_str: str) -> Neo4jPropertyGraph:
        """
        Load arm graph from fim
        :param graph_str: graph_str
        :return: Neo4jPropertyGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer()
        graph = neo4j_graph_importer.import_graph_from_string_direct(graph_string=graph_str)

        return graph

    @staticmethod
    def get_graph_from_string(*, graph_str: str) -> Neo4jPropertyGraph:
        """
        Load arm graph from fim
        :param graph_str: graph_str
        :return: Neo4jPropertyGraph
        """
        neo4j_graph_importer = FimHelper.get_neo4j_importer()
        graph = neo4j_graph_importer.import_graph_from_string(graph_string=graph_str)

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
            node = neo4j_topo.nodes[node_name]
            node.set_properties(capacity_allocations=sliver.capacity_allocations,
                                reservation_info=sliver.reservation_info,
                                node_map=sliver.node_map,
                                management_ip=sliver.management_ip)
            if sliver.label_allocations is not None:
                node.set_properties(label_allocations=sliver.label_allocations)
            if sliver.attached_components_info is not None:
                for component in sliver.attached_components_info.devices.values():
                    cname = component.get_name()
                    node.components[cname].set_properties(capacity_allocations=component.capacity_allocations,
                                                          node_map=component.node_map)
                    if component.label_allocations is not None:
                        node.components[cname].set_properties(label_allocations=component.label_allocations)

    @staticmethod
    def get_neo4j_asm_graph(*, slice_graph: str) -> Neo4jASM:
        """
        Load Slice in Neo4j
        :param slice_graph: slice graph string
        :return: Neo4j ASM graph
        """
        neo4j_graph = FimHelper.get_graph_from_string(graph_str=slice_graph)
        asm = Neo4jASMFactory.create(graph=neo4j_graph)
        return asm
