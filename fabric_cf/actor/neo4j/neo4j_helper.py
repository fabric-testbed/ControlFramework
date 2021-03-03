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
from fim.graph.resources.neo4j_cbm import Neo4jCBMGraph
from fim.graph.slices.abc_asm import ABCASMPropertyGraph
from fim.slivers.attached_components import AttachedComponentsInfo, ComponentSliver
from fim.slivers.capacities_labels import Capacities, Labels
from fim.slivers.network_node import NodeSliver

from fabric_cf.actor.neo4j.neo4j_graph_node import Neo4jGraphNode


class Neo4jHelper:
    """
    Provides methods to load Graph Models and perform various operations on them
    """
    @staticmethod
    def get_neo4j_importer() -> Neo4jGraphImporter:
        """
        get neo4j graph importer
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
    def get_arm_graph_from_file(*, filename: str) -> ABCPropertyGraph:
        """
        Load specified file directly with no manipulations or validation
        :param filename:
        :return:
        """
        neo4j_graph_importer = Neo4jHelper.get_neo4j_importer()
        neo4_graph = neo4j_graph_importer.import_graph_from_file(graph_file=filename)
        neo4_graph.validate_graph()

        arm_graph = Neo4jARMGraph(graph=neo4_graph)

        return arm_graph

    @staticmethod
    def get_arm_graph(*, graph_id: str) -> Neo4jARMGraph:
        """
        Load arm graph from neo4j
        :param graph_id: graph_id
        :return: Neo4jARMGraph
        """
        neo4j_graph_importer = Neo4jHelper.get_neo4j_importer()
        arm_graph = Neo4jARMGraph(graph=Neo4jPropertyGraph(graph_id=graph_id, importer=neo4j_graph_importer))

        return arm_graph

    @staticmethod
    def get_graph(*, graph_id: str) -> Neo4jPropertyGraph:
        """
        Load arm graph from neo4j
        :param graph_id: graph_id
        :return: Neo4jARMGraph
        """
        neo4j_graph_importer = Neo4jHelper.get_neo4j_importer()
        arm_graph = Neo4jPropertyGraph(graph_id=graph_id, importer=neo4j_graph_importer)

        return arm_graph

    @staticmethod
    def get_neo4j_cbm_empty_graph() -> Neo4jCBMGraph:
        """
        Load cmb empty graph
        :return: Neo4jCBMGraph
        """
        neo4j_graph_importer = Neo4jHelper.get_neo4j_importer()
        combined_broker_model = Neo4jCBMGraph(importer=neo4j_graph_importer, logger=neo4j_graph_importer.log)
        return combined_broker_model

    @staticmethod
    def get_neo4j_cbm_graph_from_database(combined_broker_model_graph_id: str) -> Neo4jCBMGraph:
        """
        Load cbm graph from neo4j
        :param combined_broker_model_graph_id: combined_broker_model_graph_id
        :return: Neo4jCBMGraph
        """
        neo4j_graph_importer = Neo4jHelper.get_neo4j_importer()
        combined_broker_model = Neo4jCBMGraph(graph_id=combined_broker_model_graph_id,
                                              importer=neo4j_graph_importer,
                                              logger=neo4j_graph_importer.log)
        if combined_broker_model.graph_exists():
            combined_broker_model.validate_graph()
        return combined_broker_model

    @staticmethod
    def get_graph_from_string(*, graph_str: str) -> Neo4jPropertyGraph:
        """
        Load arm graph from neo4j
        :param graph_str: graph_str
        :return: Neo4jPropertyGraph
        """
        neo4j_graph_importer = Neo4jHelper.get_neo4j_importer()
        graph = neo4j_graph_importer.import_graph_from_string_direct(graph_string=graph_str)

        return graph

    @staticmethod
    def delete_graph(*, graph_id: str):
        """
        Delete a graph
        @param graph_id graph id
        """
        neo4j_graph_importer = Neo4jHelper.get_neo4j_importer()
        neo4j_graph_importer.delete_graph(graph_id=graph_id)

    @staticmethod
    def get_node_from_graph(*, graph:Neo4jPropertyGraph, node_id: str) -> Neo4jGraphNode:
        """
        Build a deep NetworkNode or other similar (e.g.
        network-attached storage) sliver from a graph node
        :param graph:
        :param node_id:
        :return:
        """
        clazzes, props = graph.get_node_properties(node_id=node_id)

        neo4j_node = Neo4jGraphNode(node_id=node_id)
        resource_type = props.get(ABCPropertyGraph.PROP_TYPE, None)
        resource_model = props.get(ABCPropertyGraph.PROP_MODEL, None)

        neo4j_node.set_resource_type(resource_type=resource_type)
        neo4j_node.set_resource_model(resource_model=resource_model)
        labels = props.get(ABCPropertyGraph.PROP_LABELS, None)

        if labels is not None and labels != 'None':
            neo4j_node.set_labels(labels=Labels().from_json(labels))

        capacities = props.get(ABCPropertyGraph.PROP_CAPACITIES, None)

        if capacities is not None and capacities != 'None':
            neo4j_node.set_capacities(capacities=Capacities().from_json(capacities))

        capacity_del_json_list = graph.get_node_json_property_as_object(node_id=node_id,
                                                                        prop_name=ABCPropertyGraph.PROP_CAPACITY_DELEGATIONS)

        # Broker has dictionary of delegations
        if capacity_del_json_list is not None and isinstance(capacity_del_json_list, dict):
            # AM
            key = capacity_del_json_list.get(ABCPropertyGraph.FIELD_DELEGATION, None)
            if key is not None:
                capacity_del_json_list.pop(ABCPropertyGraph.FIELD_DELEGATION)
                neo4j_node.add_capacity_delegation(del_id=key,
                                                   capacities=Capacities().from_json(json.dumps(capacity_del_json_list)))
            #Broker
            else:
                for key, value in capacity_del_json_list.items():
                    value_json = next(iter(value))
                    neo4j_node.add_capacity_delegation(del_id=key,
                                                       capacities=Capacities().from_json(json.dumps(value_json)))

        # AM has list of delegations
        if capacity_del_json_list is not None and isinstance(capacity_del_json_list, list):
            for c_dict in capacity_del_json_list:
                key = c_dict.get(ABCPropertyGraph.FIELD_DELEGATION, None)
                c_dict.pop(ABCPropertyGraph.FIELD_DELEGATION)
                neo4j_node.add_capacity_delegation(del_id=key,
                                                   capacities=Capacities().from_json(json.dumps(c_dict)))

        label_del_json_list = graph.get_node_json_property_as_object(node_id=node_id,
                                                                     prop_name=ABCPropertyGraph.PROP_LABEL_DELEGATIONS)

        if label_del_json_list is not None:
            for key, value in label_del_json_list.items():
                value_json = next(iter(value))
                neo4j_node.add_label_delegation(del_id=key, labels=Labels().from_json(json.dumps(value_json)))

        # find and build deep slivers of switch fabrics (if any) and components (if any)
        comps = graph.get_first_neighbor(node_id=node_id, rel=ABCPropertyGraph.REL_HAS,
                                         node_label=ABCPropertyGraph.CLASS_Component)
        if comps is not None and len(comps) > 0:
            for c in comps:
                component_node = Neo4jHelper.get_node_from_graph(graph=graph, node_id=c)
                neo4j_node.add_component(component=component_node)

        return neo4j_node
