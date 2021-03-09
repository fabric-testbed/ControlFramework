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
from fim.slivers.capacities_labels import Capacities
from fim.slivers.network_node import NodeSliver

from fabric_cf.actor.core.common.constants import Constants


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
    def get_graph_from_string_direct(*, graph_str: str) -> Neo4jPropertyGraph:
        """
        Load arm graph from neo4j
        :param graph_str: graph_str
        :return: Neo4jPropertyGraph
        """
        neo4j_graph_importer = Neo4jHelper.get_neo4j_importer()
        graph = neo4j_graph_importer.import_graph_from_string_direct(graph_string=graph_str)

        return graph

    @staticmethod
    def get_graph_from_string(*, graph_str: str) -> Neo4jPropertyGraph:
        """
        Load arm graph from neo4j
        :param graph_str: graph_str
        :return: Neo4jPropertyGraph
        """
        neo4j_graph_importer = Neo4jHelper.get_neo4j_importer()
        graph = neo4j_graph_importer.import_graph_from_string(graph_string=graph_str)

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
    def get_delegation(delegated_capacities: list, delegation_name: str) -> Capacities:
        """
        Get Delegated capacity given delegation name
        :param delegated_capacities: list of delegated capacities
        :param delegation_name: delegation name
        :return: capacity for specified delegation
        """
        for capacity_dict in delegated_capacities:
            name = capacity_dict.get(ABCPropertyGraph.FIELD_DELEGATION, None)
            if name == delegation_name:
                capacity_dict.pop(ABCPropertyGraph.FIELD_DELEGATION)
                return Capacities().from_json(json.dumps(capacity_dict))
        return None

    @staticmethod
    def get_node_sliver_props(*, sliver: NodeSliver) -> dict:
        """
        Get Node sliver properties to be updated to Slice graph
        :param sliver: sliver
        :return: dictionary containing the properties that need to be updated
        """
        if sliver is not None:
            result = {}
            if sliver.management_ip is not None:
                result[ABCPropertyGraph.PROP_MGMT_IP] = sliver.management_ip
            if sliver.management_interface_mac_address is not None:
                result[Constants.MANAGEMENT_INTERFACE_MAC_ADDRESS] = sliver.management_interface_mac_address
            if sliver.instance_name is not None:
                result[Constants.INSTANCE_NAME] = sliver.instance_name
            if sliver.state is not None:
                result[Constants.INSTANCE_STATE] = sliver.state
            if sliver.worker_node_name is not None:
                result[Constants.WORKER_NODE_NAME] = sliver.worker_node_name
            if sliver.bqm_node_id is not None:
                result[Constants.BQM_NODE_ID] = sliver.bqm_node_id
            return result
        return sliver
