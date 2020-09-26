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
from fabric.actor.boot.inventory.resource_pool_factory import ResourcePoolFactory
from fim.graph.neo4j_property_graph import Neo4jGraphImporter


class Neo4jResourcePoolFactory(ResourcePoolFactory):
    def load_file_direct(self, *, filename, neo4j_config):
        """
        Load specified file directly with no manipulations or validation
        :param filename:
        :param neo4j_config:
        :return:
        """
        neo4j_graph_importer = Neo4jGraphImporter(url=neo4j_config["url"], user=neo4j_config["user"],
                                                  pswd=neo4j_config["pass"],
                                                  import_host_dir=neo4j_config["import_host_dir"],
                                                  import_dir=neo4j_config["import_dir"])
        neo4_graph = neo4j_graph_importer.import_graph_from_file_direct(graph_file=filename)
        neo4_graph.validate_graph()

        return neo4_graph

    def update_descriptor(self):
        super().update_descriptor()
