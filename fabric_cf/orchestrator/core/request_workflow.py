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
from typing import List

from fim.graph.abc_property_graph import ABCPropertyGraph
from fim.graph.neo4j_property_graph import Neo4jPropertyGraph
from fim.slivers.base_sliver import BaseElement
from fim.slivers.network_node import Node


class RequestWorkflow:
    """
    This object implements the generic workflow of turning a ASM into reservations.
    """
    # Only used for testing
    WORKER_NODE_ID = "2046922a-a8ed-4b60-8190-b6ce614c514d"

    def close(self):
        """ Close any open graph models """

    def run(self, *, bqm: Neo4jPropertyGraph, slice_graph: str) -> List[BaseElement]:
        """
        Run the workflow
        """
        # Translate Slice Graph (ASM) into individual slivers
        # TODO
        # HACK for now to create a compute Sliver
        slivers = []
        compute_node = Node()
        compute_node.set_graph_node_id(graph_node_id=self.WORKER_NODE_ID)
        compute_node.set_image_type(image_type="QCOW2")
        compute_node.set_image_ref(image_ref="centos7")
        compute_node.set_resource_type("VM")

        capacity_delegations = bqm.get_node_json_property_as_object(node_id=self.WORKER_NODE_ID,
                                                                    prop_name=ABCPropertyGraph.PROP_CAPACITY_DELEGATIONS)

        capacity_values = next(iter(capacity_delegations.values()))
        if capacity_values is not None and len(capacity_values) > 0:
            compute_node.set_cpu_cores(cpu_cores=int(capacity_values[0].get('core')))
            compute_node.set_ram_size(ram_size=int(capacity_values[0].get('ram')))
            compute_node.set_disk_size(disk_size=int(capacity_values[0].get('disk')))

        slivers.append(compute_node)
        return slivers
