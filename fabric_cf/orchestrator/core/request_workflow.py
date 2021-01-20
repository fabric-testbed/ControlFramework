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

from fim.graph.neo4j_property_graph import Neo4jPropertyGraph
from fim.slivers.base_sliver import BaseElement
from fim.slivers.network_node import Node


class RequestWorkflow:
    """
    This object implements the generic workflow of turning a ASM into reservations.
    """
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
        compute_node.set_resource_type(resource_type="VM")
        compute_node.set_cpu_cores(cpu_cores=4)
        compute_node.set_ram_size(ram_size=256)
        compute_node.set_disk_size(disk_size=1)
        compute_node.set_image_type(image_type="QCOW2")
        compute_node.set_image_ref(image_ref="centos7")

        slivers.append(compute_node)
        return slivers
