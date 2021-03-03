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

import fim.graph.slices.networkx_asm as nx_asm
from fim.graph.neo4j_property_graph import Neo4jPropertyGraph
from fim.slivers.network_node import NodeSliver


class RequestWorkflow:
    """
    This object implements the generic workflow of turning a ASM into reservations.
    """
    def __init__(self, logger):
        self.logger = logger

    def close(self):
        """ Close any open graph models """

    def run(self, *, bqm: Neo4jPropertyGraph, slice_graph: str) -> List[NodeSliver]:
        """
        Run the workflow and Translate Slice Graph (ASM) into individual slivers
        @param bqm: broker query model
        @param slice_graph: slice graph
        """
        slivers = []

        gi = nx_asm.NetworkXGraphImporter(logger=self.logger)
        g = gi.import_graph_from_string_direct(graph_string=slice_graph)
        asm = nx_asm.NetworkxASM(graph_id=g.graph_id, importer=g.importer, logger=g.importer.log)

        for nn_id in asm.get_all_network_nodes():
            sliver = asm.build_deep_node_sliver(node_id=nn_id)
            slivers.append(sliver)

        # TODO match from BQM

        return slivers
