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
# Author: Ilya Baldin (ibaldin@renci.org)

from collections import defaultdict

import uuid

from fim.graph.abc_property_graph import ABCPropertyGraph
from fim.graph.resources.abc_cbm import ABCCBMPropertyGraph
from fim.graph.resources.abc_bqm import ABCBQMPropertyGraph
from fim.graph.networkx_property_graph import NetworkXGraphImporter
from fim.graph.resources.networkx_abqm import NetworkXAggregateBQM
from fim.slivers.capacities_labels import Capacities
from fim.slivers.network_node import CompositeNodeSliver, NodeType
from fim.slivers.attached_components import ComponentSliver
from fim.slivers.switch_fabric import SFType
from fim.slivers.interface_info import InterfaceType


class AggregatedBQMPlugin:
    """
    Implement a plugin for simple aggregation of CBM into BQM, transforming site
    topologies into CompositeNodes and linking them with (Composite)Links
    This is based on fim.pluggable BrokerPluggable
    """
    # set to true to test creating ABQM without talking to database or requiring
    # an actor reference
    DEBUG_FLAG = False

    def __init__(self, actor, logger=None):
        if not self.DEBUG_FLAG:
            assert actor is not None
        self.actor = actor
        self.log = logger

    def plug_produce_bqm(self, *, cbm: ABCCBMPropertyGraph, **kwargs) -> ABCBQMPropertyGraph:
        """
        Take a CBM, sort nodes by site, aggregate servers, components and interfaces to
        create a site-based advertisement. Use a NetworkX-based implementation.
        :param cbm:
        :param kwargs:
        :return:
        """
        if kwargs.get('query_level', None) is None or kwargs['query_level'] != 1:
            return cbm.clone_graph(new_graph_id=str(uuid.uuid4()))

        # do a one-pass aggregation of servers, their components and interfaces
        nnodes = cbm.get_all_nodes_by_class(label=ABCPropertyGraph.CLASS_NetworkNode)
        slivers_by_site = defaultdict(list)
        for n in nnodes:
            # build deep slivers for each advertised server, aggregate by site
            node_sliver = cbm.build_deep_node_sliver(node_id=n)
            slivers_by_site[node_sliver.site].append(node_sliver)

        # create a new blank Aggregated BQM NetworkX graph
        abqm = NetworkXAggregateBQM(graph_id=str(uuid.uuid4()),
                                    importer=NetworkXGraphImporter(logger=self.log),
                                    logger=self.log)

        site_to_composite_node_id = dict()
        site_to_sf_node_id = dict()
        for s, ls in slivers_by_site.items():
            # add up capacities and delegated capacities, skip labels for now
            # count up components and figure out links between sites
            site_sliver = CompositeNodeSliver()
            site_sliver.capacities = Capacities()
            site_sliver.resource_name = s
            site_sliver.resource_type = NodeType.Server
            site_comps_by_type = defaultdict(dict)
            for sliver in ls:
                if sliver.get_type() != NodeType.Server:
                    # skipping NAS and dataplane switches
                    continue
                site_sliver.capacities = site_sliver.capacities + sliver.capacities
                # collect components in lists by type and model for the site
                if sliver.attached_components_info is None:
                    continue
                for comp in sliver.attached_components_info.list_devices():
                    if site_comps_by_type[comp.resource_type].get(comp.resource_model, None) is None:
                        site_comps_by_type[comp.resource_type][comp.resource_model] = list()
                    site_comps_by_type[comp.resource_type][comp.resource_model].append(comp)


            # create a Composite node for every site
            site_node_id = str(uuid.uuid4())
            site_to_composite_node_id[s] = site_node_id
            site_props = abqm.node_sliver_to_graph_properties_dict(site_sliver)
            abqm.add_node(node_id=site_node_id, label=ABCPropertyGraph.CLASS_CompositeNode,
                          props=site_props)
            # add a switch fabric
            sf_id = str(uuid.uuid4())
            site_to_sf_node_id[s] = sf_id
            sf_props = {ABCPropertyGraph.PROP_NAME: s + '_sf',
                        ABCBQMPropertyGraph.PROP_TYPE: str(SFType.SwitchFabric)}
            abqm.add_node(node_id=sf_id, label=ABCPropertyGraph.CLASS_SwitchFabric, props=sf_props)
            abqm.add_link(node_a=site_node_id, rel=ABCPropertyGraph.REL_HAS, node_b=sf_id)

            # create a component sliver for every component type/model pairing
            # and add a node for it linking back to site node
            for ctype, cdict in site_comps_by_type.items():
                for cmodel, comp_list in cdict.items():
                    comp_sliver = ComponentSliver()
                    comp_sliver.capacities = Capacities()
                    comp_sliver.set_type(ctype)
                    comp_sliver.set_model(cmodel)
                    comp_sliver.set_name(str(ctype) + '-' + cmodel)
                    for comp in comp_list:
                        comp_sliver.capacities = comp_sliver.capacities + comp.capacities
                    comp_node_id = str(uuid.uuid4())
                    comp_props = abqm.component_sliver_to_graph_properties_dict(comp_sliver)
                    abqm.add_node(node_id=comp_node_id, label=ABCPropertyGraph.CLASS_Component,
                                  props=comp_props)
                    abqm.add_link(node_a=site_node_id, rel=ABCPropertyGraph.REL_HAS,
                                  node_b=comp_node_id)

        # get all intersite links - add them to the aggregated BQM graph
        intersite_links = cbm.get_intersite_links()
        for l in intersite_links:
            source_switch = l[0]
            sink_switch = l[2]
            link = l[1]
            source_site = l[3]
            sink_site = l[4]
            source_cp = l[5]
            sink_cp = l[6]
            _, cbm_source_cp_props = cbm.get_node_properties(node_id=source_cp)
            _, cbm_sink_cp_props = cbm.get_node_properties(node_id=sink_cp)
            _, cbm_link_props = cbm.get_node_properties(node_id=link)
            # add connection point, link, connection point between to SwitchFabrics
            assert(site_to_sf_node_id.get(source_site, None) is not None and
                   site_to_sf_node_id.get(sink_site, None) is not None)
            source_cp_id = str(uuid.uuid4())
            sink_cp_id = str(uuid.uuid4())
            source_cp_props = {ABCPropertyGraph.PROP_NAME: "_".join([source_site, sink_site]),
                               ABCPropertyGraph.PROP_TYPE: str(InterfaceType.TrunkPort),
                               ABCPropertyGraph.PROP_CLASS: ABCPropertyGraph.CLASS_ConnectionPoint,
                               ABCPropertyGraph.PROP_CAPACITIES: cbm_source_cp_props[ABCPropertyGraph.PROP_CAPACITIES]}
            abqm.add_node(node_id=source_cp_id, label=ABCPropertyGraph.CLASS_ConnectionPoint,
                          props=source_cp_props)
            # FIXME: CP names may not be unique if we are dealing with a multigraph
            sink_cp_props = {ABCPropertyGraph.PROP_NAME: "_".join([sink_site, source_site]),
                             ABCPropertyGraph.PROP_TYPE: str(InterfaceType.TrunkPort),
                             ABCPropertyGraph.PROP_CLASS: ABCPropertyGraph.CLASS_ConnectionPoint,
                             ABCPropertyGraph.PROP_CAPACITIES: cbm_sink_cp_props[ABCPropertyGraph.PROP_CAPACITIES]}
            abqm.add_node(node_id=sink_cp_id, label=ABCPropertyGraph.CLASS_ConnectionPoint,
                          props=sink_cp_props)
            # selectively replicate link node and its properties from CBM
            new_link_props = {ABCPropertyGraph.PROP_NAME: cbm_link_props[ABCPropertyGraph.PROP_NAME],
                              ABCPropertyGraph.PROP_TYPE: cbm_link_props[ABCPropertyGraph.PROP_TYPE],
                              ABCPropertyGraph.PROP_CLASS: cbm_link_props[ABCPropertyGraph.PROP_CLASS],
                              ABCPropertyGraph.PROP_LAYER: cbm_link_props[ABCPropertyGraph.PROP_LAYER]
                              }
            abqm.add_node(node_id=link, label=ABCPropertyGraph.CLASS_Link, props=new_link_props)
            # connect them together
            abqm.add_link(node_a=site_to_sf_node_id[source_site], rel=ABCPropertyGraph.REL_CONNECTS,
                          node_b=source_cp_id)
            abqm.add_link(node_a=source_cp_id, rel=ABCPropertyGraph.REL_CONNECTS,
                          node_b=link)
            abqm.add_link(node_a=link, rel=ABCPropertyGraph.REL_CONNECTS,
                          node_b=sink_cp_id)
            abqm.add_link(node_a=sink_cp_id, rel=ABCPropertyGraph.REL_CONNECTS,
                          node_b=site_to_sf_node_id[sink_site])

        return abqm
