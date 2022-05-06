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

from typing import Tuple, List, Dict
from collections import defaultdict

import uuid

from fim.graph.abc_property_graph import ABCPropertyGraph, PropertyGraphQueryException
from fim.graph.resources.abc_cbm import ABCCBMPropertyGraph
from fim.graph.resources.abc_bqm import ABCBQMPropertyGraph
from fim.graph.networkx_property_graph import NetworkXGraphImporter
from fim.graph.resources.networkx_abqm import NetworkXAggregateBQM
from fim.slivers.capacities_labels import Capacities
from fim.slivers.delegations import DelegationFormat
from fim.slivers.network_node import CompositeNodeSliver, NodeType
from fim.slivers.attached_components import ComponentSliver, ComponentType
from fim.slivers.interface_info import InterfaceType
from fim.slivers.network_service import ServiceType

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
        self.logger = logger

    @staticmethod
    def _remove_none_entries(d):
        return {k: v for (k, v) in d.items() if v}

    def __occupied_node_capacity(self, *, node_id: str) -> Tuple[Capacities,
                                                                 Dict[ComponentType, Dict[str, Capacities]]]:
        """
        Figure out the total capacity occupied in the network node and return a tuple of
        capacities occupied in this node and a dict of component capacities that are occupied
        organized by component type and model.
        """
        assert node_id is not None
        # get existing reservations for this node
        existing_reservations = self.actor.get_plugin().get_database().get_reservations(graph_node_id=node_id)

        # node capacities
        occupied_capacities = Capacities()
        occupied_component_capacities = defaultdict(dict)
        # Remove allocated capacities to the reservations
        if existing_reservations is not None:
            for reservation in existing_reservations:
                # For Active or Ticketed or Ticketing reservations; compute the counts from available
                allocated_sliver = None
                if reservation.is_ticketing() and reservation.get_approved_resources() is not None:
                    allocated_sliver = reservation.get_approved_resources().get_sliver()

                if (reservation.is_active() or reservation.is_ticketed()) and \
                        reservation.get_resources() is not None:
                    allocated_sliver = reservation.get_resources().get_sliver()

                if allocated_sliver is not None:
                    occupied_capacities = occupied_capacities + allocated_sliver.get_capacities()

                    if allocated_sliver.attached_components_info is not None:
                        for allocated_component in allocated_sliver.attached_components_info.devices.values():
                            rt = allocated_component.resource_type
                            rm = allocated_component.resource_model
                            if occupied_component_capacities[rt].get(rm) is None:
                                occupied_component_capacities[rt][rm] = Capacities()

                            occupied_component_capacities[rt][rm] = occupied_component_capacities[rt][rm] + \
                                                          allocated_component.capacity_allocations

        return occupied_capacities, occupied_component_capacities

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
        # this includes facilities
        nnodes = cbm.get_all_nodes_by_class(label=ABCPropertyGraph.CLASS_NetworkNode)
        slivers_by_site = defaultdict(list)
        for n in nnodes:
            # build deep slivers for each advertised server, aggregate by site
            node_sliver = cbm.build_deep_node_sliver(node_id=n)
            slivers_by_site[node_sliver.site].append(node_sliver)

        # create a new blank Aggregated BQM NetworkX graph
        abqm = NetworkXAggregateBQM(graph_id=str(uuid.uuid4()),
                                    importer=NetworkXGraphImporter(logger=self.logger),
                                    logger=self.logger)

        site_to_composite_node_id = dict()
        site_to_ns_node_id = dict()
        facilities_by_site = defaultdict(list)
        for s, ls in slivers_by_site.items():
            # add up capacities and delegated capacities, skip labels for now
            # count up components and figure out links between site

            site_sliver = CompositeNodeSliver()
            # count what is taken
            site_sliver.capacity_allocations = Capacities()
            # count what is available
            site_sliver.capacities = Capacities()
            site_sliver.resource_name = s
            site_sliver.resource_type = NodeType.Server
            site_sliver.node_id = str(uuid.uuid4())
            # available components organized by [type][model]
            site_comps_by_type = defaultdict(dict)
            # occupied component capacities organized by [type][model] into lists (by server)
            site_allocated_comps_caps_by_type = defaultdict(dict)

            loc = None
            for sliver in ls:
                if sliver.get_type() != NodeType.Server:
                    # skipping NAS, Facility and dataplane switches
                    if sliver.get_type() == NodeType.Facility:
                        # keep track of facilities for each site
                        facilities_by_site[s].append(sliver)
                    continue
                if self.DEBUG_FLAG:
                    # for debugging and running in a test environment
                    allocated_comp_caps = dict()
                else:
                    # query database for everything taken on this node
                    allocated_caps, allocated_comp_caps = self.__occupied_node_capacity(node_id=sliver.node_id)
                    site_sliver.capacity_allocations = site_sliver.capacity_allocations + allocated_caps

                # get the location if available
                if loc is None:
                    loc = sliver.get_location()

                # calculate available node capacities based on delegations
                if sliver.get_capacity_delegations() is not None:
                    # CBM only has one delegation if it has one
                    _, delegation = sliver.get_capacity_delegations().get_sole_delegation()
                    # FIXME: skip pool definitions and references for now
                    if delegation.get_format() == DelegationFormat.SinglePool:
                        site_sliver.capacities = site_sliver.capacities + \
                            delegation.get_details()

                # merge allocated component capacities
                for kt, v in allocated_comp_caps.items():
                    for km, vcap in v.items():
                        if site_allocated_comps_caps_by_type[kt].get(km) is None:
                            site_allocated_comps_caps_by_type[kt][km] = Capacities()
                        site_allocated_comps_caps_by_type[kt][km] = site_allocated_comps_caps_by_type[kt][km] + \
                                                                    vcap

                # collect available components in lists by type and model for the site (for later aggregation)
                if sliver.attached_components_info is None:
                    continue
                for comp in sliver.attached_components_info.list_devices():
                    rt = comp.resource_type
                    rm = comp.resource_model
                    if site_comps_by_type[rt].get(rm) is None:
                        site_comps_by_type[rt][rm] = list()
                    site_comps_by_type[rt][rm].append(comp)

            # set location to whatever is available
            site_sliver.set_location(loc)
            site_sliver.set_site(s)

            # create a Composite node for every site
            site_to_composite_node_id[s] = site_sliver.node_id
            site_props = abqm.node_sliver_to_graph_properties_dict(site_sliver)
            abqm.add_node(node_id=site_sliver.node_id, label=ABCPropertyGraph.CLASS_CompositeNode,
                          props=site_props)
            # add a network service
            ns_id = str(uuid.uuid4())
            site_to_ns_node_id[s] = ns_id
            ns_props = {ABCPropertyGraph.PROP_NAME: s + '_ns',
                        ABCPropertyGraph.PROP_TYPE: str(ServiceType.MPLS)}
            abqm.add_node(node_id=ns_id, label=ABCPropertyGraph.CLASS_NetworkService, props=ns_props)
            abqm.add_link(node_a=site_sliver.node_id, rel=ABCPropertyGraph.REL_HAS, node_b=ns_id)

            # create a component sliver for every component type/model pairing
            # and add a node for it linking back to site node
            for ctype, cdict in site_comps_by_type.items():
                for cmodel, comp_list in cdict.items():
                    comp_sliver = ComponentSliver()
                    # count what is available
                    comp_sliver.capacities = Capacities()
                    # count what is taken (ignore those type/model pairings that were unused)
                    comp_sliver.capacity_allocations = site_allocated_comps_caps_by_type[ctype].get(cmodel) or \
                                                       Capacities()
                    comp_sliver.set_type(ctype)
                    comp_sliver.set_model(cmodel)
                    comp_sliver.set_name(str(ctype) + '-' + cmodel)
                    for comp in comp_list:
                        comp_sliver.capacities = comp_sliver.capacities + comp.capacities
                    comp_node_id = str(uuid.uuid4())
                    comp_props = abqm.component_sliver_to_graph_properties_dict(comp_sliver)
                    abqm.add_node(node_id=comp_node_id, label=ABCPropertyGraph.CLASS_Component,
                                  props=comp_props)
                    abqm.add_link(node_a=site_sliver.node_id, rel=ABCPropertyGraph.REL_HAS,
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
            # add connection point, link, connection point between two NetworkServices
            assert(site_to_ns_node_id.get(source_site) is not None and
                   site_to_ns_node_id.get(sink_site) is not None)
            source_cp_id = str(uuid.uuid4())
            sink_cp_id = str(uuid.uuid4())
            source_cp_props = {ABCPropertyGraph.PROP_NAME: "_".join([source_site, sink_site]),
                               ABCPropertyGraph.PROP_TYPE: str(InterfaceType.TrunkPort),
                               ABCPropertyGraph.PROP_CLASS: ABCPropertyGraph.CLASS_ConnectionPoint,
                               ABCPropertyGraph.PROP_LABELS: cbm_source_cp_props.get(ABCPropertyGraph.PROP_LABELS),
                               ABCPropertyGraph.PROP_CAPACITIES: cbm_source_cp_props.get(ABCPropertyGraph.PROP_CAPACITIES)
                               }
            source_cp_props = {k: v for (k, v) in source_cp_props.items() if v}

            abqm.add_node(node_id=source_cp_id, label=ABCPropertyGraph.CLASS_ConnectionPoint,
                          props=source_cp_props)
            # FIXME: CP names may not be unique if we are dealing with a multigraph
            sink_cp_props = {ABCPropertyGraph.PROP_NAME: "_".join([sink_site, source_site]),
                             ABCPropertyGraph.PROP_TYPE: str(InterfaceType.TrunkPort),
                             ABCPropertyGraph.PROP_CLASS: ABCPropertyGraph.CLASS_ConnectionPoint,
                             ABCPropertyGraph.PROP_LABELS: cbm_sink_cp_props.get(ABCPropertyGraph.PROP_LABELS),
                             ABCPropertyGraph.PROP_CAPACITIES: cbm_sink_cp_props.get(ABCPropertyGraph.PROP_CAPACITIES)
                             }
            sink_cp_props = {k: v for (k, v) in sink_cp_props.items() if v}
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
            abqm.add_link(node_a=site_to_ns_node_id[source_site], rel=ABCPropertyGraph.REL_CONNECTS,
                          node_b=source_cp_id)
            abqm.add_link(node_a=source_cp_id, rel=ABCPropertyGraph.REL_CONNECTS,
                          node_b=link)
            abqm.add_link(node_a=link, rel=ABCPropertyGraph.REL_CONNECTS,
                          node_b=sink_cp_id)
            abqm.add_link(node_a=sink_cp_id, rel=ABCPropertyGraph.REL_CONNECTS,
                          node_b=site_to_ns_node_id[sink_site])

        # link facilities to their sites
        for s, lf in facilities_by_site.items():
            # multiple facilities per site possible
            for fac_sliver in lf:
                fac_nbs = cbm.get_first_and_second_neighbor(node_id=fac_sliver.node_id,
                                                            rel1=ABCPropertyGraph.REL_HAS,
                                                            node1_label=ABCPropertyGraph.CLASS_NetworkService,
                                                            rel2=ABCPropertyGraph.REL_CONNECTS,
                                                            node2_label=ABCPropertyGraph.CLASS_ConnectionPoint)
                try:
                    fac_ns_node_id = fac_nbs[0][0]
                    fac_cp_node_id = fac_nbs[0][1]
                except KeyError:
                    if self.logger:
                        self.logger.warning(f'Unable to trace facility ConnectionPoint for '
                                            f'facility {fac_sliver.resource_name}, continuing')
                    else:
                        print(f'Unable to trace facility ConnectionPoint for '
                              f'facility {fac_sliver.resource_name}, continuing')
                    continue
                _, fac_props = cbm.get_node_properties(node_id=fac_sliver.node_id)
                _, fac_ns_props = cbm.get_node_properties(node_id=fac_ns_node_id)
                _, fac_cp_props = cbm.get_node_properties(node_id=fac_cp_node_id)

                # filter down only the needed properties then recreate the structure of facility in ABQM
                new_fac_props = {ABCPropertyGraph.PROP_NAME: fac_props[ABCPropertyGraph.PROP_NAME],
                                 ABCPropertyGraph.PROP_TYPE: fac_props[ABCPropertyGraph.PROP_TYPE]
                                 }
                abqm.add_node(node_id=fac_sliver.node_id, label=ABCPropertyGraph.CLASS_NetworkNode,
                              props=new_fac_props)
                new_ns_props = {ABCPropertyGraph.PROP_NAME: fac_ns_props[ABCPropertyGraph.PROP_NAME],
                                ABCPropertyGraph.PROP_TYPE: fac_ns_props[ABCPropertyGraph.PROP_TYPE]
                                }
                abqm.add_node(node_id=fac_ns_node_id, label=ABCPropertyGraph.CLASS_NetworkService,
                              props=new_ns_props)
                new_cp_props = {ABCPropertyGraph.PROP_NAME: fac_cp_props[ABCPropertyGraph.PROP_NAME],
                                ABCPropertyGraph.PROP_TYPE: fac_cp_props[ABCPropertyGraph.PROP_TYPE],
                                ABCPropertyGraph.PROP_LABELS: fac_cp_props.get(ABCPropertyGraph.PROP_LABELS),
                                ABCPropertyGraph.PROP_CAPACITIES: fac_cp_props.get(ABCPropertyGraph.PROP_CAPACITIES)
                                }
                new_cp_props = {k: v for (k, v) in new_cp_props.items() if v}
                abqm.add_node(node_id=fac_cp_node_id, label=ABCPropertyGraph.CLASS_ConnectionPoint,
                              props=new_cp_props)
                abqm.add_link(node_a=fac_sliver.node_id, rel=ABCPropertyGraph.REL_HAS, node_b=fac_ns_node_id)
                abqm.add_link(node_a=fac_ns_node_id, rel=ABCPropertyGraph.REL_CONNECTS, node_b=fac_cp_node_id)

                # trace the link to a switch port/ConnectionPoint and replicate them for simplicity
                fac_cp_nbs = cbm.get_first_and_second_neighbor(node_id=fac_cp_node_id,
                                                               rel1=ABCPropertyGraph.REL_CONNECTS,
                                                               node1_label=ABCPropertyGraph.CLASS_Link,
                                                               rel2=ABCPropertyGraph.REL_CONNECTS,
                                                               node2_label=ABCPropertyGraph.CLASS_ConnectionPoint)
                if len(fac_cp_nbs) == 0 or len(fac_cp_nbs) > 1:
                    if self.logger:
                        self.logger.warning(f'Unable to trace switch port from Facility port '
                                            f'for facility {fac_sliver.resource_name} {fac_cp_nbs}')
                    else:
                        print(f'Unable to trace switch port from Facility port '
                              f'for facility {fac_sliver.resource_name} {fac_cp_nbs}')
                    continue

                fac_link_id = fac_cp_nbs[0][0]
                fac_sp_id = fac_cp_nbs[0][1]

                _, fac_link_props = cbm.get_node_properties(node_id=fac_link_id)
                # selectively replicate link properties
                new_link_props = {ABCPropertyGraph.PROP_NAME: fac_link_props[ABCPropertyGraph.PROP_NAME],
                                  ABCPropertyGraph.PROP_TYPE: fac_link_props[ABCPropertyGraph.PROP_TYPE],
                                  ABCPropertyGraph.PROP_LAYER: fac_link_props[ABCPropertyGraph.PROP_LAYER]
                                  }
                abqm.add_node(node_id=fac_link_id, label=ABCPropertyGraph.CLASS_Link,
                              props=new_link_props)
                try:
                    abqm.get_node_properties(node_id=fac_sp_id)
                except PropertyGraphQueryException:
                    # if the node doesn't exist we need to create it (it could have been created in the first pass)
                    _, fac_sp_props = cbm.get_node_properties(node_id=fac_sp_id)
                    new_sp_props = {ABCPropertyGraph.PROP_NAME: fac_sp_props[ABCPropertyGraph.PROP_NAME],
                                    ABCPropertyGraph.PROP_TYPE: fac_sp_props[ABCPropertyGraph.PROP_TYPE],
                                    ABCPropertyGraph.PROP_CAPACITIES: fac_sp_props.get(
                                        ABCPropertyGraph.PROP_CAPACITIES),
                                    ABCPropertyGraph.PROP_LABELS: fac_sp_props.get(ABCPropertyGraph.PROP_LABELS)
                                    }
                    new_sp_props = {k: v for (k, v) in new_sp_props.items() if v}
                    abqm.add_node(node_id=fac_sp_id, label=ABCPropertyGraph.CLASS_ConnectionPoint,
                                  props=new_sp_props)

                # link these together
                abqm.add_link(node_a=fac_cp_node_id, rel=ABCPropertyGraph.REL_CONNECTS, node_b=fac_link_id)
                abqm.add_link(node_a=fac_link_id, rel=ABCPropertyGraph.REL_CONNECTS, node_b=fac_sp_id)
                abqm.add_link(node_a=fac_sp_id, rel=ABCPropertyGraph.REL_CONNECTS, node_b=site_to_ns_node_id[s])

        return abqm
