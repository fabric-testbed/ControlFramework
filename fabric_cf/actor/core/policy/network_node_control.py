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
from typing import List, Dict

from fim.slivers.attached_components import AttachedComponentsInfo
from fim.slivers.network_node import NodeSliver
from fim.user import Capacities

from fabric_cf.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric_cf.actor.core.apis.i_reservation import IReservation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import AuthorityException
from fabric_cf.actor.core.core.unit import Unit
from fabric_cf.actor.core.core.unit_set import UnitSet
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.policy.resource_control import ResourceControl
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.neo4j.neo4j_graph_node import Neo4jGraphNode


class NetworkNodeControl(ResourceControl):
    """
    Resource Control for Network Node
    """
    def __check_capacities(self, *, rid: ID, requested_capacities: Capacities, available_capacities: Capacities,
                           existing_reservations: List[IReservation]):
        self.logger.debug(f"available_capacities: {available_capacities}")
        self.logger.debug(f"requested_capacities: {requested_capacities} for reservation# {rid}")
        available_core = available_capacities.core
        available_ram = available_capacities.ram
        available_disk = available_capacities.disk

        # Remove allocated capacities to the reservations
        if existing_reservations is not None:
            for reservation in existing_reservations:
                if reservation.get_reservation_id() == rid:
                    continue
                # For Active or Ticketed or Ticketing reservations; reduce the counts from available
                resource_sliver = None
                if reservation.is_redeeming() and reservation.get_approved_resources() is not None:
                    resource_sliver = reservation.get_approved_resources().get_sliver()

                if reservation.is_active() and reservation.get_resources() is not None:
                    resource_sliver = reservation.get_resources().get_sliver()

                if resource_sliver is not None:
                    self.logger.debug(f"Excluding already allocated resources {resource_sliver.get_capacities()} to "
                                      f"reservation# {reservation.get_reservation_id()}")
                    available_core -= resource_sliver.get_capacities().core
                    available_ram -= resource_sliver.get_capacities().ram
                    available_disk -= resource_sliver.get_capacities().disk

        # Compare the requested against available
        if requested_capacities.core > available_core or requested_capacities.ram > available_ram or \
                requested_capacities.disk > available_disk:
            raise AuthorityException(f"Insufficient resources "
                                  f"Cores: [{requested_capacities.core}/{available_core}] "
                                  f"RAM: [{requested_capacities.ram}/{available_ram}] "
                                  f"Disk: [{requested_capacities.disk}/{available_disk}]")

    def __check_components(self, *, rid: ID, requested_components: AttachedComponentsInfo, graph_node: Neo4jGraphNode,
                           existing_reservations: List[IReservation]) -> AttachedComponentsInfo:
        self.logger.debug(f"requested_components: {requested_components} for reservation# {rid}")
        for name, c in requested_components.devices.items():
            if c.cbm_node_id is None:
                raise AuthorityException(f"Component of type: {c.cbm_node_id} does not have allocated CBM Node Id")

            resource_type = str(c.get_resource_type())
            available_components = graph_node.get_components_by_type(resource_type=resource_type)
            self.logger.debug(f"Resource Type: {resource_type} available_components: {available_components}")

            if available_components is None or len(available_components) == 0:
                raise AuthorityException(f"Insufficient resources Component of type: {resource_type} not available "
                                         f"in graph node: {graph_node.get_node_id()}")

            if c.cbm_node_id not in available_components:
                raise AuthorityException(f"Component of type: {resource_type} is not available "
                                         f"in graph node: {graph_node.get_node_id()}")

            for reservation in existing_reservations:
                if reservation.get_reservation_id() == rid:
                    continue
                # For Active or Ticketed or Ticketing reservations; reduce the counts from available
                allocated_sliver = None
                if reservation.is_redeeming() and reservation.get_approved_resources() is not None:
                    allocated_sliver = reservation.get_approved_resources().get_sliver()

                if reservation.is_active() and reservation.get_resources() is not None:
                    allocated_sliver = reservation.get_resources().get_sliver()

                if allocated_sliver is not None:
                    allocated_components = allocated_sliver.attached_components_info.get_cbm_node_ids()
                    self.logger.debug(f"Already allocated components {allocated_components} of resource_type "
                                      f"{resource_type} to reservation# {reservation.get_reservation_id()}")
                    for ac in allocated_components:
                        if ac == c.cbm_node_id:
                            raise AuthorityException(
                                f"Component of type: {resource_type} CBM Node Id: {c.cbm_node_id} "
                                f"in graph {graph_node.get_node_id()}"
                                f"is already assigned to reservation# {reservation}")

            component_node = graph_node.get_component(node_id=c.cbm_node_id)
            c.labels.bdf = component_node.get_labels().bdf

        return requested_components

    def assign(self, *, reservation: IAuthorityReservation, delegation_name: str,
               graph_node: Neo4jGraphNode, reservation_info: List[IReservation]) -> ResourceSet:

        if graph_node.capacity_delegations is None or len(graph_node.capacity_delegations) < 1 or reservation is None:
            raise AuthorityException(Constants.INVALID_ARGUMENT)

        available_delegated_capacity = graph_node.get_capacity_delegations().get(delegation_name, None)
        if available_delegated_capacity is None:
            raise AuthorityException(f"Allocated node {graph_node.node_id} does not have delegation: {delegation_name}")

        reservation.set_send_with_deficit(value=True)

        requested = reservation.get_requested_resources().get_sliver()
        if not isinstance(requested, NodeSliver):
            raise AuthorityException(f"Invalid resource type {requested.get_resource_type()}")

        current = reservation.get_resources()

        resource_type = ResourceType(resource_type=str(requested.get_resource_type()))

        gained = None
        lost = None
        if current is None:
            # Check if Capacities can be satisfied by Delegated Capacities
            self.__check_capacities(rid=reservation.get_reservation_id(),
                                    requested_capacities=requested.get_capacities(),
                                    available_capacities=available_delegated_capacity,
                                    existing_reservations=reservation_info)

            # Check if Capacities can be satisfied by Capacities
            self.__check_capacities(rid=reservation.get_reservation_id(),
                                    requested_capacities=requested.get_capacities(),
                                    available_capacities=graph_node.get_capacities(),
                                    existing_reservations=reservation_info)

            # Check components
            # Check if Components can be allocated
            requested.attached_components_info = self.__check_components(
                rid=reservation.get_reservation_id(),
                requested_components=requested.attached_components_info,
                graph_node=graph_node,
                existing_reservations=reservation_info)

            unit = Unit(uid=ID(), rid=reservation.get_reservation_id(), slice_id=reservation.get_slice_id(),
                        actor_id=self.authority.get_guid(), sliver=requested, rtype=resource_type)
            gained = UnitSet(plugin=self.authority.get_plugin(), units={unit.uid: unit})
        else:
            raise AuthorityException("Modify not supported")

        result = ResourceSet(gained=gained, lost=lost, rtype=resource_type)
        result.set_sliver(sliver=requested)
        return result

    def revisit(self, *, reservation: IReservation):
        return

    def free(self, *, uset: dict):
        if uset is not None:
            for u in uset.values():
                self.logger.debug(f"Freeing 1 unit {u}")
