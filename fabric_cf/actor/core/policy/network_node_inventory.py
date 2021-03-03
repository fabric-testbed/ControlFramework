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
from typing import Tuple, List, Dict

from fim.slivers.attached_components import AttachedComponentsInfo
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Capacities
from fim.slivers.network_node import NodeSliver

from fabric_cf.actor.core.apis.i_actor import IActor
from fabric_cf.actor.core.apis.i_reservation import IReservation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import BrokerException
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.neo4j.neo4j_helper import Neo4jGraphNode


class NetworkNodeInventory(InventoryForType):
    def __init__(self):
        self.logger = None

    def set_logger(self, *, logger):
        self.logger = logger

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None

    def __check_capacities(self, *, rid: ID, requested_capacities: Capacities, capacity_dlg: Dict[str, Capacities],
                           existing_reservations: List[IReservation]) -> str:
        self.logger.debug(f"requested_capacities: {requested_capacities} for reservation# {rid}")
        result = None
        for dlg_id, capacities in capacity_dlg.items():
            self.logger.debug(f"available_capacity_delegations: {capacities} for delegation# {dlg_id}")
            available_core = capacities.core
            available_ram = capacities.ram
            available_disk = capacities.disk

            # Remove allocated capacities to the reservations
            if existing_reservations is not None:
                for reservation in existing_reservations:
                    if rid == reservation.get_reservation_id():
                        continue
                    # For Active or Ticketed or Ticketing reservations; reduce the counts from available
                    resource_sliver = None
                    if reservation.is_ticketing() and reservation.get_approved_resources() is not None:
                        resource_sliver = reservation.get_approved_resources().get_sliver()

                    if (reservation.is_active() or reservation.is_ticketed()) and \
                            reservation.get_resources() is not None:
                        resource_sliver = reservation.get_resources().get_sliver()

                    if resource_sliver is not None:
                        self.logger.debug(
                            f"Excluding already assigned resources {resource_sliver.get_capacities()} to "
                            f"reservation# {reservation.get_reservation_id()}")
                        available_core -= resource_sliver.get_capacities().core
                        available_ram -= resource_sliver.get_capacities().ram
                        available_disk -= resource_sliver.get_capacities().disk

            # Compare the requested against available
            if requested_capacities.core > available_core or requested_capacities.ram > available_ram or \
                    requested_capacities.disk > available_disk:
                raise BrokerException(f"Insufficient resources "
                                      f"Cores: [{requested_capacities.core}/{available_core}] "
                                      f"RAM: [{requested_capacities.ram}/{available_ram}] "
                                      f"Disk: [{requested_capacities.disk}/{available_disk}]")

            result = dlg_id
            break
        return result

    def __check_components(self, *, rid: ID, requested_components: AttachedComponentsInfo, graph_node: Neo4jGraphNode,
                           existing_reservations: List[IReservation]) -> AttachedComponentsInfo:

        self.logger.debug(f"requested_components: {requested_components} for reservation# {rid}")
        for name, c in requested_components.devices.items():
            resource_type = str(c.get_resource_type())
            available_components = graph_node.get_components_by_type(resource_type=resource_type)

            self.logger.debug(f"Resource Type: {resource_type} available_components: {available_components}")

            if available_components is None or len(available_components) == 0:
                raise BrokerException(f"Insufficient resources Component of type: {resource_type} not available in "
                                      f"graph node: {graph_node.get_node_id()}")

            for reservation in existing_reservations:
                if rid == reservation.get_reservation_id():
                    continue
                # For Active or Ticketed or Ticketing reservations; reduce the counts from available
                allocated_sliver = None
                if reservation.is_ticketing() and reservation.get_approved_resources() is not None:
                    allocated_sliver = reservation.get_approved_resources().get_sliver()

                if (reservation.is_active() or reservation.is_ticketed()) and reservation.get_resources() is not None:
                    allocated_sliver = reservation.get_resources().get_sliver()

                if allocated_sliver is not None:
                    allocated_components = allocated_sliver.attached_components_info.get_cbm_node_ids()
                    self.logger.debug(f"Already allocated components {allocated_components} of resource_type "
                                      f"{resource_type} to reservation# {reservation.get_reservation_id()}")
                    for ac in allocated_components:
                        if ac in available_components:
                            self.logger.debug(f"Excluding component {ac} assigned to "
                                              f"res# {reservation.get_reservation_id()}")
                            available_components.remove(ac)

            if available_components is None or len(available_components) == 0:
                raise BrokerException(f"Insufficient resources Component of type: {resource_type} not available in "
                                      f"graph node: {graph_node.get_node_id()}")

            for ac in available_components:
                component = graph_node.get_component(node_id=ac)
                # check model matches the requested model
                if c.get_resource_model() is not None and component.get_resource_model() == c.get_resource_model():
                    self.logger.debug(f"Found a matching component with resource model {c.get_resource_model()}")
                    self.logger.debug(f"Assigning {component.get_node_id()} to component# {c} in reservation# {rid} ")
                    c.cbm_node_id = component.get_node_id()
                    # Remove the component from available components as it is assigned
                    graph_node.components_by_type[resource_type].remove(ac)
                    break

                if c.get_resource_model() is None:
                    self.logger.debug(f"Found a matching component# {component.get_node_id()}")
                    self.logger.debug(f"Assigning {component.get_node_id()} to component# {c} in reservation# {rid} ")
                    c.cbm_node_id = component.get_node_id()
                    # Remove the component from available components as it is assigned
                    graph_node.components_by_type[resource_type].remove(ac)
                    break

            if c.cbm_node_id is None:
                raise BrokerException(f"Component of type: {resource_type} model: {c.get_resource_model()} "
                                      f"not available in graph node: {graph_node.get_node_id()}")

        return requested_components

    def allocate(self, *, reservation: IReservation, actor: IActor, graph_node: Neo4jGraphNode,
                 reservation_info: List[IReservation]) -> Tuple[str, BaseSliver]:
        if graph_node.capacity_delegations is None or len(graph_node.capacity_delegations) < 1 or reservation is None:
            raise BrokerException(Constants.INVALID_ARGUMENT)

        requested = reservation.get_requested_resources().get_sliver()
        if not isinstance(requested, NodeSliver):
            raise BrokerException(f"Invalid resource type {requested.get_resource_type()}")

        # Check if Capacities can be satisfied
        result = self.__check_capacities(rid=reservation.get_reservation_id(),
                                         requested_capacities=requested.get_capacities(),
                                         capacity_dlg=graph_node.get_capacity_delegations(),
                                         existing_reservations=reservation_info)

        # Check if Components can be allocated
        requested.attached_components_info = self.__check_components(
            rid=reservation.get_reservation_id(),
            requested_components=requested.attached_components_info,
            graph_node=graph_node,
            existing_reservations=reservation_info)

        return result, requested

    def allocate_revisit(self, *, count: int, resource: dict):
        return

    def free(self, *, count: int, request: dict = None, resource: dict = None) -> dict:
        return
