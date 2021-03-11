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
from typing import Tuple, List

from fim.slivers.attached_components import AttachedComponentsInfo, ComponentSliver
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Capacities, Labels
from fim.slivers.network_node import NodeSliver

from fabric_cf.actor.core.apis.i_reservation import IReservation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import BrokerException
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
from fabric_cf.actor.core.util.id import ID


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

    def __check_capacities(self, *, rid: ID, requested_capacities: Capacities, delegated_capacities: dict,
                           existing_reservations: List[IReservation]) -> str:
        """
        Check if the requested capacities can be satisfied with the available capacities
        :param rid: reservation id of the reservation being served
        :param requested_capacities: Requested Capacities
        :param delegated_capacities: Delegated Capacities
        :param existing_reservations: Existing Reservations served by the same BQM node
        :return: Delegation Id of the delegation which satisfies the request
        :raises: BrokerException in case the request cannot be satisfied
        """
        self.logger.debug(f"requested_capacities: {requested_capacities} for reservation# {rid}")

        for delegation_id, delegated_list in delegated_capacities.items():
            for delegated in delegated_list:
                self.logger.debug(f"available_capacity_delegations: {delegated} {type(delegated)}")
                delegated_capacity = Capacities().from_json(json.dumps(delegated))

                available_core = delegated_capacity.core
                available_ram = delegated_capacity.ram
                available_disk = delegated_capacity.disk

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

                return delegation_id
        return None

    def __check_component_labels_and_capacities(self, *, available_component: ComponentSliver,
                                                requested_component: ComponentSliver) -> ComponentSliver:
        """
        Check if available component capacities, labels to match requested component
        :param available_component: available component
        :param requested_component: requested component
        :return: requested component annotated with properties in case of success, None otherwise
        """
        if requested_component.get_resource_model() is not None and \
                requested_component.get_resource_model() != available_component.get_resource_model():
            return requested_component

        # Checking capacity for component
        capacity_delegations = available_component.get_capacity_delegations()
        for delegation_id, delegation_list in capacity_delegations.items():
            for delegated in delegation_list:
                self.logger.debug(f"available_capacity_delegations : {delegated} {type(delegated)} for component {available_component}")
                delegated_capacity = Capacities().from_json(json.dumps(delegated))
                if delegated_capacity.unit < 1:
                    raise BrokerException(f"Insufficient resources for component: {requested_component.get_name()}"
                                          f"Unit: [{1}/{delegated_capacity.unit}]")
                requested_component.capacities = delegated_capacity

        # Check labels
        label_delegations = available_component.get_label_delegations()
        for delegation_id, delegation_list in label_delegations.items():
            for delegated in delegation_list:
                self.logger.debug(f"available_label_delegations : {delegated} {type(delegated)} for component {available_component}")
                delegated_label = Labels().from_json(json.dumps(delegated))
                requested_component.labels = delegated_label

        requested_component.bqm_node_id = available_component.node_id

        return requested_component

    def __check_components(self, *, rid: ID, requested_components: AttachedComponentsInfo, graph_node: BaseSliver,
                           existing_reservations: List[IReservation]) -> AttachedComponentsInfo:
        """
        Check if the requested capacities can be satisfied with the available capacities
        :param rid: reservation id of the reservation being served
        :param requested_components: Requested components
        :param graph_node: BQM graph node identified to serve the reservation
        :param existing_reservations: Existing Reservations served by the same BQM node
        :return: Components updated with the corresponding BQM node ids
        :raises: BrokerException in case the request cannot be satisfied
        """

        self.logger.debug(f"requested_components: {requested_components} for reservation# {rid}")
        for name, requested_component in requested_components.devices.items():
            resource_type = requested_component.get_resource_type()
            available_components = graph_node.attached_components_info.get_devices_by_type(resource_type=resource_type)

            self.logger.debug(f"Resource Type: {resource_type} available_components: {available_components}")

            if available_components is None or len(available_components) == 0:
                raise BrokerException(f"Insufficient resources Component of type: {resource_type} not available in "
                                      f"graph node: {graph_node.node_id}")

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
                    for allocated in allocated_sliver.attached_components_info.devices.values():
                        if allocated.bqm_node_id is not None:
                            self.logger.debug(f"Already allocated components {allocated} of resource_type "
                                              f"{resource_type} to reservation# {reservation.get_reservation_id()}")

                            for av in available_components:
                                if av.node_id == allocated.bqm_node_id:
                                    self.logger.debug(f"Excluding component {allocated} assigned to "
                                                      f"res# {reservation.get_reservation_id()}")
                                    # Exclude already allocated component from available components
                                    graph_node.attached_components_info.remove_device(av.get_name())
                                    break

            available_components = graph_node.attached_components_info.get_devices_by_type(resource_type=resource_type)
            self.logger.debug(f"available_components after excluding allocated components: {available_components}")

            if available_components is None or len(available_components) == 0:
                raise BrokerException(f"Insufficient resources Component of type: {resource_type} not available in "
                                      f"graph node: {graph_node.node_id}")

            for component in available_components:
                # check model matches the requested model
                requested_component = self.__check_component_labels_and_capacities(available_component=component,
                                                                                   requested_component=requested_component)
                if requested_component.bqm_node_id is not None:
                    self.logger.debug(f"Found a matching component with resource model "
                                      f"{requested_component.get_resource_model()}")

                    self.logger.debug(f"Assigning {component.node_id} to component# "
                                      f"{requested_component} in reservation# {rid} ")

                    # Remove the component from available components as it is assigned
                    graph_node.attached_components_info.remove_device(component.get_name())
                    break

            if requested_component.bqm_node_id is None:
                raise BrokerException(f"Component of type: {resource_type} model: {requested_component.get_resource_model()} "
                                      f"not available in graph node: {graph_node.node_id}")

        return requested_components

    def allocate(self, *, reservation: IReservation, graph_node: BaseSliver,
                 existing_reservations: List[IReservation]) -> Tuple[str, BaseSliver]:
        """
        Allocate an extending or ticketing reservation
        :param reservation: reservation to be allocated
        :param graph_node: BQM graph node identified to serve the reservation
        :param existing_reservations: Existing Reservations served by the same BQM node
        :return: Tuple of Delegation Id and the Requested Sliver annotated with BQM Node Id and other properties
        :raises: BrokerException in case the request cannot be satisfied
        """
        if graph_node.capacity_delegations is None or len(graph_node.capacity_delegations) < 1 or reservation is None:
            raise BrokerException(Constants.INVALID_ARGUMENT)

        requested = reservation.get_requested_resources().get_sliver()
        if not isinstance(requested, NodeSliver):
            raise BrokerException(f"Invalid resource type {requested.get_resource_type()}")

        # Check if Capacities can be satisfied
        delegation_id = self.__check_capacities(rid=reservation.get_reservation_id(),
                                                requested_capacities=requested.get_capacities(),
                                                delegated_capacities=graph_node.get_capacity_delegations(),
                                                existing_reservations=existing_reservations)

        # Check if Components can be allocated
        requested.attached_components_info = self.__check_components(
            rid=reservation.get_reservation_id(),
            requested_components=requested.attached_components_info,
            graph_node=graph_node,
            existing_reservations=existing_reservations)

        return delegation_id, requested

    def free(self, *, count: int, request: dict = None, resource: dict = None) -> dict:
        return
