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

from fim.slivers.attached_components import AttachedComponentsInfo
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.network_node import NodeSliver
from fim.user import Capacities, ComponentType

from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import AuthorityException
from fabric_cf.actor.core.core.unit import Unit
from fabric_cf.actor.core.core.unit_set import UnitSet
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.policy.resource_control import ResourceControl
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.fim.fim_helper import FimHelper


class NetworkNodeControl(ResourceControl):
    """
    Resource Control for Network Node
    """
    def __check_capacities(self, *, rid: ID, requested_capacities: Capacities, available_capacities: Capacities,
                           existing_reservations: List[ABCReservationMixin]):
        """
        Check if the allocated capacities by the broker can be assigned
        :param rid: reservation id
        :param requested_capacities: Requested capacities
        :param available_capacities: Available capacities
        :param existing_reservations: Existing Reservations served by the same ARM node
        :raises: AuthorityException in case the request cannot be satisfied
        """
        self.logger.debug(f"available_capacities: {available_capacities}")
        self.logger.debug(f"requested_capacities: {requested_capacities} for reservation# {rid}")

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
                    self.logger.debug(f"Excluding already allocated resources "
                                      f"{resource_sliver.get_capacity_allocations()} to "
                                      f"reservation# {reservation.get_reservation_id()}")
                    # Compare the requested against available
                    available_capacities = available_capacities - resource_sliver.get_capacity_allocations()

        # Compare the requested against available
        available_capacities = available_capacities - requested_capacities
        negative_fields = available_capacities.negative_fields()
        if len(negative_fields) > 0:
            raise AuthorityException(f"Insufficient resources: {negative_fields}")

    def __check_components(self, *, rid: ID, requested_components: AttachedComponentsInfo, graph_node: BaseSliver,
                           existing_reservations: List[ABCReservationMixin]):
        """
        Check if the allocated components by the broker can be assigned
        :param rid: reservation id
        :param requested_components: Requested components
        :param graph_node: ARM Graph Node serving the reservation
        :param existing_reservations: Existing Reservations served by the same ARM node
        :raises: AuthorityException in case the request cannot be satisfied
        """
        self.logger.debug(f"requested_components: {requested_components} for reservation# {rid}")
        for name, c in requested_components.devices.items():
            node_map = c.get_node_map()
            if node_map is None:
                raise AuthorityException(f"Component of type: {c.get_type()} "
                                         f"does not have allocated BQM Node Id")

            resource_type = c.get_type()
            available_components = graph_node.attached_components_info.get_devices_by_type(resource_type=resource_type)
            self.logger.debug(f"Resource Type: {resource_type} available_components: {available_components}")

            if available_components is None or len(available_components) == 0:
                raise AuthorityException(f"Insufficient resources Component of type: {resource_type} not available "
                                         f"in graph node: {graph_node.node_id}")

            confirm_component = False

            for av in available_components:
                if node_map[1] == av.node_id:
                    confirm_component = True
                    break

            if not confirm_component:
                raise AuthorityException(f"Graph node: {graph_node.node_id} has no component: {node_map}")

            for reservation in existing_reservations:
                if reservation.get_reservation_id() == rid:
                    continue
                # For Active or Ticketed or Ticketing reservations; reduce the counts from available
                allocated_sliver = None
                if reservation.is_redeeming() and reservation.get_approved_resources() is not None:
                    allocated_sliver = reservation.get_approved_resources().get_sliver()

                if reservation.is_active() and reservation.get_resources() is not None:
                    allocated_sliver = reservation.get_resources().get_sliver()

                if allocated_sliver is not None and allocated_sliver.attached_components_info is not None:
                    allocated_components = allocated_sliver.attached_components_info.get_devices_by_type(
                        resource_type=resource_type)

                    self.logger.debug(f"Already allocated components {allocated_components} of resource_type "
                                      f"{resource_type} to reservation# {reservation.get_reservation_id()}")

                    for ac in allocated_components:
                        ac_node_map = ac.get_node_map()
                        if ac_node_map[1] == node_map[1]:
                            # For Shared NIC, make sure PCI devices are not same
                            # For other components, having same node map is an error
                            if (ac.get_type() == ComponentType.SharedNIC and
                                ac.get_label_allocations().bdf == c.get_label_allocations()) or \
                                    ac.get_type() != ComponentType.SharedNIC:
                                raise AuthorityException(
                                    f"Component of type: {resource_type} BQM Node Id: {node_map[1]} "
                                    f"in graph {graph_node.node_id}"
                                    f"is already assigned to reservation# {reservation}")

    def assign(self, *, reservation: ABCAuthorityReservation, delegation_name: str,
               graph_node: BaseSliver, existing_reservations: List[ABCReservationMixin]) -> ResourceSet:
        """
        Assign a reservation
        :param reservation: reservation
        :param delegation_name: Name of delegation serving the request
        :param graph_node: ARM Graph Node serving the reservation
        :param existing_reservations: Existing Reservations served by the same ARM node
        :return: ResourceSet with updated sliver annotated with properties
        :raises: AuthorityException in case the request cannot be satisfied
        """

        if graph_node.capacity_delegations is None or reservation is None:
            raise AuthorityException(Constants.INVALID_ARGUMENT)

        delegated_capacities = graph_node.get_capacity_delegations()
        available_delegated_capacity = FimHelper.get_delegation(delegated_capacities=delegated_capacities,
                                                                delegation_name=delegation_name)
        if available_delegated_capacity is None:
            raise AuthorityException(f"Allocated node {graph_node.node_id} does not have delegation: {delegation_name}")

        reservation.set_send_with_deficit(value=True)

        requested = reservation.get_requested_resources().get_sliver()
        if not isinstance(requested, NodeSliver):
            raise AuthorityException(f"Invalid resource type {requested.get_type()}")

        current = reservation.get_resources()

        resource_type = ResourceType(resource_type=str(requested.get_type()))

        gained = None
        lost = None
        if current is None:
            # Check if Capacities can be satisfied by Delegated Capacities
            self.__check_capacities(rid=reservation.get_reservation_id(),
                                    requested_capacities=requested.get_capacity_allocations(),
                                    available_capacities=available_delegated_capacity,
                                    existing_reservations=existing_reservations)

            # Check if Capacities can be satisfied by Capacities
            self.__check_capacities(rid=reservation.get_reservation_id(),
                                    requested_capacities=requested.get_capacity_allocations(),
                                    available_capacities=graph_node.get_capacities(),
                                    existing_reservations=existing_reservations)

            # Check components
            # Check if Components can be allocated
            if requested.attached_components_info is not None:
                self.__check_components(rid=reservation.get_reservation_id(),
                                        requested_components=requested.attached_components_info,
                                        graph_node=graph_node,
                                        existing_reservations=existing_reservations)

            self.logger.debug(f"Slice properties: {reservation.get_slice().get_config_properties()}")
            unit = Unit(rid=reservation.get_reservation_id(), slice_id=reservation.get_slice_id(),
                        actor_id=self.authority.get_guid(), sliver=requested, rtype=resource_type,
                        properties=reservation.get_slice().get_config_properties())
            gained = UnitSet(plugin=self.authority.get_plugin(), units={unit.reservation_id: unit})
        else:
            # FIX ME: handle modify
            self.logger.info(f"Extend Lease for now, no modify supported res# {reservation}")
            return current

        result = ResourceSet(gained=gained, lost=lost, rtype=resource_type)
        result.set_sliver(sliver=requested)
        return result

    def revisit(self, *, reservation: ABCReservationMixin):
        return

    def free(self, *, uset: dict):
        if uset is not None:
            for u in uset.values():
                self.logger.debug(f"Freeing 1 unit {u}")