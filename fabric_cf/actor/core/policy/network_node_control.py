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

from fabric_cf.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric_cf.actor.core.apis.i_reservation import IReservation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import AuthorityException
from fabric_cf.actor.core.core.unit import Unit
from fabric_cf.actor.core.core.unit_set import UnitSet
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.policy.resource_control import ResourceControl
from fabric_cf.actor.core.util.id import ID


class NetworkNodeControl(ResourceControl):
    """
    Resource Control for Network Node
    """
    def assign(self, *, reservation: IAuthorityReservation, delegation_name: str,
               capacities: dict, capacity_del: list,
               labels: dict, label_del: list,
               reservation_info: List[IReservation]) -> ResourceSet:

        if capacity_del is None or capacities is None:
            raise AuthorityException(Constants.INVALID_ARGUMENT)

        # Number of Available Units would always 1 for a worker node, so it is not validated

        reservation.set_send_with_deficit(value=True)

        requested = reservation.get_requested_resources()
        rtype = requested.get_type()
        current = reservation.get_resources()
        ticket = requested.get_resources()

        delegated_capacity = self.get_delegated_capacity_or_label(delegation_name=delegation_name,
                                                                  capacity_or_label_delegations=capacity_del)

        if delegated_capacity is None:
            raise AuthorityException(f"No delegated resources available {delegation_name}")

        available_core = capacities.get(Constants.SLIVER_PROPERTY_CORE, 0)
        available_ram = capacities.get(Constants.SLIVER_PROPERTY_RAM, 0)
        available_disk = capacities.get(Constants.SLIVER_PROPERTY_DISK, 0)

        available_core_del = delegated_capacity.get(Constants.SLIVER_PROPERTY_CORE, 0)
        available_ram_del = delegated_capacity.get(Constants.SLIVER_PROPERTY_RAM, 0)
        available_disk_del = delegated_capacity.get(Constants.SLIVER_PROPERTY_DISK, 0)

        gained = None
        lost = None
        if current is None:
            config_properties = requested.get_config_properties()

            requested_core = int(config_properties.get(Constants.SLIVER_PROPERTY_CORE))
            requested_ram = int(config_properties.get(Constants.SLIVER_PROPERTY_RAM))
            requested_disk = int(config_properties.get(Constants.SLIVER_PROPERTY_DISK))

            # Remove allocated capacities to the reservations
            if reservation_info is not None:
                for reservation in reservation_info:
                    # For Active or Ticketed reservations; reduce the counts from available
                    if reservation.is_active() or reservation.is_ticketed():
                        resource_properties = reservation.get_resources().get_resource_properties()
                        available_core -= int(resource_properties.get(Constants.SLIVER_PROPERTY_CORE))
                        available_ram -= int(resource_properties.get(Constants.SLIVER_PROPERTY_RAM))
                        available_disk -= int(resource_properties.get(Constants.SLIVER_PROPERTY_DISK))

                        available_core_del -= int(resource_properties.get(Constants.SLIVER_PROPERTY_CORE))
                        available_ram_del -= int(resource_properties.get(Constants.SLIVER_PROPERTY_RAM))
                        available_disk_del -= int(resource_properties.get(Constants.SLIVER_PROPERTY_DISK))

            # Compare the requested against available delegated resources
            if requested_core > available_core_del or requested_ram > available_ram_del or \
                    requested_disk > available_disk_del:
                raise AuthorityException(f"Requested Resources more than the available delegated resources "
                                         f"Cores: [{requested_core}/{available_core_del}] "
                                         f"RAM: [{requested_ram}/{available_ram_del}] "
                                         f"Disk: [{requested_disk}/{available_disk_del}]")

            # Compare the requested against available
            if requested_core > available_core or requested_ram > available_ram or requested_disk > available_disk:
                raise AuthorityException(f"Requested Resources more than the available resources "
                                         f"Cores: [{requested_core}/{available_core}] "
                                         f"RAM: [{requested_ram}/{available_ram}] "
                                         f"Disk: [{requested_disk}/{available_disk}]")

            properties = {Constants.SLIVER_PROPERTY_CORE: str(requested_core),
                          Constants.SLIVER_PROPERTY_RAM: str(requested_ram),
                          Constants.SLIVER_PROPERTY_DISK: str(requested_disk),
                          Constants.SLIVER_PROPERTY_GRAPH_NODE_ID:
                              config_properties.get(Constants.SLIVER_PROPERTY_GRAPH_NODE_ID)}

            unit = Unit(uid=ID(), rid=reservation.get_reservation_id(), slice_id=reservation.get_slice_id(),
                        actor_id=self.authority.get_guid(), properties=properties, rtype=rtype)
            gained = UnitSet(plugin=self.authority.get_plugin(), units={unit.uid: unit})
        else:
            raise AuthorityException("Modify not supported")

        return ResourceSet(gained=gained, lost=lost, rtype=rtype)

    def revisit(self, *, reservation: IReservation):
        return

    def free(self, *, uset: dict):
        if uset is not None:
            for u in uset.values():
                self.logger.debug(f"Freeing 1 unit {u}")
