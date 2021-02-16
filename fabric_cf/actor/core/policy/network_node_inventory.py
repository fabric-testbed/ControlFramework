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
from typing import Tuple, List

from fabric_cf.actor.core.apis.i_actor import IActor
from fabric_cf.actor.core.apis.i_reservation import IReservation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import BrokerException
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType


class NetworkNodeInventory(InventoryForType):
    def allocate(self, *, needed: int, request: dict, actor: IActor, capacities: dict = None,
                 labels: dict = None, reservation_info: List[IReservation] = None,
                 resource: dict = None) -> Tuple[str, dict]:

        if capacities is None or len(capacities) < 1:
            raise BrokerException(Constants.INVALID_ARGUMENT)

        properties = None
        delegation_id = None

        requested_core = int(request.get(Constants.SLIVER_PROPERTY_CORE))
        requested_ram = int(request.get(Constants.SLIVER_PROPERTY_RAM))
        requested_disk = int(request.get(Constants.SLIVER_PROPERTY_DISK))

        # For each delegation; check if it satisfies the incoming request
        for capacity_delegation_id, capacity_delegation_value_list in capacities.items():
            available_core = 0
            available_ram = 0
            available_disk = 0

            # Accumulate all the Available Capacities
            for capacity_delegation in capacity_delegation_value_list:
                available_core += capacity_delegation.get(Constants.SLIVER_PROPERTY_CORE, 0)
                available_ram += capacity_delegation.get(Constants.SLIVER_PROPERTY_RAM, 0)
                available_disk += capacity_delegation.get(Constants.SLIVER_PROPERTY_DISK, 0)

            # Remove allocated capacities to the reservations
            if reservation_info is not None:
                for reservation in reservation_info:
                    # For Active or Ticketed reservations; reduce the counts from available
                    if reservation.is_active() or reservation.is_ticketed():
                        resource_properties = reservation.get_resources().get_resource_properties()
                        available_core -= int(resource_properties.get(Constants.SLIVER_PROPERTY_CORE))
                        available_ram -= int(resource_properties.get(Constants.SLIVER_PROPERTY_RAM))
                        available_disk -= int(resource_properties.get(Constants.SLIVER_PROPERTY_DISK))

            # Compare the requested against available
            if requested_core > available_core or requested_ram > available_ram or requested_disk > available_disk:
                raise BrokerException(f"Insufficient resources "
                                      f"Cores: [{requested_core}/{available_core}] "
                                      f"RAM: [{requested_ram}/{available_ram}] "
                                      f"Disk: [{requested_disk}/{available_disk}]")

            properties = {Constants.SLIVER_PROPERTY_CORE: str(requested_core),
                          Constants.SLIVER_PROPERTY_RAM: str(requested_ram),
                          Constants.SLIVER_PROPERTY_DISK: str(requested_disk),
                          Constants.SLIVER_PROPERTY_GRAPH_NODE_ID:
                              request.get(Constants.SLIVER_PROPERTY_GRAPH_NODE_ID)}
            return capacity_delegation_id, properties
        return delegation_id, properties

    def allocate_revisit(self, *, count: int, resource: dict):
        return

    def free(self, *, count: int, request: dict = None, resource: dict = None) -> dict:
        return
