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
from __future__ import annotations
from typing import TYPE_CHECKING

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import PolicyException
from fabric_cf.actor.core.common.resource_pool_attribute_descriptor import ResourcePoolAttributeDescriptor, \
    ResourcePoolAttributeType
from fabric_cf.actor.core.common.resource_pool_descriptor import ResourcePoolDescriptor
from fabric_cf.actor.core.policy.simpler_units_inventory import SimplerUnitsInventory
from fabric_cf.actor.core.util.prop_list import PropList
from fabric_cf.actor.core.util.reflection_utils import ReflectionUtils
from fabric_cf.actor.core.util.resource_type import ResourceType

if TYPE_CHECKING:
    from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
    from fabric_cf.actor.core.apis.i_client_reservation import IClientReservation


class Inventory:
    def __init__(self):
        self.map = {}

    def contains_type(self, *, resource_type: ResourceType):
        if resource_type is None:
            raise PolicyException(Constants.invalid_argument)

        if resource_type in self.map:
            return True

        return False

    def get(self, *, resource_type: ResourceType) -> InventoryForType:
        if resource_type is None:
            raise PolicyException(Constants.invalid_argument)

        return self.map.get(resource_type, None)

    def remove(self, *, source: IClientReservation):
        """
        Removes the inventory derived from the specified source.
        @param source source reservation
        @return true if the inventory was update, false otherwise
        """
        rtype = source.get_type()

        if rtype in self.map:
            inv = self.map[rtype]
            if inv.source == source:
                self.map.pop(rtype)
                return True

        return False

    def get_new(self, *, reservation: IClientReservation):
        if reservation is None:
            raise PolicyException(Constants.invalid_argument)

        rtype = reservation.get_type()

        if rtype in self.map:
            raise PolicyException("There is already inventory for type: {}".format(rtype))

        properties = {}

        rset = reservation.get_resources()
        cset = rset.get_resources()
        ticket = cset.get_ticket()

        properties = ticket.get_properties()
        properties = PropList.merge_properties(incoming=rset.get_resource_properties(), outgoing=properties)
        rpd = ResourcePoolDescriptor()
        rpd.reset(properties=properties)

        desc_attr = rpd.get_attribute(key=Constants.resource_class_inventory_for_type)
        inv = None
        if desc_attr is not None:
            module_name, class_name = desc_attr.get_value().rsplit(".", 1)
            inv = ReflectionUtils.create_instance(module_name=module_name, class_name=class_name)
        else:
            inv = SimplerUnitsInventory()

        inv.set_type(rtype=rtype)
        inv.set_descriptor(rpd=rpd)
        inv.donate(source=reservation)

        self.map[rtype] = inv
        return inv

    def get_inventory(self) -> dict:
        return self.map

    def get_resource_pools(self) -> dict:
        result = {}
        count = 0
        for inv in self.map.values():
            rpd = inv.get_descriptor().clone()
            attr = ResourcePoolAttributeDescriptor()
            attr.set_type(rtype=ResourcePoolAttributeType.INTEGER)
            attr.set_key(value=Constants.resource_available_units)
            attr.set_value(value=str(inv.get_free()))
            rpd.add_attribute(attribute=attr)
            result = rpd.save(properties=result, prefix=Constants.pool_prefix + str(count) + ".")
            count += 1

        result[Constants.pools_count] = str(len(self.map))
        return result
