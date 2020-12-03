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

from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.common.exceptions import PolicyException
from fabric.actor.core.core.unit import Unit, UnitState
from fabric.actor.core.core.unit_set import UnitSet
from fabric.actor.core.policy.free_allocated_set import FreeAllocatedSet
from fabric.actor.core.policy.resource_control import ResourceControl
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.kernel.resource_set import ResourceSet

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_client_reservation import IClientReservation
    from fabric.actor.core.apis.i_reservation import IReservation


class VlanControl(ResourceControl):
    def __init__(self):
        super().__init__()
        self.tags = FreeAllocatedSet()
        self.rtype = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['authority']
        del state['logger']
        del state['initialized']

        del state['tags']
        del state['rtype']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

        self.authority = None
        self.logger = None
        self.initialized = False

        self.tags = FreeAllocatedSet()
        self.rtype = None

    def donate_reservation(self, *, reservation: IClientReservation):
        self.logger.debug("VlanControl.donate(): donating resouces r={}".format(reservation))

        if self.tags.size() != 0:
            raise PolicyException("only a single source reservation is supported")

        rset = reservation.get_resources()
        rtype = reservation.get_type()
        local = rset.get_local_properties()
        if local is None:
            raise PolicyException("Missing local properties")

        self.rtype = rtype
        size = 0

        if Constants.property_vlan_range_num in local:
            num_range = int(local[Constants.property_vlan_range_num])
            for i in range(num_range):
                start_p = Constants.property_start_vlan + str(i)
                end_p = Constants.property_end_vlan + str(i)
                if start_p in local and end_p in local:
                    start = int(local[start_p])
                    end = int(local[end_p])

                    if start == 0 and end == 0:
                        break

                    for j in range(start, end + 1):
                        self.tags.add_inventory(item=j)

                    size = size + (end - start + 1)
                    self.logger.info("VlanControl.donate(): Tag Donation:{}:{}-{}:{}".format(rtype, start, end, size))

        if size < reservation.get_units():
            raise PolicyException("Insufficient vlan tags specified in donated reservation: donated {} rset says: {}".format(
                size, reservation.get_units()))

    def donate(self, *, resource_set: ResourceSet):
        return

    def assign(self, *, reservation: IAuthorityReservation) -> ResourceSet:
        reservation.set_send_with_deficit(value=True)

        if self.tags.size() == 0:
            raise PolicyException("no inventory")

        requested = reservation.get_requested_resources()
        rtype = requested.get_type()
        current = reservation.get_resources()
        ticket = requested.get_resources()
        rt = ticket.get_ticket()
        ticket_properties = rt.get_properties()
        config_properties = requested.get_config_properties()

        if current is None:
            needed = ticket.get_units()
            if needed > 1:
                reservation.fail(message="Cannot assign more than 1 VLAN per reservation", exception=None)

            config_tag = config_properties.get(Constants.config_unit_tag, None)
            static_tag = None
            if config_tag is not None:
                static_tag = int(config_tag)

            if self.tags.get_free() > 0:
                tag = 0
                if static_tag is None:
                    tag = self.tags.allocate()
                else:
                    tag = self.tags.allocate(tag=static_tag, config_tag=True)

                rd = ResourceData()
                rd.resource_properties[Constants.unit_vlan_tag] = tag
                gained = UnitSet(plugin=self.authority.get_plugin())
                unit = Unit(id=ID())
                unit.set_resource_type(rtype=rtype)
                unit.set_property(name=Constants.unit_vlan_tag, value=str(tag))

                if Constants.resource_bandwidth in ticket_properties:
                    bw = int(ticket_properties[Constants.resource_bandwidth])
                    burst = bw/8
                    unit.set_property(name=Constants.unit_vlan_qo_s_rate, value=str(bw))
                    unit.set_property(name=Constants.unit_vlan_qo_s_burst_size, value=str(burst))
                gained.add_unit(u=unit)
                return ResourceSet(gained=gained, rtype=rtype, rdata=rd)
            else:
                return None
        else:
            return ResourceSet(rtype=rtype)

    def get_tag(self, *, u: Unit) -> int:
        tag = None
        if u.get_property(name=Constants.unit_vlan_tag) is not None:
            tag = int(u.get_property(name=Constants.unit_vlan_tag))
        return tag

    def free(self, *, uset: dict):
        if self is not None:
            for u in uset.values():
                try:
                    tag = self.get_tag(u=u)
                    if tag is None:
                        self.logger.error("VlanControl.free(): attempted to free a unit with a missing tag")
                    else:
                        self.logger.debug("VlanControl.free(): freeing tag: {}".format(tag))
                    self.tags.free(item=tag)
                except Exception as e:
                    self.logger.error("VlanControl.free(): Failed to release vlan tag {}".format(e))

    def revisit(self, *, reservation: IReservation):
        unit_set = reservation.get_resources().get_resources()
        for u in unit_set.get_set().values:
            try:
                if u.get_state() == UnitState.DEFAULT or \
                        u.get_state() == UnitState.FAILED or \
                        u.get_state() == UnitState.CLOSING or \
                        u.get_state() == UnitState.PRIMING or \
                        u.get_state() == UnitState.ACTIVE or \
                        u.get_state() == UnitState.MODIFYING:
                    tag = self.get_tag(u=u)
                    if tag is None:
                        self.logger.error("VlanControl.revisit(): Recoverying a unit without a tag")
                    else:
                        self.logger.debug("VlanControl.revisit(): reserving tag {} during recovery".format(tag))
                        self.tags.allocate(tag=tag, config_tag=True)

                elif u.get_state() == UnitState.CLOSED:
                    self.logger.debug("VlanControl.revisit(): node is closed. Nothing to recover")
            except Exception as e:
                self.fail(u=u, message="revisit with vmcontrol", e=e)

    def recovery_starting(self):
        self.logger.info("Beginning VlanControl recovery")

    def recovery_ended(self):
        self.logger.info("Completing VlanControl recovery")
        self.logger.debug("Restored VlanControl resource type {} with tags: {}".format(self.rtype, self.tags))
