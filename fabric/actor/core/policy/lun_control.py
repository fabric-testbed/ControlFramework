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

from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.core.unit import Unit, UnitState
from fabric.actor.core.core.unit_set import UnitSet
from fabric.actor.core.policy.free_allocated_set import FreeAllocatedSet
from fabric.actor.core.policy.resource_control import ResourceControl
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.kernel.resource_set import ResourceSet

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_client_reservation import IClientReservation
    from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation


class LUNControl(ResourceControl):
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
        if self.tags.size() != 0:
            Exception("only a single source reservation is supported")

        rset = reservation.get_resources()
        rtype = reservation.get_type()
        local = rset.get_local_properties()

        if local is None:
            raise Exception("Missing local properties")

        self.rtype = rtype
        size = 0

        if Constants.PropertyLunRangeNum in local:
            num_range = int(local[Constants.PropertyLunRangeNum])
            for i in range(num_range):
                startP = Constants.PropertyStartLUN + str(i)
                endP = Constants.PropertyEndLUN + str(i)
                if startP in local and endP in local:
                    start = int(local[startP])
                    end = int(local[endP])
                    if start == 0 and end == 0:
                        break
                    for j in range(start, end):
                        self.tags.add_inventory(item=j)

                    size = size + (end - start + 1)
                    self.logger.info("Tag donation: {}:{}-{}:{}".format(self.rtype, start, end, size))

        if size < reservation.get_units():
            raise Exception("Insufficient lun tags specified in donated reservation: donated {} rset says: {}".format(size, reservation.get_units()))

    def assign(self, *, reservation: IAuthorityReservation) -> ResourceSet:
        reservation.set_send_with_deficit(value=True)

        if self.tags.size() == 0:
            raise Exception("no inventory")

        requested = reservation.get_requested_resources()
        rtype = reservation.get_type()
        current = reservation.get_resources()
        ticket = requested.get_resources()
        rt = ticket.get_ticket()
        ticket_properties = rt.get_properties()

        if current is None:
            needed = ticket.get_units()
            if needed > 1:
                reservation.fail(message="Cannot assign more than 1 LUN per reservation", exception=None)

            if self.tags.get_free() > 0:
                tag = self.tags.allocate()
                rd = ResourceData()
                rd.resource_properties[Constants.UnitLUNTag] = tag
                gained = UnitSet(plugin=self.authority.get_plugin())
                u = Unit(id=ID())
                u.set_resource_type(rtype=rtype)
                u.set_property(name=Constants.UnitLUNTag, value=str(tag))
                if Constants.ResourceStorageCapacity in ticket_properties:
                    capacity = int(ticket_properties[Constants.ResourceStorageCapacity])
                    u.set_property(name=Constants.UnitStorageCapacity, value=str(capacity))

                gained.add_unit(u=u)
                return ResourceSet(gained=gained, rtype=rtype, rdata=rd)
            else:
                return None
        else:
            return ResourceSet(rtype=rtype)

    def get_tag(self, *, u: Unit):
        tag = None
        if u.get_property(name=Constants.UnitLUNTag) is not None:
            tag = int(u.get_property(name=Constants.UnitLUNTag))
        return tag

    def free(self, *, uset: dict):
        if uset is not None:
            for u in uset:
                try:
                    tag = self.get_tag(u=u)
                    if tag is None:
                        self.logger.error("LUNControl.free(): attempted to free a unit with a missing tag")
                    else:
                        self.logger.debug("LUNControl.free(): freeing tag: {}".format(tag))
                    self.tags.free(item=tag)
                except Exception as e:
                    self.logger.error("Failed to release lun tag {}".format(e))

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
                        self.logger.error("LUNControl.revisit(): Recoverying a unit without a tag")
                    else:
                        self.logger.debug("LUNControl.revisit(): reserving tag {} during recovery".format(tag))
                        self.tags.allocate(tag=tag, config_tag=True)

                elif u.get_state() == UnitState.CLOSED:
                    self.logger.debug("LUNControl.revisit(): node is closed. Nothing to recover")
            except Exception as e:
                self.fail(u=u, message="revisit with vmcontrol", e=e)

    def recovery_starting(self):
        self.logger.info("Beginning LUNControl recovery")

    def recovery_ended(self):
        self.logger.info("Completing LUNControl recovery")
        self.logger.debug("Restored LUNControl resource type {} with tags: {}".format(self.rtype, self.tags))