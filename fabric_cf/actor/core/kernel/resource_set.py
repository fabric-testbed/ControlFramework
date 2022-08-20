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
from datetime import datetime

from fim.slivers.base_sliver import BaseSliver

from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ResourcesException
from fabric_cf.actor.core.util.utils import sliver_to_str

if TYPE_CHECKING:
    from fabric_cf.actor.core.time.term import Term
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.util.notice import Notice
    from fabric_cf.actor.core.apis.abc_concrete_set import ABCConcreteSet
    from fabric_cf.actor.core.util.resource_type import ResourceType


class ResourceSet:
    """
    ResourceSet is an abstract set of resources describing some number of
    resource units of a given type, e.g., to represent a resource request. A
    ResourceSet with an attached ConcreteSet is "concrete." The ConcreteSet binds
    real resources (or a promise) for some or all of the abstract units in the
    set: for example, a concrete ResourceSet can represent a ticket or
    lease.
    """
    def __init__(self, *, concrete: ABCConcreteSet = None, gained: ABCConcreteSet = None,
                 lost: ABCConcreteSet = None, modified: ABCConcreteSet = None,
                 rtype: ResourceType = None, sliver: BaseSliver = None, units: int = None):
        # What type of resources does this set contain. The meaning/assignment of
        # type values is an externally defined convention of interacting actors.
        self.type = rtype
        # How many units (abstract) the set contains. This count reflects the
        # resources intended or requested for this set. For an active reservation
        # in steady state, the unit count will typically match the number of
        # concrete resources, but it might not match if the resource set is in flux
        # for some reason. For an inventory set, the abstract count reflects the
        # original size of the inventory, independent of any allocations extracted
        # from it.
        self.units = 0
        # Concrete resources.
        self.resources = concrete
        if concrete is not None:
            self.units = concrete.get_units()
        else:
            self.units = units
        # Sliver
        self.sliver = sliver
        # The previous value of the sliver. This is essential for
        # supporting recovery on Authority.
        self.previous_sliver = None
        # A set of resources recently ejected from the resource set, pending
        # processing, e.g., by a "probe" method.
        self.released = None
        # A recent update to the concrete resource set, pending processing by a
        # "service" method. Client-side only (i.e., for ticket or lease updates
        # through callback interface).
        self.updated = None
        # Recent additions of resources to the concrete set, pending processing by
        # a "service" method. Only for authority role only.
        self.gained = gained
        # Recently lost resources pending processing by a "service" method. Only
        # for authority role only.
        self.lost = lost
        # Recently changed resources pending a processing by a "service" method.
        # set.
        self.modified = modified
        # Reservation with which this set is associated.
        self.rid = None
        self.is_closing = False

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['released']
        del state['updated']
        del state['gained']
        del state['lost']
        del state['modified']
        del state['rid']
        del state['is_closing']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.released = None
        self.updated = None
        self.gained = None
        self.lost = None
        self.modified = None
        self.rid = None
        self.is_closing = False

    def restore(self, *, plugin: ABCBasePlugin, reservation: ABCReservationMixin):
        """
        Restore post stateful restart
        @param plugin plugin
        @param reservation reservation
        """
        if reservation is not None:
            self.rid = reservation.get_reservation_id()

        if self.resources is not None:
            self.resources.restore(plugin=plugin, reservation=reservation)

    def abstract_clone(self):
        """
        Clones the set, but without any of the concrete sets. Used on Authority
        and Service Manager to create a ResourceSet to hold resources, given a
        ResourceSet holding a ticket.
        @return a resources set that is a copy of the current but without any concrete sets.
        """
        result = ResourceSet(units=self.units, rtype=self.type, sliver=self.sliver)
        return result

    def collect_released(self) -> ResourceSet:
        """
        Any units that fail or are rejected/released asynchronously accumulate
        within the ConcreteSet until collected. These are cached by a
        prepareProbe.
        @returns a ResourceSet
        @raises Exception in case of error
        """
        result = None
        if self.released is not None:
            result = ResourceSet(concrete=self.released, rtype=self.type)
            self.released = None
        return result

    def merge_properties(self, *, reservation: ABCReservationMixin, resource_set):
        """
        Merge properties
        @param reservation reservation
        @param resource_set resource set
        """
        if reservation is None or resource_set is None:
            raise ResourcesException(Constants.INVALID_ARGUMENT)

        self.sliver = resource_set.get_sliver()

    def delta_update(self, *, reservation: ABCReservationMixin, resource_set):
        if reservation is None or resource_set is None:
            raise ResourcesException(Constants.INVALID_ARGUMENT)

        if self.resources is None:
            # in case of close for a canceled reservation.
            if resource_set.gained is None:
                return
            # first time we give concrete resources to this resource set
            # Since this set has no concrete resources, we can only gain
            # resources. Lost and modified have no meaning in this case. Assert just in case
            if resource_set.lost is not None or resource_set.modified is not None:
                return
            # take the units and type
            self.units = resource_set.gained.get_units()
            self.type = resource_set.type
            self.resources = resource_set.gained.clone_empty()
            self.resources.setup(reservation=reservation)
            self.merge_properties(reservation=reservation, resource_set=resource_set)
            self.gained = resource_set.gained
        else:
            self.type = resource_set.type
            difference = 0

            if resource_set.lost is not None:
                self.lost = resource_set.lost
                difference -= resource_set.lost.get_units()

            if resource_set.modified is not None:
                self.modified = resource_set.modified

            self.units += difference
            self.previous_sliver = self.sliver
            self.merge_properties(reservation=reservation, resource_set=resource_set)

    def fix_abstract_units(self):
        """
        Sets the number of abstract units to equal the number of concrete units.
        """
        if self.resources is None:
            self.units = self.resources.get_units()
        else:
            self.units = 0

    def full_update(self, *, reservation: ABCReservationMixin, resource_set):
        if reservation is None or resource_set is None:
            raise ResourcesException(Constants.INVALID_ARGUMENT)

        # take the units and the type
        self.units = resource_set.units
        self.type = resource_set.type
        # take in the sliver
        self.previous_sliver = self.sliver
        self.merge_properties(reservation=reservation, resource_set=resource_set)

        # make a concrete set if the current concrete set is None
        if self.resources is None:
            self.resources = resource_set.resources.clone_empty()
            self.resources.setup(reservation=reservation)
        # remember the update so that it can be processed later
        self.updated = resource_set.resources

    def get_concrete_units(self, *, when: datetime = None) -> int:
        """
        Estimate the concrete resource units the resource set will contain at the
        specified date.
        @params when: the date
        @returns number of concrete units
        """
        if self.resources is None:
            return 0
        elif when is None:
            return self.resources.get_units()
        else:
            return self.resources.holding(when=when)

    def get_deficit(self) -> int:
        """
        Returns the number of concrete units needed or in excess in this resource
        set.
        @returns number of units in excess or needed
        """
        result = self.units
        if self.resources is not None:
            result -= self.resources.get_units()

        return result

    def get_notices(self) -> Notice:
        """
        Returns a string of notices or events pertaining to the underlying
        resources. The event notices are consumed: subsequent calls return only
        new information. May return null.
        @returns Notice
        """
        if self.resources is None:
            return None
        return self.resources.get_notices()

    def get_sliver(self) -> BaseSliver:
        """
        Return sliver
        @param sliver
        """
        return self.sliver

    def get_reservation_id(self) -> ID:
        """
        Returns the reservation identifier attached to this resource set.
        @returns reservation identifier
        """
        return self.rid

    def get_resources(self) -> ABCConcreteSet:
        """
        Returns the concrete resources.
        @returns concrete resource set
        """
        return self.resources

    def get_site_proxy(self):
        """
        Returns a proxy to the site authority, which owns the resources
        represented in the set.
        @returns site authority proxy.
        @raises Exception in case of error
        """
        if self.resources is not None:
            return self.resources.get_site_proxy()
        return None

    def get_type(self) -> ResourceType:
        """
        Returns the resource type of the set.
        @returns resource type
        """
        return self.type

    def get_units(self) -> int:
        """
        Returns the number of abstract units in the set.
        @returns number of abstract units
        """
        return self.units

    def is_active(self) -> bool:
        """
        Checks if the resource set is active: allocated units are active.
        @returns true if this ResourceSet is active
        """
        if self.resources is not None:
            return self.resources.is_active()
        return False

    def is_closed(self) -> bool:
        """
        Checks if the resource set is closed: there are no active units. Do not
        call this method unless the set had a close in progress: a set with
        failed units or one that has not yet been activated may register as
        "closed".
        @returns true if this ResourceSet is active
        """
        if self.resources is None:
            return True
        if self.resources.get_units() == 0:
            return True
        return False

    def is_empty(self) -> bool:
        if self.updated is not None and self.updated.get_units() > 0:
            return False
        if self.gained is not None and self.gained.get_units() > 0:
            return False
        if self.lost is not None and self.lost.get_units() > 0:
            return False
        if self.modified is not None and self.modified.get_units() > 0:
            return False
        return True

    def prepare_probe(self):
        """
        Prepares a probe: updates ConcreteSet to reflect underlying resource
        status.
        @raises Exception in case of error
        """
        if self.resources is not None:
            self.resources.probe()
            if self.released is None:
                self.released = self.resources.collect_released()

    def probe(self):
        return

    def service_check(self):
        if self.resources is None:
            raise ResourcesException("Internal Error: WARNING: service post-op call on non-concrete reservation")

    def close(self):
        """
        Initiate close on the concrete resources
        """
        if not self.is_closing:
            self.is_closing = True
            self.resources.close()

    def service_extend(self):
        """
        Complete service for a term extension (server side).
        """
        self.service_check()
        # An elastic reservation can change concrete resources on extend. The
        # modifications are left in update/gained/lost by *Change() above. On
        # agent the concrete is updated synchronously in SoftChange, so this
        # code segment applies to authority only.
        my_gained = self.gained
        self.gained = None
        # An elastic reservation can change concrete resources on extend. The
        # modifications are left in update/gained/lost by *Change() above. On
        # agent the concrete is updated synchronously in SoftChange, so this
        # code segment applies to authority only.
        my_lost = self.lost
        self.lost = None
        # An elastic reservation can change concrete resources on extend. The
        # modifications are left in update/gained/lost by *Change() above. On
        # agent the concrete is updated synchronously in SoftChange, so this
        # code segment applies to authority only.
        my_modified = self.modified
        self.modified = None
        if my_gained is not None:
            self.resources.add(concrete_set=my_gained, configure=True)
        if my_lost is not None:
            self.resources.remove(concrete_set=my_lost, configure=True)
        if my_modified is not None:
            self.resources.modify(sliver=self.sliver)

    def service_modify(self):
        """
        Complete service for a term extension (server side).
        @raises Exception in case of error
        """
        self.service_check()
        self.resources.modify(sliver=self.sliver)

    def service_reserve_site(self):
        cs = None
        if self.gained is not None:
            cs = self.gained
            self.gained = None

        if cs is not None:
            self.resources.add(concrete_set=cs, configure=True)

    def service_update(self, *, reservation: ABCReservationMixin):
        """
        Service a resource set update. Any changes to existing
        concrete resources should have been left in "updated" by an update
        operation.
        @params reservation: reservation
        @raises Exception in case of error
        """
        cs = None
        if self.updated is not None:
            cs = self.updated
            self.updated = None
        if cs is not None:
            self.resources.change(concrete_set=cs, configure=True)

    def set_sliver(self, *, sliver: BaseSliver):
        """
        Sets the request sliver.
        @params sliver : request sliver
        """
        self.sliver = sliver

    def set_reservation_id(self, *, rid: ID):
        """
        Attaches the reservation identifier to the set.
        @params rid reservation identifier
        """
        self.rid = rid

    def set_resources(self, *, cset: ABCConcreteSet):
        """
        Set the concrete resources. Used by proxies.
        @params cset :concrete resource set
        """
        self.resources = cset

    def set_type(self, *, rtype: ResourceType):
        """
        Sets the resource type for the set.
        @params rtype : resource type
        """
        self.type = rtype

    def set_units(self, *, units: int):
        """
        Sets the number of abstract units in the set.
        @params units: number of abstract units
        """
        self.units = units

    def setup(self, *, reservation: ABCReservationMixin):
        """
        Passes information about the containing reservation to the concrete set.
        @params reservation: containing reservation
        """
        if self.resources is not None:
            self.resources.setup(reservation=reservation)

    def __str__(self):
        result = f"rset: units=[{self.units}] "
        if self.resources is not None:
            result += f" concrete:[{self.resources}]"
        return result

    def update(self, *, reservation: ABCReservationMixin, resource_set: ResourceSet):
        if reservation is None or resource_set is None:
            raise ResourcesException(Constants.INVALID_ARGUMENT)

        if resource_set.resources is not None:
            self.full_update(reservation=reservation, resource_set=resource_set)
        else:
            self.delta_update(reservation=reservation, resource_set=resource_set)

    def update_properties(self, *, reservation: ABCReservationMixin, resource_set):
        if reservation is None or resource_set is None:
            raise ResourcesException(Constants.INVALID_ARGUMENT)

        self.merge_properties(reservation=reservation, resource_set=resource_set)

    def validate(self):
        """
        Validates a fresh ResourceSet passed in from outside
        @raises Exception in case of error thrown if the set is determined to be invalid
        """
        if self.units < 0:
            raise ResourcesException("invalid unit count:{}".format(self.units))

    def validate_incoming(self):
        """
        Validates a ResourceSet in an incoming ticket or lease
        request (server) or in an incoming ticket or lease update (client).
        Called for each incoming request/update to check validity with no locks
        held.
        @raises Exception in case of error
        """
        self.validate()
        if self.resources is not None:
            self.resources.validate_incoming()

    def validate_incoming_ticket(self, *, term: Term):
        """
        Validate match between abstract and concrete ResourceSet in a ResourceSet
        representing an incoming ticket.
        @params t : optional term associated with ResourceSet
        @raises Exception in case of error if validation fails
        """
        if self.resources is None:
            if self.units != 0:
                raise ResourcesException("no resources to back incoming ticket")
            return
        if self.resources.get_units() != self.units:
            raise ResourcesException("size mismatch on incoming ticket {} != {}".format(self.resources.get_units(),
                                                                                        self.units))
        self.resources.validate_concrete(rtype=self.type, units=self.units, term=term)

    def validate_outgoing(self):
        """
        Validates a ResourceSet that is about to be sent to another
        actor. Client-side only.
        @raises Exception in case of error
        """
        self.validate()
        if self.resources is not None:
            self.resources.validate_outgoing()

    def clone(self):
        clone = ResourceSet(units=self.units, rtype=self.type, sliver=self.sliver)
        clone.resources = self.resources.clone()
        return clone
