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

from datetime import datetime

from abc import abstractmethod, ABC
from typing import TYPE_CHECKING

from fim.slivers.base_sliver import BaseSliver

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_authority_proxy import ABCAuthorityProxy
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.time.term import Term
    from fabric_cf.actor.core.util.notice import Notice
    from fabric_cf.actor.core.util.resource_type import ResourceType
    from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin


class ABCConcreteSet(ABC):
    """
    IConcreteSet defines the interface for concrete set resources. A
    concrete set is intended to represent a set of resources. For example,
    compute servers, storage servers, network paths, etc.
    Note: each concrete set type should implement a default constructor.

    Concrete sets are single threaded: their methods are invoked by the actor
    main thread and there is no need for internal synchronization. When a concrete set
    method needs to trigger a configuration action, the configuration action should
    be executed on a separate thread. Once the configuration action completes, an event
    should be queued to the actor to be processed on the actor main thread.
    """
    @abstractmethod
    def add(self, *, concrete_set: ABCConcreteSet, configure: bool):
        """
        Adds the passed set to the current set. Optionally triggers configuration
        actions on all added units.
        @param concrete_set : set to add
        @param configure :if true, configuration actions will be triggered for all
                   added units
        @raises Exception in case of error
        """

    @abstractmethod
    def change(self, *, concrete_set: ABCConcreteSet, configure: bool):
        """
        Makes changes to the resources in the concrete set. The incoming concrete
        set represents the state that the current set has to be updated to. The
        implementation must determine what units have been added/removed/modified
        and perform the appropriate actions.
        @param concrete_set : concrete resources representing the new state of the current set
        @param configure : if true, configuration actions will be triggered for all added, removed, or modified units
        @raises Exception thrown if something is wrong
        """

    @abstractmethod
    def close(self):
        """
        Initiates close operations on all resources contained in the set.
        """

    @abstractmethod
    def collect_released(self):
        """
        Collects any released (closed) and/or failed resources.
        @returns a concrete set containing released and or/failed resources
        @raises Exception in case of error
        """

    @abstractmethod
    def get_notices(self) -> Notice:
        """
        Gets a a collection of notices or events pertaining to the underlying
        resources. The event notices are consumed: subsequent calls return only
        new information. May return null.
        """

    @abstractmethod
    def get_site_proxy(self) -> ABCAuthorityProxy:
        """
        Return a proxy or reference for the unique site that owns these
        resources.
        @returns the authority that owns the resources
        @raises Exception in case of error
        """

    @abstractmethod
    def holding(self, *, when: datetime) -> int:
        """
        Returns how many units are in the set at the given time instance.
        @param when: time instance
        @returns how many units will be in the set for at the given time instance.
        """

    @abstractmethod
    def is_active(self) -> bool:
        """
        Checks if the concrete set is active. A concrete set is active if all
        units contained in the set are active.
        @returns true if the set is active
        """

    @abstractmethod
    def modify(self, *, sliver: BaseSliver):
        """
        Updates the units in the current set with information contained in the
        passed sliver.
        @param sliver:  Updated Sliver
        @raises Exception in case of error
        """

    @abstractmethod
    def probe(self):
        """
        Checks the status of pending operations.
        @raises Exception in case of error
        """

    @abstractmethod
    def remove(self, *, concrete_set: ABCConcreteSet, configure: bool):
        """
        Removes the passed set from the current set. Optionally triggers
        configuration actions for all removed units. If the lease term for the
        concrete set has changed, this call must be followed by a call to
        extend(Term).
        @param concrete_set : set to remove
        @param configure : if true, configuration actions will be triggered for all
                      removed units
        @raises Exception in case of error
        """

    @abstractmethod
    def setup(self, *, reservation: ABCReservationMixin):
        """
        Initializes the concrete set with information about the containing
        reservation. This method is called with the manager lock on and hence
        should not block for long periods of time.
        @param reservation : reservation this concrete set belongs to
        """

    @abstractmethod
    def validate_concrete(self, *, rtype: ResourceType, units: int, term: Term):
        """
        Validate that the concrete set matches the abstract resource set
        parameters.
        @param rtype : abstract resources resource type
        @param units : abstract resources units
        @param term : abstract resources term
        @raises Exception in case of error
        """

    @abstractmethod
    def validate_incoming(self):
        """
        Validates a concrete set as it is received by an actor from another
        actor. This method should examine the contents of the concrete set and
        determine whether it is well-formed. Well-formed is an implementation
        specific notion. For example, a ticket is well-formed if all claims
        the ticket is composed of nest properly. This method is also the place to
        perform any additional verification required to ascertain that the
        resources represented by the concrete set are valid. For example, a Sharp
        ticket is valid if it does not result in oversubscription.

        This method is called from ResourceSet with no locks on.

        @raises Exception if validation fails
        """

    @abstractmethod
    def validate_outgoing(self):
        """
        Validates a concrete set as it is about to be sent from the actor to
        another actor. This method should examine the contents of the concrete
        set and determine whether it is well-formed. Any other validation
        required before sending the concrete set should go in this function.

        This method is called from ResourceSet with no locks on.

        This method is called from ResourceSet with no locks on.

        @raises Exception if validation fails
        """

    @abstractmethod
    def restart_actions(self):
        """
        This method will be called during recovery to ensure that all pending
        actions are restarted. If a unit has an outstanding action that has not
        completed yet, that action would have to be restarted during this call.
        @raises Exception in case of error
        """

    @abstractmethod
    def clone_empty(self):
        """
        Makes an "empty" clone of this concrete set. An "empty" clone is a copy
        of a concrete set with the "set" removed from it.
        @returns an "empty" clone of this concrete set
        """

    @abstractmethod
    def clone(self):
        """
        Makes a clone of the concrete set. Unlike clone_empty(), this
        method preserves the set: the set elements are the same objects as the
        original IConcreteSet, but the indexing structures are different. That
        is, adding/removing units to the original should not affect the clone.
        But modifications to an individual unit should be visible form the
        original and the clone.
        @returns a clone of the concrete set
        """

    @abstractmethod
    def get_units(self) -> int:
        """
        Return the number of units
        @return number of units
        """
        return 0

    @abstractmethod
    def restore(self, *, plugin: ABCBasePlugin, reservation: ABCReservationMixin):
        """
        Restore the Concrete set
        @param plugin plugin
        @param reservation reservation
        """
