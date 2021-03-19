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

from abc import abstractmethod, ABC
from typing import TYPE_CHECKING, List

from fim.slivers.base_sliver import BaseSliver


if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.util.resource_type import ResourceType


class ABCResourceControl(ABC):
    """
    Interface for authority policy resource control implementations. An authority
    policy organizes the authorities inventory into ARM - Aggregate Resource Model.
    """
    @abstractmethod
    def eject(self, *, resource_set: ResourceSet):
        """
        Forcefully removes the specified resources from the control's inventory.
        @params resource_set set of resources to eject from the inventory (inventory
                   units)
        @raises Exception in case of error
        """

    @abstractmethod
    def release(self, *, resource_set: ResourceSet):
        """
        Releases previously allocated resources. Note that some of those
        resources may represent failed resource units. Failed units should not be
        released. Once the failure is corrected (by an entity external to the
        control), the control will be notified through freed
        @params resource_set set of released resources (allocated units)
        @raises Exception in case of error
        """

    @abstractmethod
    def available(self, *, resource_set: ResourceSet):
        """
        Notifies the control that inventory resources are now available for use.
        @params resource_set inventory resources that have become available (inventory units)
        @raises Exception in case of error
        """

    @abstractmethod
    def unavailable(self, *, resource_set: ResourceSet) -> int:
        """
        Indicates that some inventory resources should be marked as unavailable.
        @params resource_set resources (inventory units)
        @returns -1 if at least one resource unit is currently in use; 0 otherwise
        @raises Exception in case of error
        """

    @abstractmethod
    def freed(self, *, resource_set: ResourceSet):
        """
        Indicates that previously committed resources have been freed. Most
        likely these resources represent failed allocations.
        @params resource_set set of freed resources (allocated units)
        @raises Exception in case of error
        """

    @abstractmethod
    def failed(self, *, resource_set: ResourceSet):
        """
        Notifies the control that some inventory resources have failed and cannot
        be used to satisfy client requests.
        @params resource_set set of failed resources (inventory units)
        @raises Exception in case of error
        """

    @abstractmethod
    def recovered(self, *, resource_set: ResourceSet):
        """
        Notifies the policy that previously failed resources have been recovered
        and can be safely used to satisfy client requests.
        @params resource_set set of recovered resources
        @raises Exception in case of error
        """

    @abstractmethod
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

    @abstractmethod
    def correct_deficit(self, *, reservation: ABCAuthorityReservation) -> ResourceSet:
        """
        Informs the control that a reservation with previously allocated
        resources has a deficit. The deficit may be positive or negative. The
        control must decide how to handle the deficit. For example, the control
        may try allocating new resources.

        This method may be invoked multiple times. The core will invoke either
        until the deficit is corrected or the control indicates that the
        reservation must be sent back to the client even though it has a deficit.
        @params reservation reservation with a deficit
        @returns resources allocated/removed so that the deficit is corrected (allocated units)
        @raises Exception in case of error
        """

    @abstractmethod
    def close(self, *, reservation: ABCReservationMixin):
        """
        Notifies the control that a reservation is about to be closed.
        This method will be invoked for every reservation that is about to be
        closed, even if the close was triggered by the control itself. The
        policy should update its internal state/cancel pending operations
        associated with the reservation. This method is invoked with the kernel
        lock on.
        @params reservation: reservation to close
        """

    @abstractmethod
    def recovery_starting(self):
        """
        Notifies the control that recovery is starting.
        """

    @abstractmethod
    def revisit(self, *, reservation: ABCReservationMixin):
        """
        Recovers state for a reservation.
        @params reservation reservation
        @raises Exception in case of error
        """

    @abstractmethod
    def recovery_ended(self):
        """
        Notifies the control that recovery has ended.
        """

    @abstractmethod
    def configuration_complete(self, *, action: str, token: ConfigToken, out_properties: dict):
        """
        Notifies the control that a configuration action for the object
        represented by the token parameter has completed.

        @params action configuration action. See Config.Target*
        @params token object or a token for the object whose configuration action has completed
        @params out_properties output properties produced by the configuration action
        """

    @abstractmethod
    def get_guid(self) -> ID:
        """
        Returns the control guid. Every control must have a unique guid.
        @return resource control guid
        """

    @abstractmethod
    def get_types(self) -> set:
        """
        Returns the resource types this resource control handles. A resource
        control can be responsible for more than one resource type, although
        usually the mapping will be one-to-one.
        @returns list of resource types
        """

    @abstractmethod
    def add_type(self, *, rtype: ResourceType):
        """
        Instructs the control the handle the specified type.
        @param rtype type
        """

    @abstractmethod
    def remove_type(self, *, rtype: ResourceType):
        """
        Instructs the control it no longer handle the specified type.
        @param rtype type
        """

    @abstractmethod
    def register_type(self, *, rtype: ResourceType):
        """
        Registers the specified resource type with the resource control.
        @param rtype type
        """

    @abstractmethod
    def set_actor(self, *, actor: ABCActorMixin):
        """
        Sets the actor the control is associated with.
        @params actor actor
        """
