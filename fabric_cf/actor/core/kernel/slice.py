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
import threading
from datetime import datetime
from enum import Enum
from typing import Dict, Tuple

from fim.graph.abc_property_graph import ABCPropertyGraph
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import ReservationInfo

from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_slice import ABCSlice
from fabric_cf.actor.core.apis.abc_kernel_reservation import ABCKernelReservation
from fabric_cf.actor.core.apis.abc_kernel_slice import ABCKernelSlice
from fabric_cf.actor.core.common.exceptions import SliceException
from fabric_cf.actor.core.kernel.slice_state_machine import SliceStateMachine, SliceState, SliceOperation
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.fim.fim_helper import FimHelper
from fabric_cf.actor.security.auth_token import AuthToken


class SliceTypes(Enum):
    InventorySlice = 1
    ClientSlice = 2
    BrokerClientSlice = 3


class Slice(ABCKernelSlice):
    """
    Slice implementation. A slice has a globally unique identifier, name,
    description, property list, an owning identity, an access control list, and a
    set of reservations.
    This class is used within the Service Manager, which may hold reservations on
    many sites; on the Broker, which may have provided tickets to the slice for
    reservations at many sites; and on the site Authority, where each slice may
    hold multiple reservations for resources at that site.
    """
    def __init__(self, *, slice_id: ID = None, name: str = "unspecified"):
        # Globally unique identifier.
        self.guid = slice_id
        # Slice name. Not required to be globally or locally unique.
        self.name = name
        # Description string. Has only local meaning.
        self.description = "no description"
        # The slice type: inventory or client.
        self.type = SliceTypes.ClientSlice
        # The owner of the slice.
        self.owner = None
        # Resource type associated with this slice. Used when the slice is used to
        # represent an inventory pool.
        self.resource_type = None
        # The reservations in this slice.
        self.reservations = ReservationSet()
        self.delegations = {}
        # Neo4jGraph Id
        self.graph_id = None
        self.graph = None
        self.state_machine = SliceStateMachine(slice_id=slice_id)
        self.dirty = False
        self.config_properties = None
        self.lock = threading.Lock()
        self.lease_end = None
        self.lease_start = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['reservations']
        del state['delegations']
        del state['graph']
        del state['lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.reservations = ReservationSet()
        self.graph = None
        self.delegations = {}
        self.lock = threading.Lock()

    def set_graph_id(self, graph_id: str):
        self.graph_id = graph_id

    def get_graph_id(self) -> str:
        return self.graph_id

    def set_graph(self, *, graph: ABCPropertyGraph):
        self.graph = graph
        self.set_graph_id(graph_id=self.graph.get_graph_id())

    def get_graph(self) -> ABCPropertyGraph:
        return self.graph

    def clone_request(self) -> ABCSlice:
        result = Slice()
        result.name = self.name
        result.guid = self.guid
        return result

    def get_lease_end(self) -> datetime:
        return self.lease_end

    def get_lease_start(self) -> datetime:
        return self.lease_start

    def get_description(self) -> str:
        return self.description

    def get_name(self) -> str:
        return self.name

    def get_owner(self) -> AuthToken:
        return self.owner

    def get_reservations(self) -> ReservationSet:
        return self.reservations

    def get_delegations(self) -> Dict[str, ABCDelegation]:
        return self.delegations

    def get_reservations_list(self) -> list:
        return self.reservations.values()

    def get_resource_type(self) -> ResourceType:
        return self.resource_type

    def get_slice_id(self) -> ID:
        return self.guid

    def is_broker_client(self) -> bool:
        return self.type == SliceTypes.BrokerClientSlice

    def is_client(self) -> bool:
        return not self.is_inventory()

    def is_inventory(self) -> bool:
        return self.type == SliceTypes.InventorySlice

    def is_empty(self) -> bool:
        return self.reservations.is_empty()

    def prepare(self):
        self.reservations.clear()
        self.delegations.clear()
        self.state_machine.clear()
        self.transition_slice(operation=SliceStateMachine.CREATE)

    def register(self, *, reservation: ABCKernelReservation):
        if self.reservations.contains(rid=reservation.get_reservation_id()):
            raise SliceException("Reservation #{} already exists in slice".format(reservation.get_reservation_id()))

        self.reservations.add(reservation=reservation)

    def register_delegation(self, *, delegation: ABCDelegation):
        if delegation.get_delegation_id() in self.delegations:
            raise SliceException("Delegation #{} already exists in slice".format(delegation.get_delegation_id()))

        self.delegations[delegation.get_delegation_id()] = delegation

    def set_lease_end(self, *, lease_end: datetime):
        self.lease_end = lease_end

    def set_lease_start(self, *, lease_start: datetime):
        self.lease_start = lease_start

    def set_broker_client(self):
        self.type = SliceTypes.BrokerClientSlice

    def set_client(self):
        self.type = SliceTypes.ClientSlice

    def set_description(self, *, description: str):
        self.description = description

    def set_inventory(self, *, value: bool):
        if value:
            self.type = SliceTypes.InventorySlice
        else:
            self.type = SliceTypes.ClientSlice

    def get_slice_type(self) -> SliceTypes:
        return self.type

    def set_name(self, *, name: str):
        self.name = name

    def set_owner(self, *, owner: AuthToken):
        self.owner = owner

    def set_resource_type(self, *, resource_type: ResourceType):
        self.resource_type = resource_type

    def soft_lookup(self, *, rid: ID) -> ABCKernelReservation:
        return self.reservations.get(rid=rid)

    def soft_lookup_delegation(self, *, did: str) -> ABCDelegation:
        return self.delegations.get(did, None)

    def __str__(self):
        msg = "{}({})".format(self.name, str(self.guid))
        if self.graph_id is not None:
            msg += " Graph Id:{}".format(self.graph_id)
        if self.owner is not None:
            msg += " Owner:{}".format(self.owner)
        if self.state_machine is not None:
            msg += " State:{}".format(self.state_machine.get_state())
        return msg

    def unregister(self, *, reservation: ABCKernelReservation):
        self.reservations.remove(reservation=reservation)

    def unregister_delegation(self, *, delegation: ABCDelegation):
        if delegation.get_delegation_id() in self.delegations:
            self.delegations.pop(delegation.get_delegation_id())

    def get_state(self) -> SliceState:
        return self.state_machine.get_state()

    def set_dirty(self):
        self.dirty = True

    def clear_dirty(self):
        self.dirty = False

    def is_dirty(self) -> bool:
        return self.dirty

    def transition_slice(self, *, operation: SliceOperation) -> Tuple[bool, SliceState]:
        return self.state_machine.transition_slice(operation=operation, reservations=self.reservations)

    def is_stable_ok(self) -> bool:
        state_changed, slice_state = self.transition_slice(operation=SliceStateMachine.REEVALUATE)

        if slice_state == SliceState.StableOk:
            return True

        return False

    def is_stable_error(self) -> bool:
        state_changed, slice_state = self.transition_slice(operation=SliceStateMachine.REEVALUATE)

        if slice_state == SliceState.StableError:
            return True

        return False

    def is_stable(self) -> bool:
        state_changed, slice_state = self.transition_slice(operation=SliceStateMachine.REEVALUATE)

        if slice_state == SliceState.StableError or slice_state == SliceState.StableOk:
            return True

        return False

    def is_dead_or_closing(self) -> bool:
        state_changed, slice_state = self.transition_slice(operation=SliceStateMachine.REEVALUATE)

        if slice_state == SliceState.Dead or slice_state == SliceState.Closing:
            return True

        return False

    def is_dead(self) -> bool:
        state_changed, slice_state = self.transition_slice(operation=SliceStateMachine.REEVALUATE)

        if slice_state == SliceState.Dead:
            return True

        return False

    def set_config_properties(self, *, value: dict):
        self.config_properties = value

    def get_config_properties(self) -> dict:
        return self.config_properties

    def update_slice_graph(self, sliver: BaseSliver, rid: str, reservation_state: str) -> BaseSliver:
        try:
            self.lock.acquire()
            # Update for Orchestrator for Active / Ticketed Reservations
            if sliver is not None and self.graph_id is not None:
                if sliver.reservation_info is None:
                    sliver.reservation_info = ReservationInfo()
                sliver.reservation_info.reservation_id = rid
                sliver.reservation_info.reservation_state = reservation_state
                FimHelper.update_node(graph_id=self.graph_id, sliver=sliver)
            return sliver
        finally:
            self.lock.release()


class SliceFactory:
    @staticmethod
    def create(*, slice_id: ID, name: str = None, properties: dict = None) -> ABCSlice:
        """
        Create slice
        :param slice_id:
        :param name:
        :param properties:
        :return:
        """
        result = Slice(slice_id=slice_id, name=name)
        result.set_config_properties(value=properties)
        return result