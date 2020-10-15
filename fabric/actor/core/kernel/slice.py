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
from enum import Enum
from typing import Dict

from fabric.actor.core.apis.i_delegation import IDelegation
from fabric.actor.core.apis.i_slice import ISlice
from fabric.actor.core.apis.i_kernel_reservation import IKernelReservation
from fabric.actor.core.apis.i_kernel_slice import IKernelSlice
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.reservation_set import ReservationSet
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.util.resource_type import ResourceType
from fabric.actor.security.auth_token import AuthToken
from fabric.actor.security.guard import Guard
from fim.graph.abc_property_graph import ABCPropertyGraph


class SliceTypes(Enum):
    InventorySlice = 1
    ClientSlice = 2
    BrokerClientSlice = 3


class Slice(IKernelSlice):
    """
    Slice implementation. A slice has a globally unique identifier, name,
    description, property list, an owning identity, an access control list, and a
    set of reservations.
    This class is used within the Service Manager, which may hold reservations on
    many sites; on the Broker, which may have provided tickets to the slice for
    reservations at many sites; and on the site Authority, where each slice may
    hold multiple reservations for resources at that site.
    """
    def __init__(self, *, id: ID = None, name: str = "unspecified", data: ResourceData = None):
        # Globally unique identifier.
        self.guid = id
        # Slice name. Not required to be globally or locally unique.
        self.name = name
        # Description string. Has only local meaning.
        self.description = "no description"
        # A collection of property lists inherited by each reservation in this
        # slice. Properties defined on the reservation level override properties
        # defined here.
        self.rsrcdata = data
        # The slice type: inventory or client.
        self.type = SliceTypes.ClientSlice
        # The owner of the slice.
        self.owner = None
        # Access control monitor.
        self.guard = Guard()
        # Resource type associated with this slice. Used when the slice is used to
        # represent an inventory pool.
        self.resource_type = None
        # The reservations in this slice.
        self.reservations = ReservationSet()
        self.delegations = {}
        # Neo4jGraph Id
        self.graph_id = None
        self.graph = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['reservations']
        del state['delegations']
        del state['graph']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.reservations = ReservationSet()
        self.graph = None
        self.delegations = {}

    def set_graph_id(self, graph_id: ID):
        self.graph_id = graph_id

    def get_graph_id(self) -> ID:
        return self.graph_id

    def set_graph(self, graph: ABCPropertyGraph):
        self.graph = graph
        self.set_graph_id(graph_id=ID(id=self.graph.get_graph_id()))

    def get_graph(self) -> ABCPropertyGraph:
        return self.graph

    def clone_request(self) -> ISlice:
        result = Slice()
        result.slice_name = self.name
        result.guid = self.guid
        return result

    def get_config_properties(self):
        if self.rsrcdata is not None:
            return self.rsrcdata.get_configuration_properties()
        return None

    def get_description(self):
        return self.description

    def get_guard(self) -> Guard:
        return self.guard

    def get_local_properties(self):
        if self.rsrcdata is not None:
            return self.rsrcdata.get_local_properties()
        return None

    def get_name(self):
        return self.name

    def get_owner(self):
        return self.owner

    def get_properties(self) -> ResourceData:
        return self.rsrcdata

    def get_request_properties(self):
        if self.rsrcdata is not None:
            return self.rsrcdata.get_request_properties()
        return None

    def get_reservations(self) -> ReservationSet:
        return self.reservations

    def get_delegations(self) -> Dict[str, IDelegation] :
        return self.delegations

    def get_reservations_list(self) -> list:
        return self.reservations.values()

    def get_resource_properties(self):
        if self.rsrcdata is not None:
            return self.rsrcdata.get_resource_properties()
        return None

    def get_resource_type(self):
        return self.resource_type

    def get_slice_id(self) -> ID:
        return self.guid

    def is_broker_client(self):
        return self.type == SliceTypes.BrokerClientSlice

    def is_client(self):
        return not self.is_inventory()

    def is_inventory(self):
        return self.type == SliceTypes.InventorySlice

    def is_empty(self) -> bool:
        return self.reservations.is_empty()

    def prepare(self):
        self.reservations.clear()

    def register(self, *, reservation: IKernelReservation):
        if self.reservations.contains(rid=reservation.get_reservation_id()):
            raise Exception("Reservation #{} already exists in slice".format(reservation.get_reservation_id()))

        self.reservations.add(reservation=reservation)

    def register_delegation(self, *, delegation: IDelegation):
        if delegation.get_delegation_id() in self.delegations:
            raise Exception("Delegation #{} already exists in slice".format(delegation.get_delegation_id()))

        self.delegations[delegation.get_delegation_id()] = delegation

    def set_broker_client(self):
        self.type = SliceTypes.BrokerClientSlice

    def set_client(self):
        self.type = SliceTypes.ClientSlice

    def set_description(self, *, description: str):
        self.description = description

    def set_guard(self, *, g: Guard):
        self.guard = g

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
        self.guard.set_owner(owner=owner)
        self.guard.set_object_id(object_id=self.guid)

    def set_properties(self, *, rsrcdata: ResourceData):
        self.rsrcdata = rsrcdata

    def set_resource_type(self, *, resource_type: ResourceType):
        self.resource_type = resource_type

    def soft_lookup(self, *, rid: ID) -> IKernelReservation:
        return self.reservations.get(rid=rid)

    def soft_lookup_delegation(self, *, did: ID) -> IDelegation:
        return self.delegations.get(did, None)

    def __str__(self):
        msg = "{}({})".format(self.name, str(self.guid))
        if self.graph_id is not None:
            msg += " Graph Id:{}".format(self.graph_id)
        return msg

    def unregister(self, *, reservation: IKernelReservation):
        self.reservations.remove(reservation=reservation)

    def unregister_delegation(self, *, delegation: IDelegation):
        if delegation.get_delegation_id() in self.delegations:
            self.delegations.pop(delegation.get_delegation_id())

    def set_local_properties(self, *, value: dict):
        if self.rsrcdata is not None:
            self.rsrcdata.local_properties = value

    def set_config_properties(self, *, value: dict):
        if self.rsrcdata is not None:
            self.rsrcdata.configuration_properties = value

    def set_request_properties(self, *, value: dict):
        if self.rsrcdata is not None:
            self.rsrcdata.request_properties = value

    def set_resource_properties(self, *, value: dict):
        if self.rsrcdata is not None:
            self.rsrcdata.resource_properties = value
