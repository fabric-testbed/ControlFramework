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

from abc import abstractmethod
from typing import TYPE_CHECKING, List

from fim.graph.abc_property_graph import ABCPropertyGraph
from fabric_cf.actor.core.apis.abc_mgmt_actor import ABCMgmtActor

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
    from fabric_mb.message_bus.messages.slice_avro import SliceAvro
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.manage.messages.client_mng import ClientMng
    from fabric_cf.actor.security.auth_token import AuthToken


class ABCMgmtServerActor(ABCMgmtActor):
    """
    Interface for Management Server Actor
    """
    @abstractmethod
    def get_broker_reservations(self, *, id_token: str = None) -> List[ReservationMng]:
        """
        Returns all reservations for which this actor acts as a broker.
        @param id_token id token
        @return list of the reservations
        """

    @abstractmethod
    def get_inventory_slices(self, *, id_token: str = None) -> List[SliceAvro]:
        """
        Obtains all slices holding inventory, i.e., resources that can be
        delegated to other actors.
        @param id_token id token
        @return list of slices
        """

    @abstractmethod
    def get_inventory_reservations(self, *, slice_id: ID = None, id_token: str = None) -> List[ReservationMng]:
        """
        Returns all resources held by this actor that can be used for delegations
        to client actors.
        @param slice_id slice id
        @param id_token id token
        @return list of reservations
        """

    @abstractmethod
    def get_client_slices(self, *, id_token: str = None) -> List[SliceAvro]:
        """
        Obtains all slices that hold delegated resources to other actors.
        @param id_token id token
        @return list of client slices
        """

    @abstractmethod
    def add_client_slice(self, *, slice_mng: SliceAvro) -> ID:
        """
        Adds a new client slice.
        @param slice_mng slice to be added
        @return sliceid of the added slice
        """

    @abstractmethod
    def get_clients(self, *, guid: ID = None, id_token: str = None) -> List[ClientMng]:
        """
        Returns all registered clients of this server actor.
        @param guid client guid
        @param id_token id token
        @return list of clients
        """

    @abstractmethod
    def register_client(self, *, client: ClientMng, kafka_topic: str) -> bool:
        """
        Registers a new client
        @param client client
        @param kafka_topic Kafka topic
        @return true for success; false otherwise
        """

    @abstractmethod
    def unregister_client(self, *, guid: ID) -> bool:
        """
        Unregisters the specified client.
        @param guid client guid
        @return true for success; false otherwise
        """

    @abstractmethod
    def get_client_reservations(self, *, slice_id: ID = None, id_token: str = None) -> List[ReservationMng]:
        """
        Obtains all client reservations.
        @param slice_id slice id
        @param id_token id token
        @return list of client reservations
        """

    @abstractmethod
    def advertise_resources(self, *, delegation: ABCPropertyGraph, delegation_name: str, client: AuthToken) -> ID:
        """
        Advertise resources to the broker
        @param delegation: delegation
        @param delegation_name delegation name
        @param client: client
        @return return the reservation id
        """
