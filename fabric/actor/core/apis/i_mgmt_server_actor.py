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
from typing import TYPE_CHECKING

from fabric.actor.core.apis.i_mgmt_actor import IMgmtActor
from fabric.actor.core.util.resource_type import ResourceType
from fabric.message_bus.messages.slice_avro import SliceAvro

if TYPE_CHECKING:
    from fabric.actor.core.util.id import ID
    from fabric.actor.core.manage.messages.ClientMng import ClientMng
    from fabric.actor.security.auth_token import AuthToken


class IMgmtServerActor(IMgmtActor):
    def get_broker_reservations(self) -> list:
        """
        Returns all reservations for which this actor acts as a broker.
        @return list of the reservations
        """
        raise NotImplementedError

    def get_inventory_slices(self) -> list:
        """
        Obtains all slices holding inventory, i.e., resources that can be
        delegated to other actors.
        @return list of slices
        """
        raise NotImplementedError

    def get_inventory_reservations(self) -> list:
        """
        Returns all resources held by this actor that can be used for delegations
        to client actors.
        @return list of reservations
        """
        raise NotImplementedError

    def get_inventory_reservations_by_slice_id(self, slice_id: ID) -> list:
        """
        Returns all resources in the specified slice held by this actor that can
        be used for delegations to client actors.
        @param sliceID slice id
        @return list of reservations for specific slice
        """
        raise NotImplementedError

    def get_client_slices(self) -> list:
        """
        Obtains all slices that hold delegated resources to other actors.
        @return list of client slices
        """
        raise NotImplementedError

    def add_client_slice(self, slice_mng: SliceAvro) -> ID:
        """
        Adds a new client slice.
        @param slice_mng slice to be added
        @return sliceid of the added slice
        """
        raise NotImplementedError

    def get_clients(self) -> list:
        """
        Returns all registered clients of this server actor.
        @return list of clients
        """
        raise NotImplementedError

    def get_client(self, guid: ID) -> ClientMng:
        """
        Returns the specified client record.
        @param guid client guid
        @return specified client record
        """
        raise NotImplementedError

    def register_client(self, client: ClientMng, kafka_topic: str) -> bool:
        """
        Registers a new client
        @param client client
        @param kafka_topic Kafka topic
        @return true for success; false otherwise
        """
        raise NotImplementedError

    def unregister_client(self, guid: ID) -> bool:
        """
        Unregisters the specified client.
        @param guid client guid
        @return true for success; false otherwise
        """
        raise NotImplementedError

    def get_client_reservations(self) -> list:
        """
        Obtains all client reservations.
        @return list of client reservations
        """
        raise NotImplementedError

    def get_client_reservations_by_slice_id(self, slice_id: ID) -> list:
        """
        Obtains all client reservations in the specified slice
        @param slice_id slice id
        @return list of reservations
        """
        raise NotImplementedError

    def export_resources_pool_client_slice(self, client_slice_id: ID, pool_id: ID, start: datetime, end: datetime,
                                           units: int, ticket_properties: dict, resource_properties: dict,
                                           source_ticket_id: ID) -> ID:
        """
        Exports resources into the specified client slice from the specified
        resource pool using the given source reservation. units number
        of units are exported from start to end.
        All properties passed into ticketProperties will be part of the ticket and signed.
        All properties passed into resourceProperties will be attached as resource propertie
        to the resource set (unsigned).
        @param client_slice_id client slice id
        @param pool_id pool slice id
        @param start start date
        @param end end date
        @param units units
        @param ticket_properties ticket properties
        @param resource_properties resource properties
        @param source_ticket_id ticket id
        @return returns the reservation id
        """
        raise NotImplementedError

    def export_resources_pool(self, pool_id: ID, start: datetime, end: datetime, units: int,
                              ticket_properties: dict, resource_properties: dict, source_ticket_id: ID,
                              client: AuthToken) -> ID:
        raise NotImplementedError

    def export_resources_client_slice(self, client_slice_id: ID, rtype: ResourceType, start: datetime, end: datetime,
                                      units: int, ticket_properties: dict, resource_properties: dict,
                                      source_ticket_id: ID) -> ID:
        raise NotImplementedError

    def export_resources(self, rtype: ResourceType, start: datetime, end: datetime, units: int,
                         ticket_properties: dict, resource_properties: dict, source_ticket_id: ID,
                         client: AuthToken) -> ID:
        raise NotImplementedError