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
from datetime import datetime
from typing import TYPE_CHECKING, List

from fabric.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fabric.message_bus.messages.result_strings_avro import ResultStringsAvro
from fabric.message_bus.messages.result_avro import ResultAvro

if TYPE_CHECKING:
    from fabric.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
    from fabric.actor.security.auth_token import AuthToken
    from fabric.message_bus.messages.result_string_avro import ResultStringAvro
    from fabric.actor.core.util.resource_type import ResourceType
    from fabric.message_bus.messages.reservation_mng import ReservationMng
    from fabric.actor.core.util.id import ID
    from fabric.message_bus.messages.result_reservation_avro import ResultReservationAvro
    from fabric.message_bus.messages.proxy_avro import ProxyAvro
    from fabric.message_bus.messages.result_proxy_avro import ResultProxyAvro
    from fabric.message_bus.messages.result_pool_info_avro import ResultPoolInfoAvro


class IClientActorManagementObject:
    @abstractmethod
    def add_reservation(self, *, reservation: TicketReservationAvro, caller: AuthToken) -> ResultStringAvro:
        """
        Add a reservation
        @params reservation : reservation to be added
        @params caller: caller
        @returns reservation id on success and error code on failure (ResultStringAvro)
        """

    @abstractmethod
    def add_reservations(self, *, reservations: List[TicketReservationAvro], caller: AuthToken) -> ResultStringsAvro:
        """
        Add reservations
        @params reservations : reservations to be added
        @params caller: caller
        @returns reservation ids on success and error code on failure (ResultStringsAvro)
        """

    @abstractmethod
    def extend_reservation(self, *, reservation: id, new_end_time: datetime, new_units: int,
                           new_resource_type: ResourceType, request_properties: dict,
                           config_properties: dict, caller: AuthToken) -> ResultAvro:
        """
        Extend a reservation
        @params reservation : reservation to be extended
        @params new_end_time: new end time
        @params new_resource_type: new resource type
        @params config_properties: config_properties
        @params caller: caller
        @returns success or failure status
        """

    @abstractmethod
    def demand_reservation(self, *, reservation: ReservationMng, caller: AuthToken) -> ResultStringAvro:
        """
        Demand a reservation
        @params reservation : reservation to be demanded
        @params caller: caller
        @returns reservation id on success and error code on failure (ResultStringAvro)
        """

    @abstractmethod
    def demand_reservation_rid(self, *, rid: ID, caller: AuthToken) -> ResultAvro:
        """
        Demand a reservation by reservation id
        @params rid : reservation id of reservation to be demanded
        @params caller: caller
        @returns success or failure status
        """

    @abstractmethod
    def get_brokers(self, *, caller: AuthToken, id_token: str = None) -> ResultProxyAvro:
        """
        Get all brokers
        @params caller: caller
        @param id_token: id token
        @returns list of all the brokers
        """

    @abstractmethod
    def get_broker(self, *, broker_id: ID, caller: AuthToken, id_token: str = None) -> ResultProxyAvro:
        """
        Get a broker
        @params broker_id : broker_id
        @params caller: caller
        @param id_token: id token
        @returns broker information
        """

    @abstractmethod
    def add_broker(self, *, broker: ProxyAvro, caller: AuthToken) -> ResultAvro:
        """
        Add a broker
        @params broker: broker_proxy to be added
        @params caller: caller
        @returns success or failure status
        """

    @abstractmethod
    def claim_resources_slice(self, *, broker: ID, slice_id: ID, rid: ID, caller: AuthToken) -> ResultReservationAvro:
        """
        Claim resources for a slice
        @params broker : broker ID
        @params slice_id : slice ID
        @params rid : reservation ID
        @params caller: caller
        @returns ResultReservationAvro
        """

    @abstractmethod
    def claim_resources(self, *, broker: ID, rid: ID, caller: AuthToken) -> ResultReservationAvro:
        """
        Claim resources
        @params broker : broker ID
        @params rid : reservation ID
        @params caller: caller
        @returns ResultReservationAvro
        """

    @abstractmethod
    def get_pool_info(self, *, broker: ID, caller: AuthToken, id_token: str) -> ResultPoolInfoAvro:
        """
        Get Pool Info
        @params broker : broker ID
        @params caller: caller
        @params id_token: str
        @returns pool information
        """

    @abstractmethod
    def claim_delegations(self, *, broker: ID, did: ID, caller: AuthToken, id_token: str = None) -> ResultDelegationAvro:
        """
        Claim delegations
        @params broker : broker ID
        @params did : delegations ID
        @params caller: caller
        @param id_token: id token
        @returns ResultDelegationAvro
        """

    @abstractmethod
    def reclaim_delegations(self, *, broker: ID, did: ID, caller: AuthToken, id_token: str = None) -> ResultDelegationAvro:
        """
        ReClaim delegations
        @params broker : broker ID
        @params did : delegations ID
        @params caller: caller
        @param id_token: id token
        @returns ResultDelegationAvro
        """