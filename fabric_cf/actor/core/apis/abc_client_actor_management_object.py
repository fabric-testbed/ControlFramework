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
from datetime import datetime
from typing import TYPE_CHECKING, List

from fabric_mb.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fabric_mb.message_bus.messages.result_strings_avro import ResultStringsAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fim.user import GraphFormat

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
    from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
    from fabric_mb.message_bus.messages.result_proxy_avro import ResultProxyAvro
    from fabric_mb.message_bus.messages.result_broker_query_model_avro import ResultBrokerQueryModelAvro
    from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
    from fabric_mb.message_bus.messages.reservation_mng import ReservationMng

    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.util.id import ID


class ABCClientActorManagementObject(ABC):
    @abstractmethod
    def add_reservation(self, *, reservation: TicketReservationAvro, caller: AuthToken) -> ResultStringAvro:
        """
        Add a reservation
        @param reservation : reservation to be added
        @param caller: caller
        @return reservation id on success and error code on failure (ResultStringAvro)
        """

    @abstractmethod
    def add_reservations(self, *, reservations: List[TicketReservationAvro], caller: AuthToken) -> ResultStringsAvro:
        """
        Add reservations
        @param reservations : reservations to be added
        @param caller: caller
        @return reservation ids on success and error code on failure (ResultStringsAvro)
        """

    @abstractmethod
    def extend_reservation(self, *, reservation: id, new_end_time: datetime, new_units: int,
                           caller: AuthToken) -> ResultAvro:
        """
        Extend a reservation
        @param reservation : reservation to be extended
        @param new_end_time: new end time
        @param new_units: new units
        @param caller: caller
        @return success or failure status
        """

    @abstractmethod
    def demand_reservation(self, *, reservation: ReservationMng, caller: AuthToken) -> ResultStringAvro:
        """
        Demand a reservation
        @param reservation : reservation to be demanded
        @param caller: caller
        @return reservation id on success and error code on failure (ResultStringAvro)
        """

    @abstractmethod
    def demand_reservation_rid(self, *, rid: ID, caller: AuthToken) -> ResultAvro:
        """
        Demand a reservation by reservation id
        @param rid : reservation id of reservation to be demanded
        @param caller: caller
        @return success or failure status
        """

    @abstractmethod
    def get_brokers(self, *, caller: AuthToken, broker_id: ID = None, id_token: str = None) -> ResultProxyAvro:
        """
        Get all brokers
        @param caller: caller
        @param broker_id : broker_id
        @param id_token: id token
        @return list of all the brokers
        """

    @abstractmethod
    def add_broker(self, *, broker: ProxyAvro, caller: AuthToken) -> ResultAvro:
        """
        Add a broker
        @param broker: broker_proxy to be added
        @param caller: caller
        @return success or failure status
        """

    @abstractmethod
    def get_broker_query_model(self, *, broker: ID, caller: AuthToken, id_token: str,
                               level: int, graph_format: GraphFormat) -> ResultBrokerQueryModelAvro:
        """
        Get Pool Info
        @param broker : broker ID
        @param caller: caller
        @param id_token: str
        @param level: level of details
        @param graph_format: Graph Format
        @return pool information
        """

    @abstractmethod
    def claim_delegations(self, *, broker: ID, did: ID, caller: AuthToken,
                          id_token: str = None) -> ResultDelegationAvro:
        """
        Claim delegations
        @param broker : broker ID
        @param did : delegations ID
        @param caller: caller
        @param id_token: id token
        @return ResultDelegationAvro
        """

    @abstractmethod
    def reclaim_delegations(self, *, broker: ID, did: ID, caller: AuthToken,
                            id_token: str = None) -> ResultDelegationAvro:
        """
        ReClaim delegations
        @param broker : broker ID
        @param did : delegations ID
        @param caller: caller
        @param id_token: id token
        @return ResultDelegationAvro
        """
