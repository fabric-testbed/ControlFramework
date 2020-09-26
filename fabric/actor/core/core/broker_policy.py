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

import threading
from typing import TYPE_CHECKING, Dict

from fabric.actor.core.common.constants import Constants
from fabric.actor.core.common.resource_pool_descriptor import ResourcePoolDescriptor
from fabric.actor.core.core.ticket import Ticket
from fabric.actor.core.util.prop_list import PropList
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.util.resource_type import ResourceType

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_broker import IBroker
    from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
    from fabric.actor.core.apis.i_client_reservation import IClientReservation
    from fabric.actor.core.apis.i_reservation import IReservation
    from fabric.actor.core.time.term import Term
    from fabric.actor.core.delegation.resource_delegation import ResourceDelegation
    from fabric.actor.core.apis.i_server_reservation import IServerReservation
    from fabric.actor.core.util.id import ID

from fabric.actor.core.apis.i_broker_policy import IBrokerPolicy
from fabric.actor.core.core.policy import Policy
from fabric.actor.core.kernel.resource_set import ResourceSet


class BrokerPolicy(Policy, IBrokerPolicy):
    def __init__(self, *, actor: IBroker):
        super().__init__(actor=actor)
        # If true, every allocated ticket will require administrative approval
        # before being sent back.
        self.required_approval = False
        # A list of reservations that require administrative approval. Note: this
        # field is private so that derived classes are forced to use the locked
        # methods to operate on it.
        self.for_approval = None
        # Initialization status.
        self.initialized = False

        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']

        del state['for_approval']
        del state['lock']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.actor = None
        self.clock = None
        self.initialized = False

        self.lock = threading.Lock()

        # TODO Fetch Actor object and setup logger, actor and clock member variables

    def add_for_approval(self, *, reservation: IBrokerReservation):
        """
        Adds the reservation to the approval list.

        @param reservation reservation
        """
        try:
            self.lock.acquire()
            self.for_approval.add(reservation=reservation)
        finally:
            self.lock.release()

    def allocate(self, *, cycle: int):
        return

    def approve(self, *, reservation: IBrokerReservation):
        """
        Approve a reservation. To be used by policies that require administrative
        intervention. Override to provide your own approval policy.

        @param reservation reservation
        """
        return

    def bind(self, *, reservation: IBrokerReservation) -> bool:
        return False

    def demand(self, *, reservation: IClientReservation):
        return

    def donate_reservation(self, *, reservation: IClientReservation):
        return

    def extend_broker(self, *, reservation:IBrokerReservation) -> bool:
        return False

    def initialize(self):
        if not self.initialized:
            super().initialize()
            self.initialized = True

    def release_not_approved(self, *, reservation: IBrokerReservation):
        """
        Releases the resources for a reservation that was rejected by the
        administrator.
        
        @param reservation reservation
        """
        return

    def remove_for_approval(self, *, reservation: IBrokerReservation):
        try:
            self.lock.acquire()
            self.for_approval.remove(reservation=reservation)
        finally:
            self.lock.release()

    def revisit(self, *, reservation: IReservation):
        if isinstance(reservation, IClientReservation):
            self.donate_reservation(reservation=reservation)

    def ticket_satisfies(self, *, requested_resources: ResourceSet, actual_resources: ResourceSet,
                         requested_term: Term, actual_term: Term):
        return

    def update_ticket_complete(self, *, reservation: IClientReservation):
        self.donate_reservation(reservation=reservation)

    def extract(self, *, source: ResourceSet, delegation: ResourceDelegation) -> ResourceSet:
        """
        Creates a new resource set using the source and the specified delegation.
        
        @param source source
        @param delegation delegation 
        @return returns ResourceSet 
        @throws Exception in case of error
        """
        rd = ResourceData()
        temp = PropList.merge_properties(incoming=source.get_resource_properties(),
                                         outgoing=rd.get_resource_properties())
        rd.resource_properties = temp

        extracted = ResourceSet(units=delegation.get_units(), rtype=delegation.get_resource_type(), rdata=rd)

        ticket = source.get_resources()
        resource_ticket = source.get_resources().get_ticket()

        new_ticket = self.actor.get_plugin().get_ticket_factory().make_ticket(delegation=delegation,
                                                                              source=resource_ticket)

        cset = Ticket(plugin=ticket.get_plugin(), ticket=new_ticket, authority=ticket.get_authority())

        extracted.set_resources(cset=cset)
        return extracted

    def get_client_id(self, *, reservation: IServerReservation) -> ID:
        return reservation.get_client_auth_token().get_guid()

    @staticmethod
    def get_resource_pools_query() -> dict:
        properties = {Constants.QueryAction : Constants.QueryActionDiscoverPools}
        return properties

    @staticmethod
    def get_resource_pools(response: dict) -> Dict[ResourceType, ResourcePoolDescriptor]:
        result = {}

        try:
            if Constants.PoolsCount in response:
                count = int(response[Constants.PoolsCount])
                for i in range(count):
                    rd = ResourcePoolDescriptor()
                    rd.reset(properties=response, prefix=Constants.PoolPrefix + str(i) + ".")
                    result[rd.get_resource_type()] = rd

        except Exception as e:
            raise e

        return result

    @staticmethod
    def get_query_action(properties: dict) -> str:
        if properties is None:
            return None
        return properties.get(Constants.QueryAction, None)