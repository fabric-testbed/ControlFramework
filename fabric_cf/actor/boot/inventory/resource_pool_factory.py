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

from fabric_cf.actor.boot.inventory.i_resource_pool_factory import IResourcePoolFactory, ResourcePoolException
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.ticket import Ticket
from fabric_cf.actor.core.kernel.client_reservation_factory import ClientReservationFactory
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.resource_data import ResourceData

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_client_reservation import IClientReservation
    from fabric_cf.actor.core.apis.i_slice import ISlice
    from fabric_cf.actor.core.delegation.resource_ticket import ResourceTicket
    from fabric_cf.actor.core.apis.i_substrate import ISubstrate
    from fabric_cf.actor.core.common.resource_pool_descriptor import ResourcePoolDescriptor


class ResourcePoolFactory(IResourcePoolFactory):
    def __init__(self):
        # The resource pool descriptor. Its initial version is passed during initialization.
        # The factory can manipulate it as it sees fit and returns it back the the PoolCreator.
        self.desc = None
        # The actor's substrate
        self.substrate = None
        # The authority proxy for this actor.
        self.proxy = None
        # Slice representing the resource pool.
        self.slice_obj = None

    def update_descriptor(self):
        """
        Modifies the resource pool descriptor as needed
        @raises Exception in case of error
        """
        # Use this function to modify the resource pool descriptor, as needed. For example, you can define attributes
        # and resource pool properties needed by the resource pool. Resource pool attributes will become resource
        # properties (of the pool/slice and source reservation), while properties attached to the resource pool
        # descriptor will become local properties.

    def create_term(self) -> Term:
        """
        Creates the term for the source reservation.
        @return Term
        @throws Exception in case of error
        """
        clock = self.substrate.get_actor().get_actor_clock()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        now = GlobalsSingleton.get().get_container().get_current_cycle()
        start = self.desc.get_start()
        if start is None:
            start = clock.cycle_start_date(cycle=now)
        end = self.desc.get_end()
        if end is None:
            # export for one year
            length = 1000 * 60 * 60 * 24 * 365
            end = clock.cycle_end_date(cycle=(now + length))

        return Term(start=start, end=end)

    def create_resource_ticket(self, *, term: Term) -> ResourceTicket:
        """
        Creates the resource ticket for the source reservation
        @param term term
        @return ResourceTicket
        @throws Exception in case of error
        """
        try:
            delegation = self.substrate.get_ticket_factory().make_delegation(units=self.desc.get_units(), term=term,
                                                                             rtype=self.desc.get_resource_type())
            ticket = self.substrate.get_ticket_factory().make_ticket(delegation=delegation)
            return ticket
        except Exception as e:
            raise ResourcePoolException("Could not make ticket {}".format(e))

    def create_resource_data(self) -> ResourceData:
        rdata = ResourceData()
        rdata.resource_properties = self.slice_obj.get_resource_properties()
        rdata.local_properties = self.desc.get_pool_properties()
        rdata.resource_properties[Constants.pool_name] = self.slice_obj.get_name()
        return rdata

    def create_source_reservation(self, *, slice_obj: ISlice) -> IClientReservation:
        self.slice_obj = slice_obj
        term = self.create_term()
        resource_ticket = self.create_resource_ticket(term=term)
        ticket = Ticket(ticket=resource_ticket, plugin=self.substrate, authority=self.proxy)
        rdata = self.create_resource_data()
        resources = ResourceSet(concrete=ticket, rtype=self.desc.get_resource_type(), rdata=rdata)
        reservation = ClientReservationFactory.create(rid=ID(), resources=resources, term=term, slice_object=slice_obj)
        ClientReservationFactory.set_as_source(reservation=reservation)
        return reservation

    def get_descriptor(self) -> ResourcePoolDescriptor:
        self.update_descriptor()
        return self.desc

    def set_descriptor(self, *, descriptor: ResourcePoolDescriptor):
        self.desc = descriptor

    def set_substrate(self, *, substrate: ISubstrate):
        self.substrate = substrate
        auth = self.substrate.get_actor().get_identity()
        try:
            self.proxy = ActorRegistrySingleton.get().get_proxy(protocol=Constants.protocol_kafka,
                                                                actor_name=auth.get_name())
            if self.proxy is None:
                raise ResourcePoolException("Missing proxy")
        except Exception as e:
            raise ResourcePoolException("Could not obtain authority proxy: {} {}".format(auth.get_name(), e))
