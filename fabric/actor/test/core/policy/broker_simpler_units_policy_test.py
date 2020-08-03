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
from datetime import datetime

from fabric.actor.core.apis.i_broker import IBroker
from fabric.actor.core.apis.i_broker_policy import IBrokerPolicy
from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric.actor.core.apis.i_slice import ISlice
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.common.resource_pool_attribute_descriptor import ResourcePoolAttributeDescriptor, \
    ResourcePoolAttributeType
from fabric.actor.core.common.resource_pool_descriptor import ResourcePoolDescriptor
from fabric.actor.core.core.ticket import Ticket
from fabric.actor.core.kernel.broker_reservation_factory import BrokerReservationFactory
from fabric.actor.core.kernel.client_reservation_factory import ClientReservationFactory
from fabric.actor.core.kernel.sesource_set import ResourceSet
from fabric.actor.core.kernel.slice_factory import SliceFactory
from fabric.actor.core.policy.broker_simpler_units_policy import BrokerSimplerUnitsPolicy
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.util.resource_type import ResourceType
from fabric.actor.test.core.policy.broker_policy_test import BrokerPolicyTest


class BrokerSimplerUnitsPolicyTest(BrokerPolicyTest):
    def get_broker_policy(self) -> IBrokerPolicy:
        policy = BrokerSimplerUnitsPolicy()
        return policy

    def get_request_from_request(self, request: IBrokerReservation, units: int, rtype: ResourceType, start: datetime, end: datetime):
        rset = ResourceSet(units=units, rtype=rtype, rdata=ResourceData())
        term = Term(start=request.get_term().get_start_time(), end=end, new_start=start)
        result = BrokerReservationFactory.create(request.get_reservation_id(), rset, term, request.get_slice())
        result.set_sequence_in(request.get_sequence_in() + 1)
        return result

    def get_request(self, units: int, rtype: ResourceType, start: datetime, end: datetime):
        slice_obj = SliceFactory.create(ID(), name="test-slice")
        rset = ResourceSet(units=units, rtype=rtype, rdata=ResourceData())
        term = Term(start=start, end=end)
        request = BrokerReservationFactory.create(ID(), rset, term, slice_obj)
        return request

    def get_pool_descriptor(self, rtype: ResourceType) -> ResourcePoolDescriptor:
        rd = ResourcePoolDescriptor()
        rd.set_resource_type(rtype)
        rd.set_resource_type_label("Pool label: {}".format(rtype))
        ad = ResourcePoolAttributeDescriptor()
        ad.set_key(Constants.ResourceMemory)
        ad.set_label("Memory")
        ad.set_unit("MB")
        ad.set_type(ResourcePoolAttributeType.INTEGER)
        ad.set_value(str(1024))
        rd.add_attribute(ad)
        return rd

    def get_source(self, units: int, rtype: ResourceType, broker: IBroker, slice_obj: ISlice):
        start = broker.get_actor_clock().cycle_start_date(self.DonateStartCycle)
        end = broker.get_actor_clock().cycle_start_date(self.DonateEndCycle)
        term = Term(start=start, end=end)

        resources = ResourceSet(units=units, rtype=rtype, rdata=ResourceData())

        properties = resources.get_resource_properties()
        rd = self.get_pool_descriptor(rtype)
        resources.set_resource_properties(rd.save(properties, None))

        delegation = broker.get_plugin().get_ticket_factory().make_delegation(units=units, term=term, rtype=rtype)
        ticket = broker.get_plugin().get_ticket_factory().make_ticket(delegation=delegation)
        cs = Ticket(ticket=ticket, plugin=broker.get_plugin())
        resources.set_resources(cs)

        source = ClientReservationFactory.create(rid=ID(), resources=resources, term=term, slice_object=slice_obj)

        return source

