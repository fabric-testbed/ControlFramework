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
import time
import unittest

from fabric_cf.actor.core.apis.i_actor import IActor
from fabric_cf.actor.core.apis.i_authority import IAuthority
from fabric_cf.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric_cf.actor.core.apis.i_client_reservation import IClientReservation
from fabric_cf.actor.core.apis.i_reservation import IReservation
from fabric_cf.actor.core.apis.i_slice import ISlice
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.resource_pool_attribute_descriptor import ResourcePoolAttributeDescriptor, \
    ResourcePoolAttributeType
from fabric_cf.actor.core.common.resource_pool_descriptor import ResourcePoolDescriptor
from fabric_cf.actor.core.core.ticket import Ticket
from fabric_cf.actor.core.kernel.client_reservation_factory import ClientReservationFactory
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.policy.authority_calendar_policy import AuthorityCalendarPolicy
from fabric_cf.actor.core.policy.resource_control import ResourceControl
from fabric_cf.actor.core.policy.simple_vm_control import SimpleVMControl
from fabric_cf.actor.core.policy.vm_control import VMControl
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_data import ResourceData
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.test.core.policy.authority_calendar_policy_test import AuthorityCalendarPolicyTest


class SimpleVMControlTest(AuthorityCalendarPolicyTest, unittest.TestCase):
    AttributeValueMemory = "128"

    from fabric_cf.actor.core.container.globals import Globals
    Globals.config_file = "../../config/config.test.yaml"
    Constants.superblock_location = './state_recovery.lock'

    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().start(force_fresh=True)
    while not GlobalsSingleton.get().start_completed:
        time.sleep(0.0001)

    def get_control(self, policy: AuthorityCalendarPolicy = None):
        if policy:
            return policy.controls_by_guid.values().__iter__().__next__()

        control = SimpleVMControl()
        properties = {ResourceControl.PropertyControlResourceTypes: str(self.Type)}
        control.configure(properties=properties)
        return control

    def get_pool_descriptor(self) -> ResourcePoolDescriptor:
        rd = ResourcePoolDescriptor()
        rd.set_resource_type(rtype=self.Type)
        rd.set_resource_type_label(rtype_label=self.Label)
        ad = ResourcePoolAttributeDescriptor()
        ad.set_key(value=Constants.resource_memory)
        ad.set_label(label="Memory")
        ad.set_unit(unit="MB")
        ad.set_type(rtype=ResourcePoolAttributeType.INTEGER)
        ad.set_value(value=self.AttributeValueMemory)
        rd.add_attribute(attribute=ad)
        return rd

    def get_source(self, units: int, rtype: ResourceType, term: Term, actor: IActor, slice_obj: ISlice):
        resources = ResourceSet(units=units, rtype=rtype, rdata=ResourceData())

        properties = resources.get_resource_properties()
        rd = self.get_pool_descriptor()
        resources.set_resource_properties(p=rd.save(properties=properties, prefix=None))

        local = resources.get_local_properties()
        local[VMControl.PropertyIPSubnet] = "255.255.255.0"
        local[VMControl.PropertyIPList] = "192.168.1.2/24"

        delegation = actor.get_plugin().get_ticket_factory().make_delegation(units=units, term=term, rtype=rtype,
                                                                             vector=None)
        ticket = actor.get_plugin().get_ticket_factory().make_ticket(delegation=delegation, source=None)
        cs = Ticket(ticket=ticket, plugin=actor.get_plugin())
        resources.set_resources(cset=cs)
        source = ClientReservationFactory.create(rid=ID(), resources=resources, term=term, slice_object=slice_obj)
        return source

    def check_before_donate(self, authority: IAuthority):
        super().check_before_donate_set(authority)
        policy = authority.get_policy()
        control = self.get_control(policy=policy)
        self.assertIsNotNone(control.inventory)
        self.assertEqual(0, len(control.inventory))

    def check_after_donate(self, authority: IAuthority, source: IClientReservation):
        policy = authority.get_policy()
        control = self.get_control(policy)

        self.assertEqual(253, control.ipset.get_free_count())
        self.assertEqual(1, len(control.inventory))
        pool = control.inventory.values().__iter__().__next__()

        self.assertEqual(self.DonateUnits, pool.get_free())
        self.assertEqual(self.Type, pool.get_type())

    my_unit = None

    def check_incoming_lease(self, authority: IAuthority, request: IAuthorityReservation,
                             incoming: IReservation, udd: UpdateData):
        super().check_incoming_lease(authority=authority, request=request, incoming=incoming, udd=udd)
        policy = authority.get_policy()
        control = self.get_control(policy)
        rset = incoming.get_resources()
        uset = rset.get_resources()
        self.assertIsNotNone(uset)
        self.assertEqual(self.TicketUnits, uset.get_units())

        u = uset.get_set().values().__iter__().__next__()
        self.assertEqual(self.AttributeValueMemory, u.get_property(name=Constants.unit_memory))
        self.assertIsNotNone(u.get_property(name=Constants.unit_management_ip))
        self.assertIsNotNone(u.get_property(name=Constants.unit_manage_subnet))
        pool = control.inventory.get(self.Type)
        self.assertIsNotNone(pool)
        self.assertEqual(1, pool.get_allocated())
        self.assertEqual(self.DonateUnits -1, pool.get_free())
        self.my_unit = u

    def check_incoming_close_lease(self, authority: IAuthority, request: IAuthorityReservation, incoming: IReservation,
                                   udd: UpdateData):
        super().check_incoming_close_lease(authority, request, incoming, udd)
        policy = authority.get_policy()
        control = self.get_control(policy)

        self.assertIsNotNone(self.my_unit)

        rset = incoming.get_resources()
        uset = rset.get_resources()
        self.assertIsNotNone(uset)
        self.assertEqual(0, uset.get_units())

        pool = control.inventory.get(self.Type)
        self.assertIsNotNone(pool)

        self.assertEqual(0, pool.get_allocated())
        self.assertEqual(self.DonateUnits, pool.get_free())

    def check_incoming_extend_lease(self, authority: IAuthority, request: IAuthorityReservation, incoming: IReservation,
                                    update_data: UpdateData):
        super().check_incoming_extend_lease(authority, request, incoming, update_data)
        policy = authority.get_policy()
        control = self.get_control(policy)

        rset = incoming.get_resources()
        uset = rset.get_resources()
        self.assertIsNotNone(uset)
        self.assertEqual(self.TicketUnits, uset.get_units())

        u = uset.get_set().values().__iter__().__next__()
        self.assertEqual(self.AttributeValueMemory, int(u.get_property(name=Constants.unit_memory)))
        self.assertIsNotNone(u.get_property(name=Constants.unit_management_ip))
        self.assertIsNotNone(u.get_property(name=Constants.unit_manage_subnet))

        pool = control.inventory.get(self.Type)
        self.assertIsNotNone(pool)
        self.assertEqual(1, pool.get_allocated())
        self.assertEqual(self.DonateUnits - 1, pool.get_free())
        self.assertEqual(self.my_unit, u)