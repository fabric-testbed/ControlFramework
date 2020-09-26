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

from fabric.actor.core.apis.i_actor import IActor
from fabric.actor.core.apis.i_authority import IAuthority
from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric.actor.core.apis.i_client_reservation import IClientReservation
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.apis.i_slice import ISlice
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.core.ticket import Ticket
from fabric.actor.core.kernel.client_reservation_factory import ClientReservationFactory
from fabric.actor.core.kernel.resource_set import ResourceSet
from fabric.actor.core.policy.authority_calendar_policy import AuthorityCalendarPolicy
from fabric.actor.core.policy.resource_control import ResourceControl
from fabric.actor.core.policy.vlan_control import VlanControl
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.util.resource_type import ResourceType
from fabric.actor.core.util.update_data import UpdateData
from fabric.actor.test.core.policy.authority_calendar_policy_test import AuthorityCalendarPolicyTest


class VlanControlTest(AuthorityCalendarPolicyTest, unittest.TestCase):
    StartVlan = 50
    EndVlan = StartVlan + AuthorityCalendarPolicyTest.DonateUnits - 1
    VlanBW = 100

    from fabric.actor.core.container.globals import Globals
    Globals.ConfigFile = Constants.TestVmAmConfigurationFile

    from fabric.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().start(force_fresh=True)
    while not GlobalsSingleton.get().start_completed:
        time.sleep(0.0001)

    def get_control(self, policy: AuthorityCalendarPolicy = None):
        if policy:
            return policy.controls_by_guid.values().__iter__().__next__()

        control = VlanControl()
        properties = {ResourceControl.PropertyControlResourceTypes: str(AuthorityCalendarPolicyTest.Type)}
        control.configure(properties=properties)
        return control

    def get_source(self, units: int, rtype: ResourceType, term: Term, actor: IActor, slice_obj: ISlice):
        resources = ResourceSet(units=units, rtype=rtype, rdata=ResourceData())

        local = resources.get_local_properties()
        local[Constants.PropertyVlanRangeNum] = "1"
        local[Constants.PropertyStartVlan + "0"] = str(self.StartVlan)
        local[Constants.PropertyEndVlan + "0"] = str(self.EndVlan)

        properties = {Constants.ResourceBandwidth: str(self.VlanBW)}

        delegation = actor.get_plugin().get_ticket_factory().make_delegation(units=units, term=term, rtype=rtype,
                                                                             properties=properties, vector=None)
        ticket = actor.get_plugin().get_ticket_factory().make_ticket(delegation=delegation, source=None)
        cs = Ticket(ticket=ticket, plugin=actor.get_plugin())
        resources.set_resources(cset=cs)
        source = ClientReservationFactory.create(rid=ID(), resources=resources, term=term, slice_object=slice_obj)
        return source

    def check_before_donate(self, authority: IAuthority):
        super().check_before_donate(authority)
        policy = authority.get_policy()
        control = self.get_control(policy)
        self.assertIsNotNone(control.tags)
        self.assertEqual(0, control.tags.size())

    def check_after_donate(self, authority: IAuthority, source: IClientReservation):
        super().check_after_donate(authority, source)
        policy = authority.get_policy()
        control = self.get_control(policy)

        self.assertEqual(self.DonateUnits, control.tags.size())
        self.assertEqual(self.DonateUnits, control.tags.get_free())
        self.assertEqual(0, control.tags.get_allocated())
        self.assertEqual(self.Type, control.rtype)

    vlan_tag = 0

    def check_incoming_lease(self, authority: IAuthority, request: IAuthorityReservation, incoming: IReservation,
                             udd: UpdateData):
        super().check_incoming_lease(authority, request, incoming, udd)
        policy = authority.get_policy()
        control = self.get_control(policy)

        rset = incoming.get_resources()
        uset = rset.get_resources()
        self.assertIsNotNone(uset)
        self.assertEqual(self.TicketUnits, uset.get_units())

        u = uset.get_set().values().__iter__().__next__()
        stag = u.get_property(name=Constants.UnitVlanTag)
        self.assertIsNotNone(stag)
        self.vlan_tag = int(stag)
        self.assertEqual(self.DonateUnits - 1, control.tags.get_free())
        self.assertEqual(1, control.tags.get_allocated())

    def check_incoming_close_lease(self, authority: IAuthority, request: IAuthorityReservation, incoming: IReservation,
                                   udd: UpdateData):
        super().check_incoming_close_lease(authority, request, incoming, udd)
        policy = authority.get_policy()
        control = self.get_control(policy)

        rset = incoming.get_resources()
        uset = rset.get_resources()
        self.assertIsNotNone(uset)
        self.assertEqual(0, uset.get_units())
        self.assertEqual(self.DonateUnits, control.tags.get_free())
        self.assertEqual(0, control.tags.get_allocated())

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
        stag = u.get_property(Constants.UnitVlanTag)
        self.assertIsNotNone(stag)
        self.assertEqual(self.vlan_tag, int(stag))
        self.assertEqual(self.DonateUnits - 1, control.tags.get_free())
        self.assertEqual(1, control.tags.get_allocated())

        self.assertEqual(self.DonateUnits, control.tags.get_free())
        self.assertEqual(0, control.tags.get_allocated())

