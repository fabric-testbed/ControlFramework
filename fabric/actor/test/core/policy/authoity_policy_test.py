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
import logging
import pickle
import time
import unittest

from fabric.actor.core.apis.i_actor import IActor
from fabric.actor.core.apis.i_actor_identity import IActorIdentity
from fabric.actor.core.apis.i_authority import IAuthority
from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric.actor.core.apis.i_client_reservation import IClientReservation
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.apis.i_slice import ISlice
from fabric.actor.core.core.ticket import Ticket
from fabric.actor.core.kernel.authority_reservation_factory import AuthorityReservationFactory
from fabric.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric.actor.core.kernel.sesource_set import ResourceSet
from fabric.actor.core.kernel.slice_factory import SliceFactory
from fabric.actor.core.plugins.config.config import Config
from fabric.actor.core.plugins.substrate.substrate import Substrate
from fabric.actor.core.plugins.substrate.db.substrate_actor_database import SubstrateActorDatabase
from fabric.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.util.resource_type import ResourceType
from fabric.actor.core.util.update_data import UpdateData
from fabric.actor.security.auth_token import AuthToken
from fabric.actor.test.base_test_case import BaseTestCase
from fabric.actor.test.controller_callback_helper import ControllerCallbackHelper


class AuthorityPolicyTest(BaseTestCase, unittest.TestCase):
    DonateStartCycle = 10
    DonateEndCycle = 100
    DonateUnits = 123
    Type = ResourceType("1")
    Label = "test resource"

    TicketStartCycle = DonateStartCycle + 1
    TicketEndCycle = TicketStartCycle + 10
    TicketNewEndCycle = TicketEndCycle + 10
    TicketUnits = 1

    Logger = logging.getLogger('AuthorityPolicyTest')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="actor.log")
    Logger.setLevel(logging.INFO)

    def setUp(self):
        #PsqlDatabase(BaseTestCase.DbUser, BaseTestCase.DbPwd, BaseTestCase.DbUser, BaseTestCase.DbHost, self.Logger).reset_db()
        #time.sleep(1)
        return

    def make_actor_database(self):
        return SubstrateActorDatabase(BaseTestCase.DbUser, BaseTestCase.DbPwd, BaseTestCase.DbUser, BaseTestCase.DbHost, self.Logger)

    def make_plugin(self):
        plugin = Substrate(None, None, None)
        config = Config()
        plugin.set_config(config)
        return plugin

    def get_authority(self):
        authority = super().get_authority()
        authority.set_recovered(True)
        Term.set_clock(authority.get_actor_clock())
        return authority

    def get_source(self, units: int, rtype: ResourceType, term: Term, actor: IActor, slice_obj: ISlice):
        raise Exception("Not implemented")

    def get_ticket(self, units: int, rtype: ResourceType, term: Term, source: IClientReservation, actor: IActor, holder: ID) -> Ticket:
        src_ticket = source.get_resources().get_resources().get_ticket()
        properties = src_ticket.get_delegation().get_properties()

        delegation = actor.get_plugin().get_ticket_factory().make_delegation(units=units, term=term, rtype=rtype, properties=properties, holder=holder)
        ticket = actor.get_plugin().get_ticket_factory().make_ticket(delegation=delegation, source=src_ticket)
        cs = Ticket(ticket, actor.get_plugin(), None)
        return cs

    def get_request_slice(self):
        return SliceFactory.create(ID(), "test-slice", ResourceData())

    def get_request(self, units: int, rtype: ResourceType, term: Term, ticket: Ticket):
        rset = ResourceSet(units=units, rtype=rtype, rdata=ResourceData())
        rset.set_resources(ticket)
        slice_object = self.get_request_slice()
        return AuthorityReservationFactory.create(rset, term, slice_object, ID())

    def get_request_from_request(self, request: IAuthorityReservation, units: int, rtype: ResourceType, term: Term, ticket: Ticket):
        rset = ResourceSet(units=units, rtype=rtype, rdata=ResourceData())
        rset.set_resources(ticket)
        result = AuthorityReservationFactory.create(rset, term, request.get_slice(), request.get_reservation_id())
        result.set_sequence_in(request.get_sequence_in() + 1)
        return result

    def check_before_donate(self, authority: IAuthority):
        return

    def check_after_donate(self, authority: IAuthority, source: IClientReservation):
        return

    def get_donate_source(self, actor: IActor) -> IClientReservation:
        slice_object = SliceFactory.create(slice_id=ID(), name="inventory-slice")
        slice_object.set_inventory(True)
        actor.register_slice(slice_object)
        start = actor.get_actor_clock().cycle_start_date(self.DonateStartCycle)
        end = actor.get_actor_clock().cycle_end_date(self.DonateEndCycle)
        term = Term(start=start, end=end)

        source = self.get_source(self.DonateUnits, self.Type, term, actor, slice_object)
        actor.register(source)
        return source

    def check_before_donate_set(self, authority: IAuthority):
        return

    def check_after_donate_set(self, authority: IAuthority, rset: ResourceSet):
        return

    def get_donate_set(self, authority: IAuthority) -> ResourceSet:
        return ResourceSet(units=self.DonateUnits, rtype=self.Type, rdata=ResourceData())

    def get_redeem_request(self, authority: IAuthority, source: IClientReservation, identity: IActorIdentity):
        req_start = authority.get_actor_clock().cycle_start_date(self.TicketStartCycle)
        req_end = authority.get_actor_clock().cycle_end_date(self.TicketEndCycle)
        req_term = Term(start=req_start, end=req_end)
        ticket = self.get_ticket(self.TicketUnits, self.Type, req_term, source, authority, identity.get_guid())
        request = self.get_request(self.TicketUnits, self.Type, req_term, ticket)
        return request

    def get_extend_lease_request(self, authority: IAuthority, source: IClientReservation, identity: IActorIdentity, request: IAuthorityReservation):
        req_start = authority.get_actor_clock().cycle_start_date(self.TicketStartCycle)
        req_new_start = authority.get_actor_clock().cycle_start_date(self.TicketEndCycle + 1)
        req_end = authority.get_actor_clock().cycle_end_date(self.TicketNewEndCycle)
        req_term = Term(start=req_start, end=req_end, new_start=req_new_start)
        ticket = self.get_ticket(self.TicketUnits, self.Type, req_term, source, authority, identity.get_guid())
        new_request = self.get_request_from_request(request, self.TicketUnits, self.Type, req_term, ticket)
        return new_request

    def check_incoming_lease(self, authority: IAuthority, request: IAuthorityReservation, incoming: IReservation, udd: UpdateData):
        self.assertIsNotNone(incoming)

        rset = incoming.get_resources()

        self.assertIsNotNone(rset)

        self.assertEqual(request.get_requested_units(), rset.get_units())

        self.assertEqual(incoming.get_term(), request.get_requested_term())

    def check_incoming_close_lease(self, authority: IAuthority, request: IAuthorityReservation, incoming: IReservation, udd: UpdateData):
        self.assertIsNotNone(incoming)

        rset = incoming.get_resources()

        self.assertIsNotNone(rset)

        self.assertEqual(request.get_requested_units(), rset.get_units())

    def external_tick(self, site: IAuthority, cycle: int):
        site.external_tick(cycle)
        while site.get_current_cycle() != cycle:
            time.sleep(0.001)

    def check_incoming_extend_lease(self, authority: IAuthority, request: IAuthorityReservation, incoming: IReservation, update_data: UpdateData):
        self.assertIsNotNone(incoming)
        self.assertEqual(ReservationStates.Active, incoming.get_state())
        self.assertEqual(ReservationPendingStates.None_, incoming.get_pending_state())
        rset = incoming.get_resources()
        self.assertIsNotNone(rset)
        self.assertEqual(request.get_requested_units(), rset.get_units())
        self.assertEqual(incoming.get_term(), request.get_requested_term())

    def test_create(self):
        authority = self.get_authority()

    def __test_donate(self):
        site = self.get_authority()
        policy = site.get_policy()
        self.check_before_donate(site)
        source = self.get_donate_source(site)
        policy.donate_reservation(source)
        self.check_after_donate(site, source)

    def __test_donate_set(self):
        site = self.get_authority()
        policy = site.get_policy()
        self.check_before_donate_set(site)
        rset = self.get_donate_set(site)
        policy.donate(rset)
        self.check_after_donate_set(site, rset)

    def __test_redeem(self):
        site = self.get_authority()
        policy = site.get_policy()
        controller = self.get_controller()
        proxy = ControllerCallbackHelper(controller.get_name(), controller.get_guid())
        authority_proxy = ControllerCallbackHelper(site.get_name(), site.get_guid())

        ActorRegistrySingleton.get().register_callback(proxy)
        ActorRegistrySingleton.get().register_callback(authority_proxy)

        source = self.get_donate_source(site)
        policy.donate_reservation(source)

        source_set = self.get_donate_set(site)
        policy.donate(source_set)

        request = self.get_redeem_request(site, source, proxy)

        class UpdateLeaseHandler(ControllerCallbackHelper.IUpdateLeaseHandler):
            def __init__(self, parent):
                self.waiting_for_lease = True
                self.waiting_for_close = False
                self.parent = parent

            def handle_update_lease(self, reservation: IReservation, update_data: UpdateData, caller: AuthToken):
                if self.waiting_for_lease:
                    self.parent.check_incoming_lease(site, request, reservation, update_data)
                    self.waiting_for_lease = False
                    self.waiting_for_close = True
                elif self.waiting_for_close:
                    self.parent.assertTrue(site.get_current_cycle() >= AuthorityPolicyTest.TicketEndCycle)
                    self.parent.check_incoming_close_lease(site, request, reservation, update_data)
                    self.waiting_for_close = False
                else:
                    raise Exception("Invalid state")

            def check_termination(self):
                self.parent.assertFalse(self.waiting_for_lease)
                self.parent.assertFalse(self.waiting_for_close)

        handler = UpdateLeaseHandler(self)
        proxy.set_update_lease_handler(handler)

        self.external_tick(site, 0)
        print("Redeeming request...")
        site.redeem(request, proxy, proxy.get_identity())

        for cycle in range(1, self.DonateEndCycle):
            self.external_tick(site, cycle)

        handler.check_termination()

    def __test_extend_lease(self):
        site = self.get_authority()
        policy = site.get_policy()
        controller = self.get_controller()
        proxy = ControllerCallbackHelper(controller.get_name(), controller.get_guid())
        auth_proxy = ControllerCallbackHelper(site.get_name(), site.get_guid())
        ActorRegistrySingleton.get().register_callback(proxy)
        ActorRegistrySingleton.get().register_callback(auth_proxy)

        source = self.get_donate_source(site)
        policy.donate_reservation(source)
        source_set = self.get_donate_set(site)
        policy.donate(source_set)
        request = self.get_redeem_request(site, source, proxy)
        extend = self.get_extend_lease_request(site, source, proxy, request)

        class UpdateLeaseHandler(ControllerCallbackHelper.IUpdateLeaseHandler):
            def __init__(self, parent):
                self.waiting_for_extend_lease = False
                self.waiting_for_lease = True
                self.waiting_for_close = False
                self.parent = parent

            def handle_update_lease(self, reservation: IReservation, update_data: UpdateData, caller: AuthToken):
                if self.waiting_for_lease:
                    self.parent.check_incoming_lease(site, request, reservation, update_data)
                    self.waiting_for_lease = False
                    self.waiting_for_close = True
                elif self.waiting_for_close:
                    self.parent.assertTrue(site.get_current_cycle() >= AuthorityPolicyTest.TicketEndCycle)
                    self.parent.check_incoming_close_lease(site, request, reservation, update_data)
                    self.waiting_for_close = False
                else:
                    raise Exception("Invalid state")

            def check_termination(self):
                self.parent.assertFalse(self.waiting_for_lease)
                self.parent.assertFalse(self.waiting_for_close)

        handler = UpdateLeaseHandler(self)

        proxy.set_update_lease_handler(handler)
        self.external_tick(site, 0)
        print("Redeeming request")
        site.redeem(request, proxy, proxy.get_identity())
        for cycle in range(self.DonateEndCycle):
            if cycle == self.TicketEndCycle - 50:
                print("Extending Lease")
                site.extend_lease(extend, proxy.get_identity())
            self.external_tick(site, cycle)
        handler.check_termination()

    def __test_close(self):
        site = self.get_authority()
        policy = site.get_policy()
        controller = self.get_controller()
        proxy = ControllerCallbackHelper(controller.get_name(), controller.get_guid())
        authority_proxy = ControllerCallbackHelper(site.get_name(), site.get_guid())

        ActorRegistrySingleton.get().register_callback(proxy)
        ActorRegistrySingleton.get().register_callback(authority_proxy)

        source = self.get_donate_source(site)
        policy.donate_reservation(source)

        source_set = self.get_donate_set(site)
        policy.donate(source_set)

        request = self.get_redeem_request(site, source, proxy)

        class UpdateLeaseHandler(ControllerCallbackHelper.IUpdateLeaseHandler):
            def __init__(self, parent):
                self.waiting_for_lease = True
                self.waiting_for_close = False
                self.parent = parent

            def handle_update_lease(self, reservation: IReservation, update_data: UpdateData, caller: AuthToken):
                if self.waiting_for_lease:
                    self.parent.check_incoming_lease(site, request, reservation, update_data)
                    self.waiting_for_lease = False
                    self.waiting_for_close = True
                elif self.waiting_for_close:
                    print(site.get_current_cycle())
                    self.parent.assertTrue(site.get_current_cycle() < AuthorityPolicyTest.TicketEndCycle)
                    self.parent.check_incoming_close_lease(site, request, reservation, update_data)
                    self.waiting_for_close = False
                else:
                    raise Exception("Invalid state")

            def check_termination(self):
                self.parent.assertFalse(self.waiting_for_lease)
                self.parent.assertFalse(self.waiting_for_close)

        handler = UpdateLeaseHandler(self)
        proxy.set_update_lease_handler(handler)

        self.external_tick(site, 0)
        print("Redeeming request...")
        site.redeem(request, proxy, proxy.get_identity())

        for cycle in range(1, self.DonateEndCycle):
            if cycle == self.TicketEndCycle - 5:
                site.close(request)
            self.external_tick(site, cycle)

        handler.check_termination()
