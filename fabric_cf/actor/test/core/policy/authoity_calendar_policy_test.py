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
import time
import unittest

from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Capacities, CapacityHints
from fim.slivers.instance_catalog import InstanceCatalog
from fim.slivers.network_node import NodeType, NodeSliver

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_authority import ABCAuthority
from fabric_cf.actor.core.apis.abc_authority_policy import ABCAuthorityPolicy
from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
from fabric_cf.actor.core.apis.abc_database import ABCDatabase
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation, DelegationState
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import AuthorityException
from fabric_cf.actor.core.core.ticket import Ticket
from fabric_cf.actor.core.delegation.delegation_factory import DelegationFactory
from fabric_cf.actor.core.delegation.resource_ticket import ResourceTicketFactory
from fabric_cf.actor.core.kernel.authority_reservation import AuthorityReservationFactory
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.plugins.handlers.ansible_handler_processor import AnsibleHandlerProcessor
from fabric_cf.actor.core.plugins.handlers.configuration_mapping import ConfigurationMapping
from fabric_cf.actor.core.plugins.substrate.db.substrate_actor_database import SubstrateActorDatabase
from fabric_cf.actor.core.plugins.substrate.substrate_mixin import SubstrateMixin
from fabric_cf.actor.core.policy.authority_calendar_policy import AuthorityCalendarPolicy
from fabric_cf.actor.core.policy.network_node_control import NetworkNodeControl
from fabric_cf.actor.core.policy.resource_control import ResourceControl
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.security.auth_token import AuthToken
from fabric_cf.actor.test.base_test_case import BaseTestCase
from fabric_cf.actor.test.controller_callback_helper import ControllerCallbackHelper, IUpdateLeaseHandler
from fabric_cf.actor.test.fim_test_helper import FimTestHelper


class AuthorityCalendarPolicyTest(BaseTestCase, unittest.TestCase):
    DonateStartCycle = 10
    DonateEndCycle = 100
    TicketStartCycle = DonateStartCycle + 1
    TicketEndCycle = TicketStartCycle + 10
    TicketNewEndCycle = TicketEndCycle + 10
    TicketUnits = 1

    my_unit = None

    logger = logging.getLogger('AuthorityCalendarPolicyTest')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="actor.log")
    logger.setLevel(logging.INFO)

    from fabric_cf.actor.core.container.globals import Globals
    Globals.config_file = "./config/config.test.yaml"
    Constants.SUPERBLOCK_LOCATION = './state_recovery.lock'

    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().start(force_fresh=True)
    while not GlobalsSingleton.get().start_completed:
        time.sleep(0.0001)

    FimTestHelper.n4j_imp.delete_all_graphs()
    arm, adm = FimTestHelper.generate_renci_adm()

    def build_sliver(self) -> BaseSliver:
        node_sliver = NodeSliver()
        node_sliver.resource_type = NodeType.VM
        node_sliver.node_id = "test-slice-node-1"
        cap = Capacities()
        cap.set_fields(core=4, ram=64, disk=500)
        catalog = InstanceCatalog()
        instance_type = catalog.map_capacities_to_instance(cap=cap)
        cap_hints = CapacityHints().set_fields(instance_type=instance_type)
        node_sliver.set_properties(name="node-1", type=NodeType.VM, site="RENC",
                                   capacities=cap, image_type='qcow2', image_ref='default_centos_8',
                                   capacity_hints=cap_hints)
        node_sliver.set_capacity_allocations(cap=catalog.get_instance_capacities(instance_type=instance_type))
        node_map = tuple([self.arm.graph_id, 'HX6VQ53'])
        node_sliver.set_node_map(node_map=node_map)
        return node_sliver

    def get_control(self) -> ResourceControl:
        control = NetworkNodeControl()
        control.add_type(rtype=ResourceType(resource_type=NodeType.VM.name))
        return control

    def get_delegation(self, actor: ABCActorMixin) -> ABCDelegation:
        slice_object = SliceFactory.create(slice_id=ID(), name="inventory-slice")
        slice_object.set_inventory(value=True)
        actor.register_slice(slice_object=slice_object)

        delegation_name = 'primary'

        dlg_obj = DelegationFactory.create(did=self.adm.get_graph_id(), slice_id=slice_object.get_slice_id(),
                                           delegation_name=delegation_name)
        dlg_obj.set_slice_object(slice_object=slice_object)
        dlg_obj.set_graph(graph=self.adm)
        actor.register_delegation(delegation=dlg_obj)
        return dlg_obj

    def check_before_donate_delegation(self, authority: ABCAuthority):
        policy = authority.get_policy()
        control = self.get_control()
        self.assertIsNotNone(control)
        self.assertEqual(len(policy.delegations), 0)

    def check_after_donate_donate_delegation(self, authority: ABCAuthority):
        policy = authority.get_policy()
        control = self.get_control()

        self.assertIsNotNone(control)
        self.assertEqual(len(policy.delegations), 1)

    def make_actor_database(self) -> ABCDatabase:
        db = SubstrateActorDatabase(user=self.db_user, password=self.db_pwd, database=self.db_name, db_host=self.db_host,
                                    logger=self.logger)
        return db

    def make_plugin(self):
        handler_processor = AnsibleHandlerProcessor()
        plugin = SubstrateMixin(actor=None, db=None, handler_processor=handler_processor)
        return plugin

    def get_config_map(self):
        config_map = ConfigurationMapping()
        config_map.set_key(key=NodeType.VM.name)
        config_map.set_class_name(class_name='NoOpHandler')
        config_map.set_module_name(module_name='fabric_cf.actor.handlers.no_op_handler')
        return config_map

    def get_authority(self, name: str = BaseTestCase.authority_name, guid: ID = BaseTestCase.authority_guid):
        db = self.get_container_database()
        db.reset_db()
        authority = super().get_authority()
        authority.set_recovered(value=True)
        authority.set_aggregate_resource_model(aggregate_resource_model=self.arm)
        authority.get_plugin().handler_processor.add_config_mapping(mapping=self.get_config_map())
        Term.set_clock(authority.get_actor_clock())
        return authority

    def get_authority_policy(self) -> ABCAuthorityPolicy:
        policy = AuthorityCalendarPolicy()
        policy.register_control(control=self.get_control())
        return policy

    def get_ticket(self, units: int, rtype: ResourceType, term: Term, source: ABCDelegation,
                   actor: ABCActorMixin) -> Ticket:
        resource_ticket = ResourceTicketFactory.create(issuer=self.broker_guid,
                                                       units=units,
                                                       term=term,
                                                       rtype=rtype)
        ticket = Ticket(resource_ticket=resource_ticket, plugin=actor.get_plugin(), authority=None)
        ticket.delegation_id = source.get_delegation_id()
        return ticket

    def get_request_slice(self):
        return SliceFactory.create(slice_id=ID(), name="test-slice")

    def get_request(self, term: Term, ticket: Ticket, sliver: BaseSliver):
        rset = ResourceSet(units=1, rtype=ResourceType(resource_type=sliver.resource_type.name), sliver=sliver)
        rset.set_resources(cset=ticket)
        slice_object = self.get_request_slice()
        return AuthorityReservationFactory.create(resources=rset, term=term, slice_obj=slice_object, rid=ID())

    def get_request_from_request(self, request: ABCAuthorityReservation, term: Term, ticket: Ticket):
        rset = ResourceSet(units=1, rtype=request.get_type())
        rset.set_resources(cset=ticket)
        result = AuthorityReservationFactory.create(resources=rset, term=term, slice_obj=request.get_slice(),
                                                    rid=request.get_reservation_id())
        result.set_sequence_in(sequence=request.get_sequence_in() + 1)
        return result

    def get_redeem_request(self, authority: ABCAuthority, delegation: ABCDelegation):
        req_start = authority.get_actor_clock().cycle_start_date(cycle=self.TicketStartCycle)
        req_end = authority.get_actor_clock().cycle_end_date(cycle=self.TicketEndCycle)
        req_term = Term(start=req_start, end=req_end)
        sliver = self.build_sliver()
        ticket = self.get_ticket(units=self.TicketUnits, rtype=ResourceType(resource_type=sliver.resource_type.name),
                                 term=req_term, source=delegation,
                                 actor=authority)

        request = self.get_request(req_term, ticket, sliver)
        return request

    def get_extend_lease_request(self, authority: ABCAuthority, delegation: ABCDelegation,
                                 request: ABCAuthorityReservation):
        req_start = authority.get_actor_clock().cycle_start_date(cycle=self.TicketStartCycle)
        req_new_start = authority.get_actor_clock().cycle_start_date(cycle=self.TicketEndCycle + 1)
        req_end = authority.get_actor_clock().cycle_end_date(cycle=self.TicketNewEndCycle)
        req_term = Term(start=req_start, end=req_end, new_start=req_new_start)
        rtype = ResourceType(resource_type=request.get_requested_resources().get_sliver().resource_type.name)
        ticket = self.get_ticket(self.TicketUnits, rtype, req_term, delegation, authority)
        new_request = self.get_request_from_request(request, req_term, ticket)
        return new_request

    def check_incoming_lease(self, request: ABCAuthorityReservation, incoming: ABCReservationMixin):
        self.assertIsNotNone(incoming)

        rset = incoming.get_resources()

        self.assertIsNotNone(rset)

        self.assertEqual(request.get_requested_units(), rset.get_units())

        self.assertEqual(incoming.get_term(), request.get_requested_term())

        uset = rset.get_resources()
        self.assertIsNotNone(uset)
        self.assertEqual(self.TicketUnits, uset.get_units())

        u = uset.get_set().values().__iter__().__next__()
        self.my_unit = u

    def check_incoming_close_lease(self, request: ABCAuthorityReservation, incoming: ABCReservationMixin):
        self.assertIsNotNone(incoming)

        rset = incoming.get_resources()

        self.assertIsNotNone(rset)

        self.assertEqual(request.get_requested_units(), rset.get_units())

        uset = rset.get_resources()
        self.assertIsNotNone(uset)
        self.assertEqual(0, uset.get_units())

    def check_incoming_extend_lease(self, request: ABCAuthorityReservation, incoming: ABCReservationMixin):
        self.assertIsNotNone(incoming)
        self.assertEqual(ReservationStates.Active, incoming.get_state())
        self.assertEqual(ReservationPendingStates.None_, incoming.get_pending_state())
        rset = incoming.get_resources()
        self.assertIsNotNone(rset)
        self.assertEqual(request.get_requested_units(), rset.get_units())
        self.assertEqual(incoming.get_term(), request.get_requested_term())

        uset = rset.get_resources()
        self.assertIsNotNone(uset)
        self.assertEqual(self.TicketUnits, uset.get_units())

        u = uset.get_set().values().__iter__().__next__()
        self.assertEqual(self.my_unit, u)

    def test_a_create(self):
        authority = self.get_authority()
        self.assertIsNotNone(authority)

    def test_b_donate(self):
        site = self.get_authority()
        policy = site.get_policy()
        self.check_before_donate_delegation(site)
        delegation = self.get_delegation(site)
        policy.donate_delegation(delegation=delegation)
        self.check_after_donate_donate_delegation(site)

    def test_c_redeem(self):
        site = self.get_authority()
        policy = site.get_policy()
        controller = self.get_controller()
        proxy = ControllerCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        authority_proxy = ControllerCallbackHelper(name=site.get_name(), guid=site.get_guid())

        ActorRegistrySingleton.get().register_callback(callback=proxy)
        ActorRegistrySingleton.get().register_callback(callback=authority_proxy)

        delegation = self.get_delegation(site)
        policy.donate_delegation(delegation=delegation)

        request = self.get_redeem_request(site, delegation)

        class UpdateLeaseHandler(IUpdateLeaseHandler):
            def __init__(self, parent, request):
                self.waiting_for_lease = True
                self.waiting_for_close = False
                self.parent = parent
                self.request = request

            def handle_update_lease(self, reservation: ABCReservationMixin, update_data: UpdateData, caller: AuthToken):
                if self.waiting_for_lease:
                    self.parent.check_incoming_lease(self.request, reservation)
                    self.waiting_for_lease = False
                    self.waiting_for_close = True
                elif self.waiting_for_close:
                    self.parent.assertTrue(site.get_current_cycle() >= AuthorityCalendarPolicyTest.TicketEndCycle)
                    self.parent.check_incoming_close_lease(self.request, reservation)
                    self.waiting_for_close = False
                else:
                    raise AuthorityException(Constants.INVALID_STATE)

            def check_termination(self):
                self.parent.assertFalse(self.waiting_for_lease)
                self.parent.assertFalse(self.waiting_for_close)

        handler = UpdateLeaseHandler(self, request)
        proxy.set_update_lease_handler(handler=handler)

        cycle = 1
        site.external_tick(cycle=cycle)
        cycle += 1

        print("Redeeming request...")
        # Force delegation to be in Delegated State
        delegation.state = DelegationState.Delegated
        site.redeem(reservation=request, callback=proxy, caller=proxy.get_identity())

        for c in range(cycle, self.DonateEndCycle):
            site.external_tick(cycle=c)
            while site.get_current_cycle() != c:
                time.sleep(0.001)

        handler.check_termination()

    def test_d_extend_lease(self):
        site = self.get_authority()
        policy = site.get_policy()
        controller = self.get_controller()
        proxy = ControllerCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        auth_proxy = ControllerCallbackHelper(name=site.get_name(), guid=site.get_guid())
        ActorRegistrySingleton.get().register_callback(callback=proxy)
        ActorRegistrySingleton.get().register_callback(callback=auth_proxy)

        delegation = self.get_delegation(site)
        policy.donate_delegation(delegation=delegation)

        request = self.get_redeem_request(site, delegation)

        extend = self.get_extend_lease_request(site, delegation, request)

        class UpdateLeaseHandler(IUpdateLeaseHandler):
            def __init__(self, parent, request):
                self.waiting_for_extend_lease = False
                self.waiting_for_lease = True
                self.waiting_for_close = False
                self.parent = parent
                self.request = request

            def handle_update_lease(self, reservation: ABCReservationMixin, update_data: UpdateData, caller: AuthToken):
                if self.waiting_for_lease:
                    self.parent.check_incoming_lease(self.request, reservation)
                    self.waiting_for_lease = False
                    self.waiting_for_close = True
                elif self.waiting_for_close:
                    self.parent.assertTrue(site.get_current_cycle() >= AuthorityCalendarPolicyTest.TicketEndCycle)
                    self.parent.check_incoming_close_lease(self.request, reservation)
                    self.waiting_for_close = False
                else:
                    raise AuthorityException(Constants.INVALID_STATE)

            def check_termination(self):
                self.parent.assertFalse(self.waiting_for_lease)
                self.parent.assertFalse(self.waiting_for_close)

        handler = UpdateLeaseHandler(self, request)

        proxy.set_update_lease_handler(handler=handler)
        cycle = 1
        site.external_tick(cycle=cycle)
        cycle += 1
        print("Redeeming request")
        # Force delegation to be in Delegated State
        delegation.state = DelegationState.Delegated

        site.redeem(reservation=request, callback=proxy, caller=proxy.get_identity())
        for cycle in range(cycle, self.DonateEndCycle):
            if cycle == self.TicketEndCycle - 50:
                print("Extending Lease")
                site.extend_lease(reservation=extend, caller=proxy.get_identity())
            site.external_tick(cycle=cycle)
            while site.get_current_cycle() != cycle:
                time.sleep(0.001)
        handler.check_termination()

    def test_e_close(self):
        site = self.get_authority()
        policy = site.get_policy()
        controller = self.get_controller()
        proxy = ControllerCallbackHelper(name=controller.get_name(), guid=controller.get_guid())
        authority_proxy = ControllerCallbackHelper(name=site.get_name(), guid=site.get_guid())

        ActorRegistrySingleton.get().register_callback(callback=proxy)
        ActorRegistrySingleton.get().register_callback(callback=authority_proxy)

        delegation = self.get_delegation(site)
        policy.donate_delegation(delegation=delegation)

        request = self.get_redeem_request(site, delegation)

        class UpdateLeaseHandler(IUpdateLeaseHandler):
            def __init__(self, parent):
                self.waiting_for_lease = True
                self.waiting_for_close = False
                self.parent = parent

            def handle_update_lease(self, reservation: ABCReservationMixin, update_data: UpdateData, caller: AuthToken):
                if self.waiting_for_lease:
                    self.parent.check_incoming_lease(request=request, incoming=reservation)
                    self.waiting_for_lease = False
                    self.waiting_for_close = True
                elif self.waiting_for_close:
                    print(site.get_current_cycle())
                    self.parent.assertTrue(site.get_current_cycle() < AuthorityCalendarPolicyTest.TicketEndCycle)
                    self.parent.check_incoming_close_lease(request=request, incoming=reservation)
                    self.waiting_for_close = False
                else:
                    raise AuthorityException(Constants.INVALID_STATE)

            def check_termination(self):
                self.parent.assertFalse(self.waiting_for_lease)
                self.parent.assertFalse(self.waiting_for_close)

        handler = UpdateLeaseHandler(self)
        proxy.set_update_lease_handler(handler=handler)

        cycle = 1
        site.external_tick(cycle=cycle)
        cycle += 1
        print("Redeeming request...")
        # Force delegation to be in Delegated State
        delegation.state = DelegationState.Delegated
        site.redeem(reservation=request, callback=proxy, caller=proxy.get_identity())

        for cycle in range(cycle, self.DonateEndCycle):
            if cycle == self.TicketEndCycle - 5:
                site.close(reservation=request)
            site.external_tick(cycle=cycle)
            while site.get_current_cycle() != cycle:
                time.sleep(0.001)

        handler.check_termination()