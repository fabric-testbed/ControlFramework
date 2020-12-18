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
import unittest
from datetime import datetime
import time
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.delegation.resource_bin import ResourceBin
from fabric_cf.actor.core.delegation.simple_resource_ticket_factory import SimpleResourceTicketFactory
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.test.base_test_case import BaseTestCase


class SimpleTicketTest(BaseTestCase, unittest.TestCase):
    from fabric_cf.actor.core.container.globals import Globals
    Globals.config_file = "../../config/config.test.yaml"
    Constants.superblock_location = './state_recovery.lock'

    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().start(force_fresh=True)
    while not GlobalsSingleton.get().start_completed:
        time.sleep(0.0001)

    def setUp(self) -> None:
        self.factory = self.make_ticket_factory()
        self.now = datetime.utcnow()
        self.end = self.now.replace(day=1)
        self.term = Term(start=self.now, end=self.end)
        self.type = ResourceType(resource_type=str(ID()))
        self.units = 100
        self.create_factories()

    def make_ticket_factory(self) -> SimpleResourceTicketFactory:
        return SimpleResourceTicketFactory()

    def create_factories(self):
        actor = self.get_actor(name="site0", guid=ID())
        self.factory = self.make_ticket_factory()
        self.factory.set_actor(actor=actor)
        self.factory.initialize()

    def test_simple_ticket(self):
        d0 = self.factory.make_delegation(units=self.units, term=self.term, rtype=self.type)
        ticket0 = self.factory.make_ticket(delegation=d0)

        print(self.factory.to_json(ticket=ticket0))

    def test_simple_ticket_with_bins(self):
        bin0 = ResourceBin(physical_units=self.units, term=self.term)
        source_list = [bin0.get_guid()]
        bin_list = [bin0]
        d0 = self.factory.make_delegation(units=self.units, term=self.term, rtype=self.type, sources=source_list,
                                          bins=bin_list, holder=ID(uid=self.factory.get_actor().get_name()))
        ticket0 = self.factory.make_ticket(delegation=d0)

        print(self.factory.to_json(ticket=ticket0))

