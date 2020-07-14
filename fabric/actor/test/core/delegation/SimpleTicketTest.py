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

from fabric.actor.core.delegation.ResourceBin import ResourceBin
from fabric.actor.core.delegation.SimpleResourceTicketFactory import SimpleResourceTicketFactory
from fabric.actor.core.time.Term import Term
from fabric.actor.core.util.ID import ID
from fabric.actor.core.util.ResourceType import ResourceType
from fabric.actor.test.BaseTestCase import BaseTestCase


class SimpleTicketTest(BaseTestCase, unittest.TestCase):

    def setUp(self) -> None:
        self.factory = self.make_ticket_factory()
        self.now = datetime.utcnow()
        self.end = (self.now.timestamp() * 1000) + (1000 * 60 * 60 *24)
        self.term = Term(start=self.now, end=datetime.fromtimestamp(self.end))
        self.type = ResourceType(str(ID()))
        self.units = 100
        self.create_factories()

    def make_ticket_factory(self) -> SimpleResourceTicketFactory:
        return SimpleResourceTicketFactory()

    def create_factories(self):
        actor = self.get_actor("site0", ID())
        self.factory = self.make_ticket_factory()
        self.factory.set_actor(actor)
        self.factory.initialize()

    def test_simple_ticket(self):
        d0 = self.factory.make_delegation(units=self.units, term=self.term, rtype=self.type)
        ticket0 = self.factory.make_ticket(delegation=d0)

        print(self.factory.toJson(ticket0))

    def test_simple_ticket_with_bins(self):
        bin0 = ResourceBin(physical_units=self.units, term=self.term)
        source_list = [bin0.get_guid()]
        bin_list = [bin0]
        d0 = self.factory.make_delegation(units=self.units, term=self.term, rtype=self.type, sources=source_list,
                                          bins=bin_list, holder=ID(self.factory.get_actor().get_name()))
        ticket0 = self.factory.make_ticket(delegation=d0)

        print(self.factory.toJson(ticket0))

