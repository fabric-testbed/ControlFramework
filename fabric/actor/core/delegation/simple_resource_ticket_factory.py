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

from fabric.actor.core.apis.i_actor import IActor
from fabric.actor.core.common.resource_vector import ResourceVector
from fabric.actor.core.apis.i_resource_ticket_factory import IResourceTicketFactory
from fabric.actor.core.delegation.resource_bin import ResourceBin
from fabric.actor.core.delegation.resource_delegation import ResourceDelegation
from fabric.actor.core.delegation.resource_ticket import ResourceTicket
from fabric.actor.core.time.actor_clock import ActorClock
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_type import ResourceType


class SimpleResourceTicketFactory(IResourceTicketFactory):
    def __init__(self):
        self.actor = None
        self.initialized = False

    def initialize(self):
        if not self.initialized:
            if self.actor is None:
                raise Exception("Factory does not have an actor")
            self.initialized = True

    def ensure_initialized(self):
        if not self.initialized:
            raise Exception("ticket factory has not been initialized")

    def get_issuer_id(self):
        return self.actor.get_identity().get_guid()

    def make_delegation(self, *, units: int = None, vector: ResourceVector = None, term: Term = None,
                        rtype: ResourceType = None, sources: list = None, bins: list = None,
                        properties: dict = None, holder: ID = None) -> ResourceDelegation:

        self.ensure_initialized()
        if (sources is None and bins is not None) or (sources is not None and bins is None):
            raise Exception("sources and bins must both be null or non-null")

        issuer = self.get_issuer_id()

        return ResourceDelegation(units=units, vector=vector, term=term, rtype=rtype, sources=sources, bins=bins,
                                  properties=properties, issuer=issuer, holder=holder)

    def make_ticket(self, *, delegation: ResourceDelegation = None, source: ResourceTicket = None) -> ResourceTicket:
        self.ensure_initialized()
        return ResourceTicket(factory=self, delegation=delegation, source=source)

    def set_actor(self, *, actor: IActor):
        self.actor = actor

    def get_actor(self) -> IActor:
        return self.actor

    def clone(self, *, original: ResourceTicket) -> ResourceTicket:
        self.ensure_initialized()
        ticket_json = self.toJson(ticket=original)
        result = self.fromJson(incoming=ticket_json)
        return result

    def toJson(self, *, ticket: ResourceTicket) -> dict:
        outgoing_ticket = {}
        delegation_list = []
        for delegation in ticket.delegations:
            d = {'guid': delegation.get_guid()}
            t = {'start_time': ActorClock.to_milliseconds(when=delegation.get_term().get_start_time()),
                 'end_time': ActorClock.to_milliseconds(when=delegation.get_term().get_end_time()),
                 'new_start_time': ActorClock.to_milliseconds(when=delegation.get_term().get_new_start_time())}
            d['term'] = t
            d['units'] = delegation.get_units()
            d['type'] = delegation.get_resource_type()
            d['issuer'] = delegation.get_issuer()
            if delegation.get_holder():
                d['holder'] = delegation.get_holder()
            if delegation.sources is not None:
                source_list = []
                for s in delegation.sources:
                    val = {'id': s}
                    source_list.append(val)
                d['sources'] = source_list
            if delegation.bins is not None:
                bin_list = []
                for b in delegation.bins:
                    bin = {'guid': b.get_guid(), 'physical_units': b.get_physical_units()}

                    if b.get_parent_guid() is not None:
                        b['parent_guid'] = b.get_parent_guid()

                    term = {'start_time': ActorClock.to_milliseconds(when=b.get_term().get_start_time()),
                            'end_time': ActorClock.to_milliseconds(when=b.get_term().get_end_time()),
                            'new_start_time': ActorClock.to_milliseconds(when=b.get_term().get_new_start_time())}

                    bin['term'] = term
                    bin_list.append(bin)
                d['bins'] = bin_list

            delegation_list.append(d)
        outgoing_ticket['delegations'] = delegation_list
        return {'ticket': outgoing_ticket}

    def fromJson(self, *, incoming: dict) -> ResourceTicket:
        ticket = None

        incoming_ticket = incoming.get('ticket', None)
        if incoming_ticket is not None:
            delegations = incoming_ticket.get('delegations', None)
            if delegations is not None:
                for d in delegations:
                    dd = ResourceDelegation()
                    dd.guid = d.get('guid', None)
                    term_dict = d.get('term', None)
                    if term_dict is not None:
                        start_time = term_dict.get('start_time', None)
                        end_time = term_dict.get('end_time', None)
                        new_start_time = term_dict.get('new_start_time', None)
                        term = Term(start=ActorClock.from_milliseconds(milli_seconds=start_time),
                                    end=ActorClock.from_milliseconds(milli_seconds=end_time),
                                    new_start=ActorClock.from_milliseconds(milli_seconds=new_start_time))
                        dd.term = term
                    dd.units = d.get('units', 0)
                    dd.type = d.get('type', None)
                    dd.issuer = d.get('issuer', None)
                    dd.holder = d.get('holder', None)
                    sources = d.get('sources', None)
                    if sources is not None:
                        dd.sources = []
                        for s in sources:
                            if s.get('id', None) is not None:
                                dd.sources.append(s['id'])
                    bins = d.get('bins', None)
                    if bins is not None:
                        dd.bins = []
                        for b in bins:
                            bb = ResourceBin()
                            bb.guid = b.get('guid', None)
                            bb.physical_units = b.get('physical_units', 0)
                            bb.parent_guid = b.get('parent_guid', None)
                            term_dict = b.get('term', None)
                            if term_dict is not None:
                                start_time = term_dict.get('start_time', None)
                                end_time = term_dict.get('end_time', None)
                                new_start_time = term_dict.get('new_start_time', None)
                                term = Term(start=ActorClock.from_milliseconds(milli_seconds=start_time),
                                            end=ActorClock.from_milliseconds(milli_seconds=end_time),
                                            new_start=ActorClock.from_milliseconds(milli_seconds=new_start_time))
                                bb.term = term
                            dd.bins.append(bb)
                    if ticket is None:
                        ticket = ResourceTicket(factory=self, delegation=dd, source=None)
                    else:
                        ticket.delegations.append(dd)

        return ticket


