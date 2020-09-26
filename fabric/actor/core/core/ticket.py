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
import pickle
from datetime import datetime

from fabric.actor.core.apis.i_base_plugin import IBasePlugin
from fabric.actor.core.apis.i_authority_proxy import IAuthorityProxy
from fabric.actor.core.apis.i_concrete_set import IConcreteSet
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.delegation.resource_ticket import ResourceTicket
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.notice import Notice
from fabric.actor.core.util.resource_type import ResourceType


class Ticket(IConcreteSet):
    """
    Ticket is an IConcreteSet implementation that wraps a ResourceTicket for use inside of a ResourceSet
    """
    PropertyTicketAuthorityProxy = "ticket.authority.proxy"
    PropertyTicketResourceTicket = "ticket.resourceTicket"

    def __init__(self, *, ticket: ResourceTicket = None, plugin: IBasePlugin = None, authority: IAuthorityProxy = None):
        # The plugin object
        self.plugin = plugin
        self.logger = plugin.get_logger()
        # The authority who owns the resources described in this concrete set
        self.authority = authority
        # The encapsulated resource ticket.
        self.resource_ticket = ticket
        # Units we used to have before the current extend
        self.old_units = 0
        # The reservation this ticket belongs to
        self.reservation = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['plugin']
        del state['logger']
        del state['reservation']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.plugin = None
        self.logger = None
        self.reservation = None

        # TODO Fetch reservation object and setup logger, reservation and plugin variables

    def __str__(self):
        result = "Ticket [units = {} oldUnits = {} ".format(self.get_units(), self.old_units)
        if self.reservation is not None:
            slice_obj = self.reservation.get_slice()
            if slice is None:
                self.logger.error("reservation inside ticket has no slice")
            else:
                result += " Slice="
                result += slice_obj.get_name()
        result += "]"
        return result

    def restore(self, *,  plugin: IBasePlugin, reservation: IReservation):
        self.plugin = plugin
        self.logger = self.plugin.get_logger()
        self.reservation = reservation

    def encode(self, *, protocol: str):
        try:
            encoded_ticket = pickle.dumps(self)
            return encoded_ticket
        except Exception as e:
            self.logger.error("Exception occurred while encoding {}".format(e))
        return None

    def decode(self, *, encoded_ticket, plugin: IBasePlugin):
        try:
            ticket = pickle.loads(encoded_ticket)
            ticket.plugin = plugin
            ticket.logger = plugin.get_logger()
        except Exception as e:
            self.logger.error("Exception occurred while decoding {}".format(e))
        return None

    def get_type(self) -> ResourceType:
        if self.resource_ticket is None:
            return None
        return self.resource_ticket.get_type()

    def get_ticket(self) -> ResourceTicket:
        return self.resource_ticket

    def add(self, *, concrete_set, configure: bool):
        raise Exception("add() is not supported by Ticket")

    def change(self, *, concrete_set: IConcreteSet, configure: bool):
        self.old_units = self.get_units()

        if not isinstance(concrete_set, Ticket):
            raise Exception("Invalid concrete_set type")

        assert concrete_set.resource_ticket is not None

        # TODO
        self.resource_ticket = self.plugin.get_ticket_factory().clone(original=concrete_set.resource_ticket)

    def clone(self):
        result = Ticket(ticket=self.resource_ticket, plugin=self.plugin, authority=self.authority)
        result.old_units = self.old_units
        return result

    def clone_empty(self):
        result = Ticket(ticket=self.resource_ticket, plugin=self.plugin, authority=self.authority)
        result.old_units = self.old_units
        return result

    def close(self):
        return

    def collect_released(self):
        return

    def get_notices(self) -> Notice:
        return None

    def get_properties(self) -> dict:
        """
        Returns the ticket properties.
        @returns ticket properties
        """
        if self.resource_ticket is None:
            return None
        return self.resource_ticket.get_properties()

    def get_authority(self) -> IAuthorityProxy:
        return self.authority

    def get_plugin(self) -> IBasePlugin:
        """
        Returns Actor Plugin
        @returns actor plugin
        """
        return self.plugin

    def get_site_proxy(self) -> IAuthorityProxy:
        return self.authority

    def get_term(self) -> Term:
        if self.resource_ticket is None:
            return None
        return self.resource_ticket.get_term()

    def holding(self, *, when: datetime) -> int:
        if when is None:
            raise Exception("InvalidArgument")

        term = self.get_term()
        if term is None:
            return 0

        if when < term.get_new_start_time():
            if when < term.get_start_time():
                # date is before start time
                return 0
            else:
                # date is in [start, newStart)
                return self.old_units
        else:
            if when > term.get_end_time():
                # date is after end time
                return 0
            else:
                # date is in [newStart,end]
                return self.get_units()

    def is_active(self):
        # valid tickets are always active, if anyone asks
        return True

    def modify(self, *, concrete_set, configure: bool):
        raise Exception("Not supported by TicketSet")

    def probe(self):
        return

    def remove(self, *, concrete_set, configure: bool):
        raise Exception("Not supported by TicketSet")

    def setup(self, *, reservation: IReservation):
        """
        Indicates that we're committing resources to a client (on an an agent).
        May need to touch TicketSet database since we're committing it. On a
        client (orchestrator) this indicates that we have successfully scored
        a ticket. The ticket has already been validated with validate().
        @params reservation the slice for the reservation
        """
        self.reservation = reservation

    def validate_concrete(self, *, rtype: ResourceType, units: int, term: Term):
        if self.get_units() < units:
            raise Exception("Ticket not valid for requested units")

    def validate_incoming(self):
        return

    def validate_outgoing(self):
        return

    def restart_actions(self):
        return

    def get_units(self) -> int:
        if self.resource_ticket is None:
            return 0
        return self.resource_ticket.get_units()
