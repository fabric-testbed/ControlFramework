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

from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin
from fabric_cf.actor.core.apis.abc_authority_proxy import ABCAuthorityProxy
from fabric_cf.actor.core.apis.abc_concrete_set import ABCConcreteSet
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import TicketException
from fabric_cf.actor.core.delegation.resource_ticket import ResourceTicket
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.notice import Notice
from fabric_cf.actor.core.util.resource_type import ResourceType


class Ticket(ABCConcreteSet):
    """
    Ticket is an IConcreteSet implementation that wraps a ResourceDelegation for use inside of a ResourceSet
    """

    def __init__(self, *, resource_ticket: ResourceTicket = None, plugin: ABCBasePlugin = None,
                 authority: ABCAuthorityProxy = None, delegation_id: str = None):
        # Persistent fields
        # The encapsulated resource ticket.
        self.resource_ticket = resource_ticket
        # Units we used to have before the current extend
        self.old_units = 0
        # The delegation from which this ticket was issued
        self.delegation_id = delegation_id

        # Non persistent fields
        # The plugin object
        self.plugin = plugin
        self.logger = None
        if plugin is not None:
            self.logger = plugin.get_logger()
        # The authority who owns the resources described in this concrete set
        self.authority = authority
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

    def __str__(self):
        result = f"Ticket [delegation_id= {self.delegation_id} units = {self.get_units()} oldUnits = {self.old_units} "
        if self.reservation is not None:
            slice_obj = self.reservation.get_slice()
            if slice is None:
                self.logger.error("reservation inside ticket has no slice")
            else:
                result += f" Slice={slice_obj.get_name()}"
        result += "]"
        return result

    def restore(self, *, plugin: ABCBasePlugin, reservation: ABCReservationMixin):
        """
        Restore members after instantiating the object post database read
        @param plugin plugin
        @param reservation reservation
        """
        self.plugin = plugin
        self.logger = self.plugin.get_logger()
        self.reservation = reservation

    def get_type(self) -> ResourceType:
        """
        Return resource type
        @return resource type
        """
        if self.resource_ticket is None:
            return None
        return self.resource_ticket.get_type()

    def get_ticket(self) -> ResourceTicket:
        """
        Return resource delegation
        @return resource delegation
        """
        return self.resource_ticket

    def add(self, *, concrete_set, configure: bool):
        raise TicketException("add() is not supported by Ticket")

    def change(self, *, concrete_set: ABCConcreteSet, configure: bool):
        self.old_units = self.get_units()

        if not isinstance(concrete_set, Ticket):
            raise TicketException(Constants.INVALID_ARGUMENT)

        assert concrete_set.resource_ticket is not None

        self.resource_ticket = concrete_set.resource_ticket.clone()

    def _clone(self):
        result = Ticket(resource_ticket=self.resource_ticket, plugin=self.plugin, authority=self.authority,
                        delegation_id=self.delegation_id)
        result.old_units = self.old_units
        return result

    def clone(self):
        return self._clone()

    def clone_empty(self):
        return self._clone()

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

    def get_plugin(self) -> ABCBasePlugin:
        """
        Returns Actor Plugin
        @returns actor plugin
        """
        return self.plugin

    def get_site_proxy(self) -> ABCAuthorityProxy:
        """
        Return corresponding Authority
        @return authority
        """
        return self.authority

    def get_term(self) -> Term:
        """
        Get term
        :return: term
        """
        if self.resource_ticket is None:
            return None
        return self.resource_ticket.get_term()

    def holding(self, *, when: datetime) -> int:
        if when is None:
            raise TicketException(Constants.INVALID_ARGUMENT)

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
        raise TicketException("Not supported by TicketSet")

    def probe(self):
        return

    def remove(self, *, concrete_set, configure: bool):
        raise TicketException("Not supported by TicketSet")

    def setup(self, *, reservation: ABCReservationMixin):
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
            raise TicketException("Ticket not valid for requested units")

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

    def get_delegation_id(self) -> str:
        """
        Get Delegation Id
        :return: delegation id
        """
        return self.delegation_id

    def set_delegation_id(self, *, delegation_id: str):
        """
        Set delgation id
        :param delegation_id: delegation id
        :return:
        """
        self.delegation_id = delegation_id
