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
from fabric.actor.core.apis.i_authority import IAuthority
from fabric.actor.core.apis.i_authority_policy import IAuthorityPolicy
from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric.actor.core.apis.i_client_reservation import IClientReservation
from fabric.actor.core.apis.i_delegation import IDelegation
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.core.policy import Policy
from fabric.actor.core.core.ticket import Ticket
from fabric.actor.core.delegation.resource_delegation import ResourceDelegation
from fabric.actor.core.kernel.resource_set import ResourceSet
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_data import ResourceData


class AuthorityPolicy(Policy, IAuthorityPolicy):
    PropertySourceTicket = "sourceTicket"

    """
    Base class for all authority policy implementations.
    """
    def __init__(self, *, actor: IAuthority = None):
        super().__init__(actor=actor)

        self.tickets = None
        self.initialized = False
        self.delegations = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']
        del state['tickets']
        del state['delegations']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.actor = None
        self.clock = None
        self.initialized = False
        self.tickets = None
        self.delegations = None

    def initialize(self):
        if not self.initialized:
            super().initialize()
            self.tickets = {}
            self.initialized = True
            self.delegations = {}

    def revisit(self, *, reservation: IReservation):
        if isinstance(reservation, IClientReservation):
            self.donate_reservation(reservation=reservation)

    def revisit_delegation(self, *, delegation: IDelegation):
        self.donate_delegation(delegation=delegation)

    def donate_delegation(self, *, delegation: IDelegation):
        self.delegations[delegation.get_delegation_id()] = delegation
        # TODO need to determine how to populate controls and tickets

    def donate(self, *, resources: ResourceSet):
        return

    def donate_reservation(self, *, reservation: IClientReservation):
        rset = reservation.get_resources()
        rset.set_reservation_id(rid=reservation.get_reservation_id())

        self.logger.info("AuthorityPolicy donateTicket {}".format(rset))

        ticket_set = self.tickets.get(rset.get_type(), None)

        if ticket_set is None:
            ticket_set = set()

        ticket_set.add(rset)

        self.logger.debug("Adding tickets {} for type: {}".format(len(ticket_set), rset.get_type()))
        self.tickets[rset.get_type()] = ticket_set

    def eject(self, *, resources: ResourceSet):
        return

    def failed(self, *, resources: ResourceSet):
        return

    def unavailable(self, *, resources: ResourceSet) -> int:
        return 0

    def available(self, *, resources: ResourceSet):
        return

    def freed(self, *, resources: ResourceSet):
        return

    def recovered(self, *, resources: ResourceSet):
        return

    def release(self, *, resources: ResourceSet):
        return

    def bind_delegation(self, *, delegation: IDelegation) -> bool:
        result = False

        if delegation.get_delegation_id() not in self.delegations:
            self.delegations[delegation.get_delegation_id()] = delegation
            result = True

        return result

    def bind(self, *, reservation: IBrokerReservation) -> bool:
        requested = reservation.get_requested_resources()
        ticket_set = self.tickets.get(requested.get_type(), None)
        if ticket_set is None or len(ticket_set) == 0:
            self.error(message="bindTicket: no tickets available for the requested resource type {}".format(
                requested.get_type()))

        #  We need a calendar to tell us what we have exported from each source
        #  ticket. For now, just take the first element from the set and export
        #  from it
        #  Also: it should be possible to specify a source ticket to use for a
        #  given export. This can be a property on the AgentReservation (say:
        #  slice and reservation id to use). Have to make sure that the resource set has
        #  the slice and the reservation id.
        rid = None
        if reservation.get_requested_resources().get_request_properties() is not None:
            temp = reservation.get_requested_resources().get_request_properties().get(self.PropertySourceTicket, None)

            if temp is not None:
                rid = ID(id=temp)

        ticket_found = None
        if rid is not None:
            for ticket in ticket_set:
                if ticket.get_reservation_id() is not None and ticket.get_reservation_id() == rid:
                    ticket_found = ticket

        else:
            if ticket_set is not None:
                ticket_found = ticket_set.__iter__().__next__()

        if ticket_found is None:
            self.error(message="bindticket: no tickets available")

        # If this is an elastic export, whittle it down to size if necessary.
        # Signal obvious export errors. This code is fragile/temporary: it
        # assumes that the request is for "now", and that there is just one
        # export per resource type. Other errors involving multiple exports per
        # type will be caught in the extract below, but we won't have a chance
        # to adjust if the request is elastic.
        units = ticket_found.get_resources().get_units()
        needed = requested.get_units()

        if needed > units:
            self.logger.error("insufficient units available to export to agent")
            needed = units

        delegation = self.actor.get_plugin().get_ticket_factory().make_delegation(
            units=needed, term=reservation.get_requested_term(), rtype=requested.get_type(),
            holder=reservation.get_client_auth_token().get_guid())

        mine = self.extract(source=ticket_found, delegation=delegation)
        reservation.set_approved(term=reservation.get_requested_term(), approved_resources=mine)
        return True

    def allocate(self, *, cycle: int):
        return

    def assign(self, *, cycle: int):
        return

    def correct_deficit(self, *, reservation: IAuthorityReservation):
        if reservation.get_resources() is None:
            return

        self.finish_correct_deficit(rset=None, reservation=reservation)

    def finish_correct_deficit(self, *, rset: ResourceSet, reservation: IAuthorityReservation):
        # We could have a partial set if there's a shortage. Go ahead and
        # install it: we'll come back later for the rest if we return a null
        # term. Alternatively, we could release them and throw an error.
        if rset is None:
            self.log_warn(message="we either do not have resources to satisfy the request or "
                                  "the reservation has/will have a pending operation")
            return

        if rset.is_empty():
            reservation.set_pending_recover(pending_recover=False)
        else:
            reservation.get_resources().update(reservation=reservation, resource_set=rset)

    def extend_authority(self, *, reservation: IAuthorityReservation) -> bool:
        return False

    def extend_broker(self, *, reservation: IBrokerReservation) -> bool:
        return False

    def extend(self, *, reservation: IReservation, resources: ResourceSet, term: Term):
        return False

    def extract(self, *, source: ResourceSet, delegation: ResourceDelegation):
        if source is None or delegation is None:
            self.error(message="Invalid Argument")

        self.logger.debug("extracting delegation: {}".format(delegation))

        rd = ResourceData()
        rd.resource_properties = source.get_resource_properties()
        extracted = ResourceSet(units=delegation.get_units(), rtype=delegation.get_resource_type(), rdata=rd)
        source_ticket = source.get_resources().get_ticket()

        new_ticket = self.actor.get_plugin().get_ticket_factory().make_ticket(delegation=delegation,
                                                                              source=source_ticket)

        cset = Ticket(ticket=new_ticket, plugin=source.get_resources().plugin,
                      authority=source.get_resources().authority)
        extracted.set_resources(cset=cset)
        return extracted
