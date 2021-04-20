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
from fabric_cf.actor.core.apis.abc_concrete_set import ABCConcreteSet
from fabric_cf.actor.core.core.controller import Controller
from fabric_cf.actor.core.core.ticket import Ticket
from fabric_cf.actor.core.delegation.resource_ticket import ResourceTicketFactory
from fabric_cf.actor.core.kernel.reservation_client import ReservationClient
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType


class ControllerTestWrapper(Controller):
    broker_guid = ID()
    def bid(self):
        candidates = self.policy.formulate_bids(cycle=self.current_cycle)
        if candidates is not None:
            ticketing = candidates.get_ticketing()
            if ticketing is not None:
                for r in ticketing.values():
                    print("cycle: {} Ticket request for: {}".format(self.current_cycle, r))

                    if r.get_slice_name().startswith("fail"):
                        already_failed = False
                        slice_obj = r.get_slice()
                        for slice_reservation in slice_obj.get_reservations().values():
                            if slice_reservation.get_state() == ReservationStates.Failed:
                                already_failed = True
                                break
                        if not already_failed:
                            self.fail_ticket(r)
                            continue

                    ticket = ResourceTicketFactory.create(issuer=self.broker_guid,
                                                          units=r.get_approved_resources().get_units(),
                                                          term=r.get_approved_term(),
                                                          rtype=r.get_approved_type())
                    cs = Ticket(resource_ticket=ticket, plugin=self.get_plugin())
                    self.update_ticket_wrapper(r, r.get_approved_type(), r.get_approved_units(), cs,
                                               r.get_approved_term())

            extending = candidates.get_extending()
            if extending is not None:
                for r in extending.values():
                    print("cycle: {} Extend Ticket request for: {}".format(self.current_cycle, r))
                    ticket = ResourceTicketFactory.create(issuer=self.broker_guid,
                                                          units=r.get_approved_resources().get_units(),
                                                          term=r.get_approved_term(),
                                                          rtype=r.get_approved_type())
                    cs = Ticket(resource_ticket=ticket, plugin=self.get_plugin())
                    self.update_ticket_wrapper(r, r.get_approved_type(), r.get_approved_units(), cs,
                                               r.get_approved_term())

    def close_expiring(self):
        rset = self.policy.get_closing(cycle=self.current_cycle)
        if rset is not None:
            for r in rset.values():
                print("cycle: {} closing reservation r: {}".format(self.current_cycle, r))
                r.transition(prefix="close", state=ReservationStates.Closed, pending=ReservationPendingStates.None_)

    def process_redeeming(self):
        rset = self.policy.get_redeeming(cycle=self.current_cycle)
        if rset is not None:
            for r in rset.values():
                if r.get_state() == ReservationStates.Ticketed:
                    print("cycle: {} redeeming reservation r: {}".format(self.current_cycle, r))
                else:
                    print("cycle: {} extending lease for reservation r: {}".format(self.current_cycle, r))

                ticket = ResourceTicketFactory.create(issuer=self.broker_guid,
                                                      units=r.resources.get_units(),
                                                      term=r.term,
                                                      rtype=r.resources.get_type())
                cs = Ticket(resource_ticket=ticket, plugin=self.get_plugin())
                self.update_lease_wrapper(r, r.get_approved_type(), r.get_approved_units(), cs, r.get_approved_term())

    def update_lease_wrapper(self, reservation: ReservationClient, rtype: ResourceType, units: int, cs: ABCConcreteSet, term: Term):
        if reservation.state == ReservationStates.Ticketed:
            reservation.leased_resources = reservation.resources.abstract_clone()
            reservation.leased_resources.units = units
            reservation.leased_resources.type = rtype
            reservation.leased_resources.set_resources(cset=cs)

            reservation.previous_lease_term = None
            reservation.previous_term = reservation.term
            reservation.lease_term = term.clone()
            reservation.term = reservation.lease_term

            reservation.transition(prefix="redeem", state=ReservationStates.Active, pending=ReservationPendingStates.None_)
        else:
            reservation.leased_resources.units = units
            reservation.leased_resources.type = rtype
            reservation.leased_resources.resources.change(concrete_set=cs, configure=False)

            reservation.previous_lease_term = reservation.requested_term
            reservation.previous_term = reservation.term
            reservation.lease_term = term.clone()
            reservation.term = reservation.lease_term

            reservation.transition(prefix="redeem", state=ReservationStates.Active, pending=ReservationPendingStates.None_)

    def update_ticket_wrapper(self, reservation: ReservationClient, rtype: ResourceType, units: int,
                              ticket: Ticket, term: Term):
        if reservation.state == ReservationStates.Nascent:
            reservation.resources = reservation.get_approved_resources().abstract_clone()
            reservation.resources.units = units
            reservation.resources.type = rtype
            reservation.resources.set_resources(cset=ticket)

            reservation.previous_ticket_term = None
            reservation.previous_term = None
            reservation.term = term.clone()
            reservation.ticket_term = reservation.term

            reservation.transition(prefix="ticket", state=ReservationStates.Ticketed, pending=ReservationPendingStates.None_)
        else:
            reservation.resources.units = units
            reservation.resources.type = rtype
            reservation.resources.resources.change(concrete_set=ticket, configure=False)

            reservation.previous_term = reservation.term
            reservation.previous_ticket_term = reservation.ticket_term
            reservation.term = term.clone()
            reservation.ticket_term = reservation.term

            reservation.transition(prefix="extendticket", state=ReservationStates.ActiveTicketed, pending=ReservationPendingStates.None_)

    def fail_ticket(self, r: ReservationClient):
        r.transition(prefix="fail", state=ReservationStates.Failed, pending=ReservationPendingStates.None_)