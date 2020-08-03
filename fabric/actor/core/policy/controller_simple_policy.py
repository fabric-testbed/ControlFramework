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
from fabric.actor.core.apis.i_client_reservation import IClientReservation
from fabric.actor.core.core.properties_manager import PropertiesManager
from fabric.actor.core.policy.broker_simple_policy import BrokerSimplePolicy
from fabric.actor.core.policy.controller_calendar_policy import ControllerCalendarPolicy
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.bids import Bids
from fabric.actor.core.util.reservation_set import ReservationSet


class ControllerSimplePolicy(ControllerCalendarPolicy):
    """
    A simple implementation of a Controller policy. This version makes the following assumptions:
    - 1 broker
    - Always bid on only the first open auction
    - Extend all reservations expiring at bidding time, if renewable
    - Close - ADVANCE_CLOSE cycles early
    """
    # The amount of time over specific policy decisions the Controller must add when
    # communicating with other actors (e.g. redeem() and renew()). Clock skew
    # must be at least one if the Controller is ticked after the agent and/or authority
    # At some point in time we may want this to not be static and learn it from
    # what we see in the system, but for now it is static.
    CLOCK_SKEW = 1
    # How far in advance a reservation should initiate the close. This allows
    # for the Controller to property close its reservation before the authority does a
    # close on its behalf, which would eliminate any state the Controller needs to save.
    ADVANCE_CLOSE = 1

    def __init__(self):
        super().__init__()

    def __getstate__(self):
        state = self.__dict__.copy()
        state['actor_id'] = self.actor.get_reference()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']
        del state['pending_notify']
        del state['lazy_close']

        return state

    def __setstate__(self, state):
        actor_id = state['actor_id']
        # TODO recover actor
        del state['actor_id']
        self.__dict__.update(state)

        # TODO Fetch Actor object and setup logger, actor and clock member variables

    def formulate_bids(self, cycle: int) -> Bids:
        """
        Form bids for expiring reservations and new demands. Return sets of reservations for new bids and renewals.
        formulateBids is unlocked on Controller. Note that a bidding policy never changes
        the state of reservations, except to suggest terms and brokers.
        It just returns sets of reservations ready to bid for and extend.
        It also never changes the membership of any reservation sets maintained by the server,
        although it does walk through them
        @params cycle: cycle
        @returns bids
        """
        extending = None
        bidding = None
        try:
            extending = self.process_renewing(cycle)
            # Select new reservations to bid, and bind to bid and term. Note:
            # here we issue all bids immediately. If we use a different policy,
            # it is our responsibility here to issue bids ahead of their
            # intended start cycles.
            bidding = self.process_demand(cycle)
            self.logger.debug("bidForSources: cycle {} bids {}".format(cycle, bidding.size()))
        except Exception as e:
            self.logger.error("an error in formulateBids:{}".format(e))

        return Bids(bidding, extending)

    def get_close(self, reservation: IClientReservation, term: Term) -> int:
        """
        Very simple policy - based on ADVANCE_CLOSE
        """
        if self.lazy_close:
            return -1
        else:
            end_cycle = self.actor.get_actor_clock().cycle(when=term.get_end_time())
            return end_cycle - self.ADVANCE_CLOSE

    def get_extend_term(self, suggested_term: Term, current_term: Term):
        """
        Returns the extension term for a reservation.
        @params suggested_term suggested term
        @params current_term current term
        @returns extension term
        @raises Exception in case of error
        """
        extend_term = None
        if suggested_term is not None:
            if suggested_term.extends_term(current_term):
                extend_term = suggested_term
            else:
                # extend the current term with the length of the term specified in suggested_term
                length = suggested_term.get_length()
                extend_term = current_term.extends_term(length)
        else:
            # Extend the term by its previous length
            extend_term = current_term.extend()
        return extend_term

    def get_redeem(self, reservation: IClientReservation) -> int:
        new_start = self.clock.cycle(when=reservation.get_term().get_new_start_time())
        result = new_start - self.CLOCK_SKEW
        if result < self.actor.get_current_cycle():
            result = self.actor.get_current_cycle()
        return result

    def get_renew(self, reservation: IClientReservation) -> int:
        """
        Call up to the agent to receive the advanceTime. Do time based on new_start so that requests are aligned.
        """
        new_start_cycle = self.actor.get_actor_clock().cycle(when=reservation.get_term().get_end_time()) + 1
        return new_start_cycle - BrokerSimplePolicy.ADVANCE_TIME - self.CLOCK_SKEW

    def prepare(self, cycle: int):
        try:
            self.check_pending()
        except Exception as e:
            self.logger.error("Exception in prepare:{}".format(e))

    def process_demand(self, cycle: int) -> ReservationSet:
        """
        For each newly requested reservation, assigns a term to request, and a broker to bid from.
        @param cycle cycle
        @return non-null set of new bids
        @throws Exception in case of error rare
        """
        outgoing = ReservationSet()
        demand = self.calendar.get_demand()
        if demand is None:
            return ReservationSet()

        for reservation in demand.values():
            kernel_slice = reservation.get_slice()
            for slice_reservation in kernel_slice.get_reservations().values():
                self.logger.trace("Reservation {} is in state: {}".format(slice_reservation.get_reservation_id(),
                                                                          slice_reservation.get_state().name))

        broker = self.actor.get_default_broker()
        for reservation in demand.values():
            if reservation.get_broker() is None:
                reservation.set_broker(broker)

            rset = reservation.get_suggested_resources()
            term = reservation.get_suggested_term()
            reservation.set_approved(term, rset)
            outgoing.add(reservation)
            self.calendar.add_pending(reservation)
            self.calendar.remove_demand(reservation)
        return outgoing

    def process_renewing(self, cycle: int) -> ReservationSet:
        """
        Returns a fresh ReservationSet of expiring reservations to try to renew
        in this bidding cycle, and suggest new terms for them.
        @param cycle cycle
        @return non-null set of renewals
        @throws Exception in case of error rare
        """
        result = ReservationSet()
        renewing = self.calendar.get_renewing(cycle)
        if renewing is None or len(renewing) == 0:
            return result

        self.logger.debug("Renewing = {}".format(len(renewing)))
        for reservation in renewing.values():
            self.logger.debug("Renewing res: {}".format(reservation))

            if reservation.is_renewable():
                self.logger.debug("Found a renewable reservation that needs an extension.")
                if reservation.is_closed() or reservation.is_closing() or reservation.is_failed():
                    self.logger.debug("Found a renewable reservation that is closing/closed/or failed")
                else:
                    suggested_term = reservation.get_suggested_term()
                    suggested_resources = reservation.get_suggested_resources()
                    current_term = reservation.get_term()
                    approved_resources = reservation.get_resources().abstract_clone()
                    approved_resources = PropertiesManager.set_elastic_time(approved_resources, False)

                    approved_term = self.get_extend_term(suggested_term, current_term)
                    if suggested_resources is not None:
                        approved_resources.set_units(suggested_resources.get_units())
                        approved_resources.set_type(suggested_resources.get_type())
                        approved_resources.get_resource_data().merge(suggested_resources.get_resource_data())
                        approved_resources = PropertiesManager.set_elastic_time(approved_resources, False)

                    reservation.set_approved(term=approved_term, approved_resources=approved_resources)
                    result.add(reservation)
                    self.calendar.add_pending(reservation)
            else:
                self.logger.error("A non-renewable reservation is on the renewing list")
        return result