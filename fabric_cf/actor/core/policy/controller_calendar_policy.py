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
from abc import abstractmethod

from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
from fabric_cf.actor.core.apis.abc_controller_policy import ABCControllerPolicy
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ControllerException
from fabric_cf.actor.core.core.policy import Policy
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.time.calendar.controller_calendar import ControllerCalendar
from fabric_cf.actor.core.util.reservation_set import ReservationSet


class ControllerCalendarPolicy(Policy, ABCControllerPolicy):
    """
    The base class for all calendar-based service manager policy implementations.
    """
    def __init__(self):
        super().__init__()
        # calendar
        self.calendar = None
        # Contains reservations for which we may have completed performing
        # bookkeeping actions but may need to wait for some other event to take
        # place before we raise the corresponding event.
        self.pending_notify = ReservationSet()
        # If the actor is initialized
        self.initialized = False
        # If true, the orchestrator will close reservations lazily: it will not
        # issue a close and will wait until the site terminates the lease. The
        # major drawback is that leave actions will not be able to connect to the
        # resources, since the resources will not exist at this time.
        self.lazy_close = False

    def check_pending(self):
        """
        Checks pending operations, and installs successfully completed
        requests in the holdings calendar. Note that the policy module must add
        bids to the pending set, or they may not install in the calendar.
       
        @raises Exception in case of error
        """
        rvset = self.calendar.get_pending()
        if rvset is None:
            return

        for reservation in rvset.values():
            if reservation.is_failed():
                # This reservation has failed. Remove it from the list. This is
                # a separate case, because we may fail but not satisfy the
                # condition of the else statement.
                self.logger.debug("Removing failed reservation from the pending list: {}".format(reservation))
                self.calendar.remove_pending(reservation=reservation)
                self.pending_notify.remove(reservation=reservation)
            elif reservation.is_no_pending() and not reservation.is_pending_recover():
                # No pending operation and we are not about the reissue a recovery operation on this reservation.
                self.logger.debug("Controller pending request completed {}".format(reservation))
                if reservation.is_closed():
                    # do nothing handled by close(IReservation)
                    self.logger.debug("No op")
                elif reservation.is_active_ticketed():
                    # An active reservation extended its ticket.
                    # cancel the current close
                    self.calendar.remove_closing(reservation=reservation)
                    # schedule a new close
                    self.calendar.add_closing(reservation=reservation,
                                              cycle=self.get_close(reservation=reservation,
                                                                   term=reservation.get_term()))
                    # Add from start to end instead of close. It is possible
                    # that holdings may not accurately reflect the actual
                    # number of resources towards the end of a lease. This is
                    # because we assume that we still have resources even after
                    # an advanceClose. When looking at this value, see if the
                    # reservation has closed.
                    self.calendar.add_holdings(reservation=reservation,
                                               start=reservation.get_term().get_new_start_time(),
                                               end=reservation.get_term().get_end_time())
                    self.calendar.add_redeeming(reservation=reservation, cycle=self.get_redeem(reservation=reservation))
                    if reservation.is_renewable():
                        cycle = self.get_renew(reservation=reservation)
                        reservation.set_renew_time(time=cycle)
                        reservation.set_dirty()
                        self.calendar.add_renewing(reservation=reservation, cycle=cycle)
                    self.pending_notify.remove(reservation=reservation)
                elif reservation.is_ticketed():
                    # The reservation obtained a ticket for the first time
                    self.calendar.add_holdings(reservation=reservation,
                                               start=reservation.get_term().get_new_start_time(),
                                               end=reservation.get_term().get_end_time())
                    self.calendar.add_redeeming(reservation=reservation,
                                                cycle=self.get_redeem(reservation=reservation))
                    self.calendar.add_closing(reservation=reservation,
                                              cycle=self.get_close(reservation=reservation,
                                                                   term=reservation.get_term()))
                    if reservation.is_renewable():
                        cycle = self.get_renew(reservation=reservation)
                        reservation.set_renew_time(time=cycle)
                        reservation.set_dirty()
                        self.calendar.add_renewing(reservation=reservation, cycle=cycle)
                    self.pending_notify.remove(reservation=reservation)
                elif reservation.get_state() == ReservationStates.Active:
                    if self.pending_notify.contains(reservation=reservation):
                        # We are waiting for transfer in operations to complete
                        # so that we can raise the lease complete event.
                        if reservation.is_active_joined():
                            self.pending_notify.remove(reservation=reservation)
                    else:
                        # Just completed a lease call (redeem or extendLease).
                        # We need to remove this reservation from closing,
                        # because we added it using r.getTerm(), and add this
                        # reservation to closing using r.getLeasedTerm() [the
                        # site could have changed the term of the reservation].
                        # This assumes that r.getTerm has not changed in the
                        # mean time. This is true now, since the state machine
                        # does not allow more than one pending operation.
                        # Should we change this, we will need to update the
                        # code below.
                        self.calendar.remove_closing(reservation=reservation)
                        self.calendar.add_closing(reservation=reservation,
                                                  cycle=self.get_close(reservation=reservation,
                                                                       term=reservation.get_lease_term()))
                        if reservation.get_renew_time() == 0:
                            reservation.set_renew_time(time=(self.actor.get_current_cycle() + 1))
                            reservation.set_dirty()
                            self.calendar.add_renewing(reservation=reservation, cycle=reservation.get_renew_time())

                        if not reservation.is_active_joined():
                            # add to the pending notify list so that we can raise the event
                            # when transfer in operations complete.
                            self.pending_notify.add(reservation=reservation)
                elif reservation.get_state() == ReservationStates.CloseWait or \
                        reservation.get_state() == ReservationStates.Failed:
                    self.pending_notify.remove(reservation=reservation)
                else:
                    self.logger.warning("Invalid state on reservation. We may be still recovering: {}".format(
                        reservation))
                    continue

                if not self.pending_notify.contains(reservation=reservation):
                    self.logger.debug("Removing from pending: {}".format(reservation))
                    self.calendar.remove_pending(reservation=reservation)

    def close(self, *, reservation: ABCReservationMixin):
        # ignore any scheduled/in progress operations
        self.calendar.remove_scheduled_or_in_progress(reservation=reservation)

    def closed(self, *, reservation: ABCReservationMixin):
        # remove the reservation from all calendar structures
        self.calendar.remove_holdings(reservation=reservation)
        self.calendar.remove_redeeming(reservation=reservation)
        self.calendar.remove_renewing(reservation=reservation)
        self.calendar.remove_closing(reservation=reservation)
        self.pending_notify.remove(reservation=reservation)

    def demand(self, *, reservation: ABCClientReservation):
        if not reservation.is_nascent():
            self.logger.error("demand reservation is not fresh")
        else:
            self.calendar.add_demand(reservation=reservation)

    def extend(self, *, reservation: ABCReservationMixin, resources: ResourceSet, term: Term):
        # cancel any previously scheduled extends
        self.calendar.remove_renewing(reservation=reservation)
        # do not cancel the close: the extend may fail cancel any pending redeem: we will redeem after the extension
        self.calendar.remove_redeeming(reservation=reservation)
        # There should be no pending operations for this reservation at this time
        # Add to the pending list so that we can track the progress of the reservation
        self.calendar.add_pending(reservation=reservation)

    def finish(self, *, cycle: int):
        super().finish(cycle=cycle)
        self.calendar.tick(cycle=cycle)

    @abstractmethod
    def get_close(self, *, reservation: ABCClientReservation, term: Term) -> int:
        """
        Returns the time that a reservation should be closed.

        @params reservation reservation
        @params term term

        @returns the close time of the reservation (cycle)

        @raises Exception in case of error
        """

    def get_closing(self, *, cycle: int) -> ReservationSet:
        closing = self.calendar.get_closing(cycle=cycle)
        result = ReservationSet()
        for reservation in closing.values():
            if not reservation.is_failed():
                self.calendar.add_pending(reservation=reservation)
                result.add(reservation=reservation)
            else:
                self.logger.warning("Removing failed reservation from the closing list: {}".format(reservation))
        return result

    @abstractmethod
    def get_redeem(self, *, reservation: ABCClientReservation) -> int:
        """
        Returns the time when the reservation should be redeemed.

        @params reservation the reservation

        @returns the redeem time of the reservation (cycle)

        @raises Exception in case of error
        """

    def get_redeeming(self, *, cycle: int) -> ReservationSet:
        redeeming = self.calendar.get_redeeming(cycle=cycle)
        for reservation in redeeming.values():
            self.calendar.add_pending(reservation=reservation)
        return redeeming

    @abstractmethod
    def get_renew(self, *, reservation: ABCClientReservation) -> int:
        """
        Returns the time when the reservation should be renewed.

        @params reservation the reservation

        @returns the renew time of the reservation (cycle)

        @raises Exception in case of error
        """

    def initialize(self):
        if not self.initialized:
            super().initialize()
            self.calendar = ControllerCalendar(clock=self.clock)
            self.initialized = True

    def is_expired(self, *, reservation: ABCReservationMixin):
        """
        Checks if the reservation has expired.

        @params reservation reservation to check

        @returns true or false
        """
        term = reservation.get_term()
        end = self.clock.cycle(when=term.get_end_time())
        return self.actor.get_current_cycle() > end

    def remove(self, *, reservation: ABCReservationMixin):
        # remove the reservation from the calendar
        self.calendar.remove(reservation=reservation)

    def revisit(self, *, reservation: ABCReservationMixin):
        super().revisit(reservation=reservation)

        if reservation.get_state() == ReservationStates.Nascent:
            self.calendar.add_pending(reservation=reservation)

        elif reservation.get_state() == ReservationStates.Ticketed:

            if reservation.get_pending_state() == ReservationPendingStates.None_:

                if reservation.is_pending_recover():

                    self.calendar.add_pending(reservation=reservation)

                else:

                    self.calendar.add_redeeming(reservation=reservation, cycle=self.get_redeem(reservation=reservation))

                self.calendar.add_holdings(reservation=reservation, start=reservation.get_term().get_new_start_time(),
                                           end=reservation.get_term().get_end_time())

                self.calendar.add_closing(reservation=reservation,
                                          cycle=self.get_close(reservation=reservation, term=reservation.get_term()))

                if reservation.is_renewable() and reservation.get_renew_time() != 0:
                    # Scheduling renewal is a bit tricky, since it may
                    # involve communication with the upstream broker.
                    # However, in some recovery cases, typical in one
                    # container deployment, the broker and the service
                    # manager will be recovering at the same time. In
                    # this case the query may fail and we will have to
                    # fail the reservation.
                    # Our approach here is as follows: we cache the
                    # renew time in the reservation class and persist
                    # it in the database. When we recover, we will
                    # check the renewTime field of the reservation if
                    # it is non-zero, we will use it, otherwise we will
                    # schedule the renew after we get the lease back
                    # from the authority.
                    self.calendar.add_renewing(reservation=reservation, cycle=reservation.get_renew_time())

            elif reservation.get_pending_state() == ReservationPendingStates.Redeeming:
                raise ControllerException(Constants.INVALID_RECOVERY_STATE)

        elif reservation.get_state() == ReservationStates.Active:
            if reservation.get_pending_state() == ReservationPendingStates.None_:
                # pending list
                if reservation.is_pending_recover():
                    self.calendar.add_pending(reservation=reservation)
                # renewing
                if reservation.is_renewable():
                    self.calendar.add_renewing(reservation=reservation, cycle=reservation.get_renew_time())
                # holdings
                self.calendar.add_holdings(reservation=reservation, start=reservation.get_term().get_new_start_time(),
                                           end=reservation.get_term().get_end_time())
                # closing
                self.calendar.add_closing(reservation=reservation,
                                          cycle=self.get_close(reservation=reservation,
                                                               term=reservation.get_lease_term()))

            elif reservation.get_pending_state() == ReservationPendingStates.ExtendingTicket:
                raise ControllerException(Constants.INVALID_RECOVERY_STATE)

        elif reservation.get_state() == ReservationStates.ActiveTicketed:

            if reservation.get_pending_state() == ReservationPendingStates.None_:
                if reservation.is_pending_recover():
                    self.calendar.add_pending(reservation=reservation)
                else:
                    self.calendar.add_redeeming(reservation=reservation, cycle=self.get_redeem(reservation=reservation))

                # holdings
                self.calendar.add_holdings(reservation=reservation, start=reservation.get_term().get_new_start_time(),
                                           end=reservation.get_term().get_end_time())

                # closing
                self.calendar.add_closing(reservation=reservation,
                                          cycle=self.get_close(reservation=reservation,
                                                               term=reservation.get_lease_term()))

                # renewing
                if reservation.is_renewable():
                    self.calendar.add_renewing(reservation=reservation, cycle=reservation.get_renew_time())

            elif reservation.get_pending_state() == ReservationPendingStates.ExtendingLease:
                raise ControllerException(Constants.INVALID_RECOVERY_STATE)

    def ticket_satisfies(self, *, requested_resources: ResourceSet, actual_resources: ResourceSet,
                         requested_term: Term, actual_term: Term):
        return

    def update_ticket_complete(self, *, reservation: ABCClientReservation):
        return

    def update_delegation_complete(self, *, delegation: ABCDelegation):
        return

    def lease_satisfies(self, *, request_resources: ResourceSet, actual_resources: ResourceSet, requested_term: Term,
                        actual_term: Term):
        return
