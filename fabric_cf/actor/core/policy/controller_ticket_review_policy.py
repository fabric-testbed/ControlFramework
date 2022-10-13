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
from enum import Enum

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.policy.controller_simple_policy import ControllerSimplePolicy
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.core.util.update_data import UpdateData


class TicketReviewSliceState(Enum):
    """
    Redeemable: the default.  No slice reservations have been found that are either Nascent or Failed.
    Nascent: occurs when any reservation is Nascent, i.e. not yet Ticketed.  Will take precedence over Failing.
    Failing: occurs when a slice reservation is found that is Failed.
    """
    Nascent = 1
    Failing = 2
    Redeemable = 3


class ControllerTicketReviewPolicy(ControllerSimplePolicy):
    """
    This implementation of a Controller policy is almost identical to the parent ControllerSimplePolicy.
    The only real difference is that it addresses that Tickets should not be redeemed if any reservations are
    currently Failed or Nascent.  This effectively acts as a "gate" between the Controller and AM.
    All reservations must be Ticketed, before any reservations are allowed to be redeemed.
    """
    def __init__(self):
        super().__init__()
        self.pending_redeem = ReservationSet()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']
        del state['pending_notify']
        del state['lazy_close']
        del state['pending_redeem']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

        self.logger = None
        self.actor = None
        self.clock = None
        self.initialized = False
        self.pending_notify = ReservationSet()
        self.lazy_close = False
        self.pending_redeem = ReservationSet()

    def check_pending(self):
        """
        Check to make sure all reservations are Ticketed (not Failed or Nascent)
        before calling the parent method.

        @throws Exception in case of error
        """
        # add all of our pendingRedeem, so they can be checked
        for reservation in self.pending_redeem.values():
            self.calendar.add_pending(reservation=reservation)

        # get set of reservations that need to be redeemed
        my_pending = self.calendar.get_pending()

        # keep track of status of the slice containing each reservation
        slice_status_map = {}

        # nothing to do!
        if my_pending is None:
            return

        # check the status of the Slice of each reservation
        for reservation in my_pending.values():
            slice_obj = reservation.get_slice()
            slice_id = slice_obj.get_slice_id()

            # only want to do this for 'new' tickets
            if reservation.is_failed() or reservation.is_ticketed():
                # check if we've examined this slice already
                if slice_id not in slice_status_map:
                    # set the default status
                    slice_status_map[slice_id] = TicketReviewSliceState.Redeemable
                    # examine every reservation contained within the slice,
                    # looking for either a Failed or Nascent reservation
                    # we have to look at everything in a slice once, to determine all/any Sites with failures
                    for slice_reservation in slice_obj.get_reservations().values():

                        # If any Reservations that are being redeemed, that means the
                        # slice has already cleared TicketReview.
                        if slice_reservation.is_redeeming():
                            if slice_status_map[slice_id] == TicketReviewSliceState.Nascent:
                                # There shouldn't be any Nascent reservations, if a reservation is being Redeemed.
                                self.logger.error(
                                    "Nascent reservation found while Reservation {} in slice {} is redeeming"
                                    .format(slice_reservation.get_reservation_id(), slice_obj.get_name()))

                            # We may have previously found a Failed Reservation,
                            # but if a ticketed reservation is being redeemed,
                            # the failure _should_ be from the AM, not Controller
                            # so it should be ignored by TicketReview
                            slice_status_map[slice_id] = TicketReviewSliceState.Redeemable

                            #  we don't need to look at any other reservations in this slice
                            break

                        # if any tickets are Nascent,
                        # as soon as we remove the Failed reservation,
                        # those Nascent tickets might get redeemed.
                        # we must wait to Close any failed reservations
                        # until all Nascent tickets are either Ticketed or Failed
                        if slice_reservation.is_nascent():
                            self.logger.debug("Found Nascent Reservation {} in slice {} when check_pending for {}"
                                              .format(slice_reservation.get_reservation_id(), slice_obj.get_name(),
                                                      reservation.get_reservation_id()))

                            slice_status_map[slice_id] = TicketReviewSliceState.Nascent
                            # once we have found a Nascent reservation, that is what we treat the entire slice
                            break

                        # track Failed reservations, but need to keep looking for Nascent or Redeemable.
                        if slice_reservation.is_failed():
                            self.logger.debug("Found failed reservation {} in slice {} when check_pending for {}"
                                              .format(slice_reservation.get_reservation_id(), slice_obj.get_name(),
                                                      reservation.get_reservation_id()))
                            slice_status_map[slice_id] = TicketReviewSliceState.Failing

                # take action on the current reservation
                if slice_status_map[slice_id] == TicketReviewSliceState.Failing:
                    if reservation.get_resources() is not None and reservation.get_resources().get_type() is not None:
                        msg = f"TicketReviewPolicy: Closing reservation {reservation.get_reservation_id()} due to " \
                              f"failure in slice {slice_obj.get_name()}"
                        self.logger.info(msg)

                        if not reservation.is_failed():
                            update_data = UpdateData()
                            update_data.failed = True
                            update_data.message = Constants.CLOSURE_BY_TICKET_REVIEW_POLICY
                            reservation.mark_close_by_ticket_review(update_data=update_data)
                        self.actor.close(reservation=reservation)
                        self.calendar.remove_pending(reservation=reservation)
                        self.pending_notify.remove(reservation=reservation)

                elif slice_status_map[slice_id] == TicketReviewSliceState.Nascent:
                    self.logger.debug(
                        "Moving reservation {} to pending redeem list due to nascent reservation in slice {}"
                        .format(reservation.get_reservation_id(), slice_obj.get_name()))
                    self.pending_redeem.add(reservation=reservation)
                    self.calendar.remove_pending(reservation=reservation)
                else:
                    # we don't need to look at any other reservations in this slice
                    self.logger.debug("Removing from pendingRedeem: {}".format(reservation))
                    self.pending_redeem.remove(reservation=reservation)
            else:
                # Remove active or close reservations
                self.logger.debug("Removing from pendingRedeem: {}".format(reservation))
                self.pending_redeem.remove(reservation=reservation)

        super().check_pending()
