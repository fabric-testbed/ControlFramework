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
from fabric_cf.actor.core.util.reservation_set import ReservationSet


class Bids:
    """
    Bid contains the result of running the bidding algorithm
    for a service manager or broker. It consists of two collections:

    - ticketing: set of new reservations to obtain tickets for
    - extending: set of existing reservations that need to be extended
    """
    def __init__(self, *, ticketing: ReservationSet, extending: ReservationSet):
        self.ticketing = ticketing
        self.extending = extending

    def get_extending(self) -> ReservationSet:
        """
        Returns the set of reservations for which the policy has decided
        to extend the existing tickets.

        @returns set of extending reservations
        """
        return self.extending

    def get_ticketing(self) -> ReservationSet:
        """
        Returns the set of reservations for which the policy has decided
        to obtain new tickets.

        @returns set of ticketing reservations
        """
        return self.ticketing
