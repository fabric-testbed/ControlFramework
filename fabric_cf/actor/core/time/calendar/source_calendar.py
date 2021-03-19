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
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.util.reservation_holdings import ReservationHoldings
from fabric_cf.actor.core.util.reservation_list import ReservationList


class SourceCalendar:
    """
    SourceCalendar organizes state for a delegation used as a
    source for client reservations. A source calendar maintains a list of
    "outlays", client reservations that have been allocated from the source
    delegation. The outlay calendar is organized by real time.
    The calendar also maintains a list of incoming extension requests for
    reservations that have been satisfied from the underlying source delegations.
    """
    def __init__(self, *, clock: ActorClock, source: ABCDelegation):
        """
        Constructor
        @params clock: clock
        @params source: source delegation
        """
        # Clock.
        self.clock = clock
        # The source delegation.
        self.source = source
        # Allocated reservations.
        self.outlays = ReservationHoldings()
        # Incoming extension requests.
        self.extending = ReservationList()

    def tick(self, *, cycle: int):
        ms = self.clock.cycle_end_in_millis(cycle=cycle)
        self.outlays.tick(time=ms)
        self.extending.tick(cycle=cycle)
