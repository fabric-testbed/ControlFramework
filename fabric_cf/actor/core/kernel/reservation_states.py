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


class ReservationStates(Enum):
    """
    Reservation states
    """
    Nascent = 1
    Ticketed = 2
    Active = 4
    ActiveTicketed = 5
    Closed = 6
    CloseWait = 7
    Failed = 8
    Unknown = 9

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    @staticmethod
    def translate(state_name: str):
        if state_name.lower() == ReservationStates.Nascent.name.lower():
            return ReservationStates.Nascent
        elif state_name.lower() == ReservationStates.Ticketed.name.lower():
            return ReservationStates.Ticketed
        elif state_name.lower() == ReservationStates.Active.name.lower():
            return ReservationStates.Active
        elif state_name.lower() == ReservationStates.ActiveTicketed.name.lower():
            return ReservationStates.ActiveTicketed
        elif state_name.lower() == ReservationStates.Closed.name.lower():
            return ReservationStates.Closed
        elif state_name.lower() == ReservationStates.CloseWait.name.lower():
            return ReservationStates.CloseWait
        elif state_name.lower() == ReservationStates.Failed.name.lower():
            return ReservationStates.Failed
        else:
            return ReservationStates.Unknown


class ReservationPendingStates(Enum):
    """
    Pending operation states
    """
    None_ = 11
    Ticketing = 12
    Redeeming = 14
    ExtendingTicket = 15
    ExtendingLease = 16
    Priming = 17
    Blocked = 18
    Closing = 19
    Probing = 20
    ClosingJoining = 21
    ModifyingLease = 22
    AbsorbUpdate = 23
    SendUpdate = 24
    Unknown = 25

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


class JoinState(Enum):
    """
    Join states
    """
    None_ = 31
    NoJoin = 32
    BlockedJoin = 33
    BlockedRedeem = 34
    Joining = 35
    BlockedTicket = 36

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name