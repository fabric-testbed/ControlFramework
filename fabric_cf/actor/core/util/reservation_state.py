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
from fabric_cf.actor.core.kernel.reservation_states import JoinState, ReservationPendingStates, ReservationStates


class ReservationState:
    """
    Represents the state of a reservation.
    """
    def __init__(self, *, state: ReservationStates, pending: ReservationPendingStates, joining: JoinState = -1):
        self.state = state
        self.pending = pending
        self.joining = joining

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, ReservationState):
            return NotImplemented

        return self.state == other.state and self.pending == other.pending and self.joining == other.joining

    def get_joining(self) -> JoinState:
        """
        Returns the joining sub-state.
      
        @return joining sub-state
        """
        return self.joining

    def get_joining_name(self) -> str:
        """
        Returns the joining sub-state name.

        @return joining sub-state name
        """
        return self.joining.name

    def get_pending(self) -> ReservationPendingStates:
        """
        Returns the pending sub-state.

        @return pending sub-state
        """
        return self.pending

    def get_pending_name(self) -> str:
        """
        Returns the pending sub-state name.

        @return pending sub-state name
        """
        return self.pending.name

    def get_state(self) -> ReservationStates:
        """
        Returns the state.

        @return state
        """
        return self.state

    def get_state_name(self) -> str:
        """
        Returns the state name

        @return state name
        """
        return self.state.name

    def __str__(self):
        if self.joining == -1:
            return "[{}, {}]".format(self.get_state_name(), self.get_pending_name())
        else:
            return "[{}, {}, {}]".format(self.get_state_name(), self.get_pending_name(), self.get_joining_name())
