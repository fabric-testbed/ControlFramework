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
import threading

from fabric_cf.actor.core.apis.i_event import IEvent
from fabric_cf.actor.core.apis.i_event_handler import IEventHandler
from fabric_cf.actor.core.apis.i_reservation_tracker import IReservationTracker
from fabric_cf.actor.core.kernel.reservation_purged_event import ReservationPurgedEvent
from fabric_cf.actor.core.kernel.reservation_state_transition_event import ReservationStateTransitionEvent
from fabric_cf.actor.core.kernel.reservation_states import ReservationPendingStates, ReservationStates, JoinState
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reservation_state import ReservationState


class ReservationTracker(IReservationTracker, IEventHandler):
    class ReservationTrackerState:
        def __init__(self):
            self.state = None
            self.purged = False

        def is_ticketing(self):
            """
            Return true if in ticketing state; false otherwise
            """
            return self.state.get_pending() == ReservationPendingStates.Ticketing or \
                   self.state.get_pending_state() == ReservationPendingStates.ExtendingTicket

        def is_ticketed(self):
            """
            Return true if in Ticketed state; false otherwise
            """
            return self.state.get_state() == ReservationStates.Ticketed or \
                   self.state.get_state() == ReservationStates.ActiveTicketed

        def is_extending_ticketing(self):
            """
            Return true if in Extending Ticketing state; false otherwise
            """
            return self.state.get_pending() == ReservationPendingStates.ExtendingTicket

        def is_active(self):
            """
            Return true if in Active state; false otherwise
            """
            if self.state.get_joining() == JoinState.None_:
                return self.state.get_state() == ReservationStates.Active and \
                       self.state.get_pending() == ReservationPendingStates.None_
            else:
                return (self.state.get_state() == ReservationStates.Active or
                        self.state.get_state() == ReservationStates.ActiveTicketed) and \
                       self.state.get_joining() == JoinState.NoJoin

        def is_closed(self):
            """
            Return true if in Closed state; false otherwise
            """
            return self.state.get_state() == ReservationStates.Closed

        def is_failed(self):
            """
            Return true if in Failed state; false otherwise
            """
            return self.state.get_state() == ReservationStates.Failed

        def is_terminal(self):
            """
            Return true if in Closed, failed or purged state; false otherwise
            """
            return self.is_closed() or self.is_failed() or self.purged

    def __init__(self):
        self.state = {}
        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.lock = threading.Lock()

    def handle_state_transition(self, *, e: ReservationStateTransitionEvent):
        """
        Handle State transition event
        @param e state transition event
        """

        ts = None
        try:
            self.lock.acquire()
            if e.get_reservation_id() in self.state:
                ts = self.state.get(e.get_reservation_id(), None)
            if ts is None:
                ts = ReservationTracker.ReservationTrackerState()
                ts.state = e.get_state()
                self.state[e.get_reservation_id()] = ts
        finally:
            self.lock.release()

    def handle_reservation_purged(self, *, e: ReservationPurgedEvent):
        """
        Handle Purge event
        @param e purge event
        """
        ts = None
        try:
            self.lock.acquire()
            if e.get_reservation_id() in self.state:
                ts = self.state.pop(e.get_reservation_id())
                ts.purged = True
        finally:
            self.lock.release()

    def handle(self, *, event: IEvent):
        """
        Handle an incoming event
        @param event incoming event
        """
        if isinstance(event, ReservationStateTransitionEvent):
            self.handle_state_transition(e=event)
        elif isinstance(event, ReservationPurgedEvent):
            self.handle_reservation_purged(e=event)

    def get_state(self, *, rid: ID) -> ReservationState:
        """
        Return state for a reservation
        @param rid reservation id
        """
        ret_val = None
        try:
            self.lock.acquire()
            if rid in self.state:
                ret_val = self.state[rid]
        finally:
            self.lock.release()
        return ret_val
