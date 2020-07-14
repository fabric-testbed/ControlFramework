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

from fabric.actor.core.apis.IEvent import IEvent
from fabric.actor.core.apis.IEventHandler import IEventHandler
from fabric.actor.core.apis.IReservationTracker import IReservationTracker
from fabric.actor.core.kernel.ReservationPurgedEvent import ReservationPurgedEvent
from fabric.actor.core.kernel.ReservationStateTransitionEvent import ReservationStateTransitionEvent
from fabric.actor.core.kernel.ReservationStates import ReservationPendingStates, ReservationStates, JoinState
from fabric.actor.core.util.ID import ID
from fabric.actor.core.util.ReservationState import ReservationState


class ReservationTracker(IReservationTracker, IEventHandler):
    class ReservationTrackerState:
        def __init__(self):
            self.state = None
            self.purged = False

        def is_ticketing(self):
            return self.state.get_pending() == ReservationPendingStates.Ticketing or \
                   self.state.get_pending_state() == ReservationPendingStates.ExtendingTicket

        def is_ticketed(self):
            return self.state.get_state() == ReservationStates.Ticketed or \
                   self.state.get_state() == ReservationStates.ActiveTicketed

        def is_extending_ticketing(self):
            return self.state.get_pending() == ReservationPendingStates.ExtendingTicket

        def is_active(self):
            if self.state.joining == -1 :
                return self.state.get_state() == ReservationStates.Active and \
                       self.state.get_pending() == ReservationPendingStates.None_
            else:
                return (self.state.get_state() == ReservationStates.ActiveTicketed or
                        self.state.get_state() == ReservationStates.ActiveTicketed) and \
                       self.state.get_joining() == JoinState.NoJoin

        def is_closed(self):
            return self.state.get_state() == ReservationStates.Closed

        def is_failed(self):
            return self.state.get_state() == ReservationStates.Failed

        def is_terminal(self):
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

    def handle_state_transition(self, e: ReservationStateTransitionEvent):
        ts = None
        try:
            self.lock.acquire()
            if e.get_reservation_id() in self.state:
                ts = self.state.get(e.get_reservation_id())
            if ts is None:
                ts = ReservationTracker.ReservationTrackerState()
                ts.state = e.get_state()
                self.state[e.get_reservation_id()] = ts
        finally:
            self.lock.release()

    def handle_reservation_purged(self, e: ReservationPurgedEvent):
        ts = None
        try:
            self.lock.acquire()
            if e.get_reservation_id() in self.state:
                ts = self.state.pop(e.get_reservation_id())
                ts.purged = True
        finally:
            self.lock.release()

    def handle(self, event: IEvent):
        if isinstance(event, ReservationStateTransitionEvent):
            self.handle_state_transition(event)
        elif isinstance(event, ReservationPurgedEvent):
            self.handle_reservation_purged(event)

    def get_state(self, rid: ID) -> ReservationState:
        ret_val = None
        try:
            self.lock.acquire()
            if rid in self.state:
                ret_val = self.state[rid]
        finally:
            self.lock.release()
        return ret_val