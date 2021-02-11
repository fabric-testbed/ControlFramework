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
from typing import List

from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.actor.core.util.id import ID
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng

from fabric_cf.orchestrator.core.exceptions import OrchestratorException


class SliceState(Enum):
    Nascent = 1
    Configuring = 2
    StableError = 3
    StableOk = 4
    Closing = 5
    Dead = 6


class SliceCommand(Enum):
    Create = 1
    Modify = 2
    Delete = 3
    Reevaluate = 4


class SliceOperation:
    def __init__(self, command: SliceCommand, *valid_from_states):
        self.command = command
        self.valid_from_states = []
        if valid_from_states is not None:
            for state in valid_from_states:
                self.valid_from_states.append(state)

    def __str__(self):
        return self.command.name


class StateBins:
    def __init__(self):
        self.bins = {}

    def add(self, *, s: ReservationStates):
        if s not in self.bins:
            self.bins[s] = 1
        else:
            self.bins[s] += 1

    def has_state(self, *, s: ReservationStates) -> bool:
        count = self.bins.get(s, None)
        if count is not None and count > 0:
            return True
        return False

    def has_state_other_than(self, *states) -> bool:
        count = 0
        for state, value in self.bins.items():
            if value > 0:
                count += 1

        count1 = 0
        for state in states:
            if self.has_state(s=state):
                count1 += 1

        if count1 == count and count > 0:
            return False

        return True


class SliceStateMachine:
    CREATE = SliceOperation(SliceCommand.Create, SliceState.Nascent)

    MODIFY = SliceOperation(SliceCommand.Modify, SliceState.StableOk, SliceState.StableError, SliceState.Configuring)

    DELETE = SliceOperation(SliceCommand.Delete, SliceState.Nascent, SliceState.StableOk, SliceState.StableError,
                            SliceState.Configuring, SliceState.Dead)

    REEVALUATE = SliceOperation(SliceCommand.Reevaluate, SliceState.Nascent, SliceState.StableOk,
                                SliceState.StableError, SliceState.Configuring, SliceState.Dead, SliceState.Closing)

    def __init__(self, *, slice_id: ID, recover: bool):
        self.slice_guid = slice_id
        if recover:
            self.state = SliceState.Configuring
        else:
            self.state = SliceState.Nascent

    def get_slice_reservations(self) -> List[ReservationMng]:
        from fabric_cf.orchestrator.core.orchestrator_state import OrchestratorStateSingleton
        controller = OrchestratorStateSingleton.get().get_management_actor()
        try:
            return controller.get_reservations(slice_id=self.slice_guid)
        except Exception:
            return None

    def all_failed(self) -> bool:
        all_res = self.get_slice_reservations()
        bins = StateBins()
        for r in all_res:
            bins.add(s=ReservationStates(r.get_state()))

        if not bins.has_state_other_than(ReservationStates.Failed):
            return True

        return False

    def transition_slice(self, *, operation: SliceOperation) -> SliceState:
        if self.state not in operation.valid_from_states:
            raise OrchestratorException("Operation: {} cannot transition from state {}".format(operation, self.state))

        if operation.command == SliceCommand.Create:
            self.state = SliceState.Configuring

        elif operation.command == SliceCommand.Modify:
            self.state = SliceState.Configuring

        elif operation.command == SliceCommand.Delete:
            if self.state != SliceState.Dead:
                self.state = SliceState.Closing

        elif operation.command == SliceCommand.Reevaluate:
            all_res = self.get_slice_reservations()
            if all_res is None or len(all_res) == 0:
                return self.state

            bins = StateBins()
            for r in all_res:
                bins.add(s=ReservationStates(r.get_state()))

            if self.state == SliceState.Nascent or self.state == SliceState.Configuring:

                if not bins.has_state_other_than(ReservationStates.Active.value, ReservationStates.Closed.value):
                    self.state = SliceState.StableOk

                if (not bins.has_state_other_than(ReservationStates.Active, ReservationStates.Failed,
                                                  ReservationStates.Closed)) and \
                        bins.has_state_other_than(ReservationStates.Failed):
                    self.state = SliceState.StableError

                if not bins.has_state_other_than(ReservationStates.Closed, ReservationStates.CloseWait,
                                                 ReservationStates.Failed):
                    self.state = SliceState.Closing

            elif self.state == SliceState.StableError or self.state == SliceState.StableOk:
                if not bins.has_state_other_than(ReservationStates.Closed, ReservationStates.CloseWait,
                                                 ReservationStates.Failed):
                    self.state = SliceState.Dead

                if not bins.has_state_other_than(ReservationStates.Closed, ReservationStates.CloseWait,
                                                 ReservationPendingStates.Closing, ReservationStates.Failed):
                    self.state = SliceState.Closing

            elif self.state == SliceState.Closing:
                if not bins.has_state_other_than(ReservationStates.CloseWait, ReservationStates.Closed,
                                                 ReservationStates.Failed):
                    self.state = SliceState.Dead

        return self.state

    def get_state(self) -> SliceState:
        return self.state
