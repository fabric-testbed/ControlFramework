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
import enum
from enum import Enum
from typing import Tuple, List

from fabric_cf.actor.core.common.exceptions import SliceException
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.actor.core.util.id import ID

from fabric_cf.actor.core.util.reservation_set import ReservationSet


class SliceState(Enum):
    Nascent = enum.auto()
    Configuring = enum.auto()
    StableError = enum.auto()
    StableOK = enum.auto()
    Closing = enum.auto()
    Dead = enum.auto()
    Modifying = enum.auto()
    ModifyError = enum.auto()
    ModifyOK = enum.auto()
    All = enum.auto()   # used only for querying

    def __str__(self):
        return self.name

    @classmethod
    def list_values(cls) -> List[int]:
        return list(map(lambda c: c.value, cls))

    @classmethod
    def list_names(cls) -> List[str]:
        return list(map(lambda c: c.name, cls))

    @staticmethod
    def list_values_ex_closing_dead() -> List[int]:
        result = SliceState.list_values()
        result.remove(SliceState.Closing.value)
        result.remove(SliceState.Dead.value)
        return result

    @staticmethod
    def translate_list(states: List[str]) -> List[int] or None:
        if states is None or len(states) == 0:
            return states

        incoming_states = list(map(lambda x: x.lower(), states))

        result = SliceState.list_values()

        if len(incoming_states) == 1 and incoming_states[0] == SliceState.All.name.lower():
            return result

        for s in SliceState:
            if s.name.lower() not in incoming_states:
                result.remove(s.value)

        return result

    @staticmethod
    def translate(state_name: str):
        if state_name.lower() == SliceState.Nascent.name.lower():
            return SliceState.Nascent
        elif state_name.lower() == SliceState.Configuring.name.lower():
            return SliceState.Configuring
        elif state_name.lower() == SliceState.StableOK.name.lower():
            return SliceState.StableOK
        elif state_name.lower() == SliceState.StableError.name.lower():
            return SliceState.StableError
        elif state_name.lower() == SliceState.ModifyOK.name.lower():
            return SliceState.ModifyOK
        elif state_name.lower() == SliceState.ModifyError.name.lower():
            return SliceState.ModifyError
        elif state_name.lower() == SliceState.Modifying.name.lower():
            return SliceState.Modifying
        elif state_name.lower() == SliceState.Closing.name.lower():
            return SliceState.Closing
        elif state_name.lower() == SliceState.Dead.name.lower():
            return SliceState.Dead
        else:
            return SliceState.All

    @staticmethod
    def is_dead_or_closing(*, state) -> bool:
        if state == SliceState.Dead or state == SliceState.Closing:
            return True
        return False

    @staticmethod
    def is_stable(*, state) -> bool:
        if state == SliceState.StableOK or state == SliceState.StableError:
            return True
        return False

    @staticmethod
    def is_modified(*, state) -> bool:
        if state == SliceState.ModifyOK or state == SliceState.ModifyError:
            return True
        return False


class SliceCommand(Enum):
    Create = enum.auto()
    Modify = enum.auto()
    Delete = enum.auto()
    Reevaluate = enum.auto()
    ModifyAccept = enum.auto()

    def __str__(self):
        return self.name


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

    MODIFY = SliceOperation(SliceCommand.Modify, SliceState.StableOK, SliceState.StableError, SliceState.Configuring)

    MODIFY_ACCEPT = SliceOperation(SliceCommand.ModifyAccept, SliceState.ModifyOK, SliceState.ModifyError,
                                   SliceState.Modifying)

    DELETE = SliceOperation(SliceCommand.Delete, SliceState.Nascent, SliceState.StableOK, SliceState.StableError,
                            SliceState.Configuring, SliceState.Modifying, SliceState.ModifyOK, SliceState.ModifyError,
                            SliceState.Dead)

    REEVALUATE = SliceOperation(SliceCommand.Reevaluate, SliceState.Nascent, SliceState.StableOK,
                                SliceState.StableError, SliceState.Configuring, SliceState.Dead, SliceState.Closing,
                                SliceState.Modifying, SliceState.ModifyError, SliceState.ModifyOK)

    def __init__(self, *, slice_id: ID):
        self.slice_guid = slice_id
        self.state = SliceState.Nascent

    @staticmethod
    def all_failed(*, reservations: ReservationSet) -> bool:
        """
        We don't introduce a special state to flag when a slice is ALL FAILED, however this helper function helps decide
        when to GC a slice
        @return true or false
        """
        bins = StateBins()
        for r in reservations.values():
            bins.add(s=r.get_state())

        if not bins.has_state_other_than(ReservationStates.Failed):
            return True

        return False

    def transition_slice(self, *, operation: SliceOperation, reservations: ReservationSet) -> Tuple[bool, SliceState]:
        """
        Attempt to transition a slice to a new state
        @param operation slice operation
        @param reservations reservations
        @return Slice State
        @throws Exception in case of error
        """
        state_changed = False
        prev_state = self.state
        if self.state not in operation.valid_from_states:
            raise SliceException(f"Operation: {operation} cannot transition from state {self.state}")

        if operation.command == SliceCommand.Create:
            self.state = SliceState.Configuring

        elif operation.command == SliceCommand.Modify:
            self.state = SliceState.Modifying

        elif operation.command == SliceCommand.ModifyAccept:
            if self.state == SliceState.ModifyError:
                self.state = SliceState.StableError
            elif self.state == SliceState.ModifyOK:
                self.state = SliceState.StableOK

        elif operation.command == SliceCommand.Delete:
            if self.state != SliceState.Dead:
                self.state = SliceState.Closing

        elif operation.command == SliceCommand.Reevaluate:
            if reservations is None or reservations.size() == 0:
                return state_changed, self.state

            bins = StateBins()
            for r in reservations.values():
                bins.add(s=r.get_state())
                if r.get_pending_state() in [ReservationPendingStates.ModifyingLease,
                                             ReservationPendingStates.ExtendingTicket,
                                             ReservationPendingStates.ExtendingLease,
                                             ReservationPendingStates.Redeeming,
                                             ReservationPendingStates.Ticketing,
                                             ReservationPendingStates.Priming]:
                    bins.add(s=r.get_pending_state())

            if self.state == SliceState.Nascent or self.state == SliceState.Configuring:
                if not bins.has_state_other_than(ReservationStates.Active, ReservationStates.Closed):
                    self.state = SliceState.StableOK

                if (not bins.has_state_other_than(ReservationStates.Active, ReservationStates.Failed,
                                                  ReservationStates.Closed)) and \
                        bins.has_state(s=ReservationStates.Failed):
                    self.state = SliceState.StableError

                if not bins.has_state_other_than(ReservationStates.Closed, ReservationStates.CloseWait,
                                                 ReservationStates.Failed):
                    self.state = SliceState.Closing

            elif self.state == SliceState.Modifying:
                if not bins.has_state_other_than(ReservationStates.Active, ReservationStates.Closed):
                    self.state = SliceState.ModifyOK

                if (not bins.has_state_other_than(ReservationStates.Active, ReservationStates.Failed,
                                                  ReservationStates.Closed)) and \
                        bins.has_state(s=ReservationStates.Failed):
                    self.state = SliceState.ModifyError

                if not bins.has_state_other_than(ReservationStates.Closed, ReservationStates.CloseWait,
                                                 ReservationStates.Failed):
                    self.state = SliceState.Closing

            elif self.state == SliceState.StableError or self.state == SliceState.StableOK or \
                    self.state == SliceState.ModifyError or self.state == SliceState.ModifyOK:
                if not bins.has_state_other_than(ReservationStates.Closed, ReservationStates.CloseWait,
                                                 ReservationStates.Failed):
                    self.state = SliceState.Dead

                if not bins.has_state_other_than(ReservationStates.Closed, ReservationStates.CloseWait,
                                                 ReservationPendingStates.Closing, ReservationStates.Failed):
                    self.state = SliceState.Closing

            elif self.state == SliceState.Closing and not bins.has_state_other_than(ReservationStates.CloseWait,
                                                                                    ReservationStates.Closed,
                                                                                    ReservationStates.Failed):
                self.state = SliceState.Dead
        if prev_state != self.state:
            state_changed = True

        return state_changed, self.state

    def get_state(self) -> SliceState:
        return self.state

    def clear(self):
        self.state = SliceState.Nascent