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

from fim.slivers.base_sliver import BaseSliver

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.notice import Notice
from fabric_cf.actor.core.util.resource_type import ResourceType


class UnitState(Enum):
    DEFAULT = 0
    PRIMING = 1
    ACTIVE = 2
    MODIFYING = 3
    CLOSING = 4
    CLOSED = 5
    FAILED = 6


class Unit(ConfigToken):
    def __init__(self, *, rid: ID, sliver: BaseSliver = None, slice_id: ID = None, actor_id: ID = None,
                 properties: dict = None, state: UnitState = None, rtype: ResourceType = None):
        # Reservation this unit belongs to (id).
        self.reservation_id = rid
        # Resource type.
        self.rtype = rtype
        # Unique identifier of parent unit (optional).
        self.parent_id = None
        # Properties list.
        self.properties = {}
        if properties is not None:
            self.properties = properties
        # Unit state
        self.state = UnitState.DEFAULT
        if state is not None:
            self.state = state
        # Configuration sequence number. Each unique configuration action is identified by a sequence number.
        self.sequence = 0
        # Slice this unit belongs to.
        self.slice_id = slice_id
        # Actor this unit belongs to.
        self.actor_id = actor_id
        self.notices = Notice()
        # Reservation this unit belongs to.
        self.reservation = None
        # The modified version of this unit.
        self.modified = None
        self.transfer_out_started = False
        self.sliver = sliver
        self.worker_node_name = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['transfer_out_started']
        del state['modified']
        del state['reservation']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.transfer_out_started = False
        self.modified = False
        self.reservation = None

    def transition(self, *, to_state: UnitState):
        """
        Transition the state
        @param to_state ti state
        """
        self.state = to_state

    def merge_properties(self, *, incoming: dict):
        """
        Merge properties
        @param incoming incoming properties
        """
        self.properties = {**incoming, **self.properties}

    def fail(self, *, message: str, exception: Exception = None):
        """
        Fail a unit
        @param message
        @param exception exception
        """
        self.notices.add(msg=message, ex=exception)
        self.transition(to_state=UnitState.FAILED)

    def fail_on_modify(self, *, message: str, exception: Exception = None):
        """
        Fail on modify
        @param message message
        @param exception exception
        """
        self.notices.add(msg=message, ex=exception)
        self.transition(to_state=UnitState.ACTIVE)
        self.merge_properties(incoming=self.modified.properties)

    def set_state(self, *, state: UnitState):
        """
        Set state
        @param state state
        """
        self.transition(to_state=state)

    def start_close(self):
        """
        Start close on a unit
        """
        self.transition(to_state=UnitState.CLOSING)
        self.transfer_out_started = True

    def start_prime(self) -> bool:
        """
        Start priming the unit
        """
        if self.state == UnitState.DEFAULT or self.state == UnitState.PRIMING:
            self.transition(to_state=UnitState.PRIMING)
            return True
        return False

    def start_modify(self) -> bool:
        """
        Start modifying
        """
        if self.state == UnitState.ACTIVE or self.state == UnitState.MODIFYING:
            self.transition(to_state=UnitState.MODIFYING)
            return True
        return False

    def activate(self):
        """
        Mark the unit as active
        """
        self.state = UnitState.ACTIVE

    def close(self):
        """
        Mark the unit as closed
        """
        self.state = UnitState.CLOSED

    def get_id(self) -> ID:
        """
        Get unit id
        @return unit id
        """
        return self.reservation_id

    def get_properties(self) -> dict:
        """
        Get unit properties
        @return unit properties
        """
        return self.properties

    def set_property(self, *, name: str, value: str):
        """
        Set property
        @param name name
        @param value value
        """
        self.properties[name] = value

    def get_property(self, *, name: str) -> str:
        """
        Get Property
        @param name name
        @return value
        """
        ret_val = None
        if name in self.properties:
            ret_val = self.properties[name]
        return ret_val

    def get_state(self) -> UnitState:
        """
        Get Unit state
        @return unit state
        """
        return self.state

    def clone(self):
        """
        Clone a unit
        @return copy of the object
        """
        ret_val = Unit(rid=self.reservation_id, sliver=self.sliver, properties=self.properties, state=self.state)
        return ret_val

    def get_sequence(self) -> int:
        """
        Get Sequence number
        @return sequence number
        """
        return self.sequence

    def get_sequence_increment(self) -> int:
        """
        Get Sequence number and increment it
        @return sequence number and increment it
        """
        self.sequence += 1
        ret_val = self.sequence
        return ret_val

    def increment_sequence(self) -> int:
        """
        Increment sequence number
        """
        self.sequence += 1
        ret_val = self.sequence
        return ret_val

    def decrement_sequence(self) -> int:
        """
        Decrement sequence number
        """
        self.sequence -= 1
        ret_val = self.sequence
        return ret_val

    def set_reservation(self, *, reservation: ABCReservationMixin):
        """
        Set Reservation
        @param reservation reservation
        """
        self.reservation = reservation
        self.reservation_id = reservation.get_reservation_id()

    def set_slice_id(self, *, slice_id: ID):
        """
        Set slice id
        @param slice_id slice id
        """
        self.slice_id = slice_id

    def set_actor_id(self, *, actor_id: ID):
        """
        Set actor id
        @param actor_id actor id
        """
        self.actor_id = actor_id

    def is_failed(self) -> bool:
        """
        Check fail status
        @return true if unit is failed, false otherwise
        """
        ret_val = self.state == UnitState.FAILED
        return ret_val

    def is_closed(self) -> bool:
        """
        Check close status
        @return true if unit is closed, false otherwise
        """
        ret_val = self.state == UnitState.CLOSED
        return ret_val

    def is_active(self) -> bool:
        """
        Check active status
        @return true if unit is active, false otherwise
        """
        ret_val = self.state == UnitState.ACTIVE
        return ret_val

    def has_pending_action(self) -> bool:
        """
        Check pending action status
        @return true if unit has pending operation, false otherwise
        """
        return self.state == UnitState.MODIFYING or self.state == UnitState.PRIMING or self.state == UnitState.CLOSING

    def set_modified(self, *, modified):
        """
        Set modified unit
        @param modified modified
        """
        self.modified = modified

    def get_modified(self):
        """
        Get modified unit
        @return modified unit
        """
        return self.modified

    def get_slice_id(self) -> ID:
        """
        Return slice id
        @return slice id
        """
        return self.slice_id

    def get_reservation_id(self) -> ID:
        """
        Get Reservation Id
        @return reservation id
        """
        return self.reservation_id

    def get_reservation(self) -> ABCReservationMixin:
        """
        Get Reservation
        @return reservation
        """
        ret_val = self.reservation
        return ret_val

    def get_parent_id(self) -> ID:
        """
        Get Parent Id
        @return parent guid
        """
        return self.parent_id

    def set_parent_id(self, *, parent_id: ID):
        """
        Set Parent id
        @param parent_id parent id
        """
        self.parent_id = parent_id

    def complete_modify(self):
        """
        Complete Modify operation
        """
        self.transition(to_state=UnitState.ACTIVE)
        self.merge_properties(incoming=self.modified.properties)

    def get_actor_id(self) -> ID:
        """
        Get Actor id
        @return actor id
        """
        return self.actor_id

    def get_resource_type(self) -> ResourceType:
        """
        Get Resource Type
        @return resource type
        """
        return self.rtype

    def set_resource_type(self, *, rtype: ResourceType):
        """
        Set resource type
        @param rtype resource type
        """
        self.rtype = rtype

    def __eq__(self, other):
        if not isinstance(other, Unit):
            return NotImplemented

        return self.reservation_id == other.reservation_id

    def get_notices(self) -> Notice:
        """
        Get notices
        @return notices
        """
        ret_val = self.notices
        return ret_val

    def add_notice(self, *, notice: str):
        """
        Add a notice
        @param notice notice
        """
        self.notices.add(msg=notice)

    def __str__(self):
        return f"[unit: {self.reservation_id} actor: {self.actor_id} state: {self.state} " \
               f"sliver: {self.sliver} properties: {self.properties}]"

    def __hash__(self):
        return self.reservation_id.__hash__()

    def get_sliver(self) -> BaseSliver:
        """
        Return the sliver associated with the Unit
        :return: sliver
        """
        return self.sliver

    def set_sliver(self, *, sliver: BaseSliver):
        """
        Set the sliver associated with the Unit
        :param sliver: sliver
        :return:
        """
        self.sliver = sliver
