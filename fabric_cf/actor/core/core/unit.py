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
from enum import Enum

from fim.slivers.base_sliver import BaseSliver
from fim.slivers.network_node import NodeSliver

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
        # The modified version of the sliver.
        self.modified = None
        self.transfer_out_started = False
        self.sliver = sliver
        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['transfer_out_started']
        del state['reservation']
        del state['lock']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.transfer_out_started = False
        self.reservation = None
        self.lock = threading.Lock()

    def transition(self, *, to_state: UnitState):
        """
        Transition the state
        @param to_state ti state
        """
        self.state = to_state

    def fail(self, *, message: str, exception: Exception = None):
        """
        Fail a unit
        @param message
        @param exception exception
        """
        try:
            self.lock.acquire()
            self.notices.add(msg=message, ex=exception)
            self.transition(to_state=UnitState.FAILED)
        finally:
            self.lock.release()

    def fail_on_modify(self, *, message: str, exception: Exception = None):
        """
        Fail on modify
        @param message message
        @param exception exception
        """
        try:
            self.lock.acquire()
            self.notices.add(msg=message, ex=exception)
            self.transition(to_state=UnitState.ACTIVE)
            self.sliver = self.modified
            self.modified = None
        finally:
            self.lock.release()

    def set_state(self, *, state: UnitState):
        """
        Set state
        @param state state
        """
        try:
            self.lock.acquire()
            self.transition(to_state=state)
        finally:
            self.lock.release()

    def start_close(self):
        """
        Start close on a unit
        """
        try:
            self.lock.acquire()
            self.transition(to_state=UnitState.CLOSING)
            self.transfer_out_started = True
        finally:
            self.lock.release()

    def start_prime(self) -> bool:
        """
        Start priming the unit
        """
        try:
            self.lock.acquire()
            if self.state == UnitState.DEFAULT or self.state == UnitState.PRIMING:
                self.transition(to_state=UnitState.PRIMING)
                return True
        finally:
            self.lock.release()
        return False

    def start_modify(self) -> bool:
        """
        Start modifying
        """
        try:
            self.lock.acquire()
            if self.state == UnitState.ACTIVE or self.state == UnitState.MODIFYING:
                self.transition(to_state=UnitState.MODIFYING)
                return True
        finally:
            self.lock.release()
        return False

    def activate(self):
        """
        Mark the unit as active
        """
        try:
            self.lock.acquire()
            self.state = UnitState.ACTIVE
        finally:
            self.lock.release()

    def close(self):
        """
        Mark the unit as closed
        """
        try:
            self.lock.acquire()
            self.state = UnitState.CLOSED
        finally:
            self.lock.release()

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
        try:
            self.lock.acquire()
            self.properties[name] = value
        finally:
            self.lock.release()

    def get_property(self, *, name: str) -> str:
        """
        Get Property
        @param name name
        @return value
        """
        ret_val = None
        try:
            self.lock.acquire()
            if name in self.properties:
                ret_val = self.properties[name]
        finally:
            self.lock.release()
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

    def increment_sequence(self) -> int:
        """
        Increment sequence number
        """
        try:
            self.lock.acquire()
            self.sequence += 1
        finally:
            self.lock.release()

        ret_val = self.sequence
        return ret_val

    def decrement_sequence(self) -> int:
        """
        Decrement sequence number
        """
        try:
            self.lock.acquire()
            self.sequence -= 1
        finally:
            self.lock.release()
        ret_val = self.sequence
        return ret_val

    def set_reservation(self, *, reservation: ABCReservationMixin):
        """
        Set Reservation
        @param reservation reservation
        """
        try:
            self.lock.acquire()
            self.reservation = reservation
            self.reservation_id = reservation.get_reservation_id()
        finally:
            self.lock.release()

    def set_slice_id(self, *, slice_id: ID):
        """
        Set slice id
        @param slice_id slice id
        """
        try:
            self.lock.acquire()
            self.slice_id = slice_id
        finally:
            self.lock.release()

    def set_actor_id(self, *, actor_id: ID):
        """
        Set actor id
        @param actor_id actor id
        """
        try:
            self.lock.acquire()
            self.actor_id = actor_id
        finally:
            self.lock.release()

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
        try:
            self.lock.acquire()
            self.modified = modified
        finally:
            self.lock.release()

    def get_modified(self):
        """
        Get modified sliver
        @return modified sliver
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
        try:
            self.lock.acquire()
            self.parent_id = parent_id
        finally:
            self.lock.release()

    def complete_modify(self):
        """
        Complete Modify operation
        """
        try:
            self.lock.acquire()
            self.transition(to_state=UnitState.ACTIVE)
            self.sliver = self.modified
            self.modified = None
        finally:
            self.lock.release()

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
        try:
            self.lock.acquire()
            self.notices.add(msg=notice)
        finally:
            self.lock.release()

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
        try:
            self.lock.acquire()
            self.sliver = sliver
        finally:
            self.lock.release()

    def update_sliver(self, *, sliver:BaseSliver):
        """
        Update the sliver associated with the Unit
        Handler Process and AM process have different memory space
        So it requires explicit updates to AM process object with the values updated by Handler Processes
        Side effect of multi-processing in Python

        :param sliver Sliver returned by Handler
        """
        try:
            self.lock.acquire()
            self.sliver.set_label_allocations(sliver.get_label_allocations())
            if isinstance(self.sliver, NodeSliver) and isinstance(sliver, NodeSliver):
                self.sliver.management_ip = sliver.management_ip
        finally:
            self.lock.release()