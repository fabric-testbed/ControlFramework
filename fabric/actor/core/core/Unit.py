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
import pickle
from enum import Enum


from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.plugins.config.config_token import ConfigToken
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.notice import Notice
from fabric.actor.core.util.resource_type import ResourceType


class UnitState(Enum):
    DEFAULT = 0
    PRIMING = 1
    ACTIVE = 2
    MODIFYING = 3
    CLOSING = 4
    CLOSED = 5
    FAILED = 6


class Unit(ConfigToken):
    def __init__(self, id: ID, rid: ID = None, slice_id: ID = None, actor_id: ID = None,
                 properties: dict = None, state: UnitState = None):
        # Unique identifier.
        self.id = id
        # Resource type.
        self.rtype = None
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
        # Reservation this unit belongs to (id).
        self.reservation_id = rid
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

    def transition(self, to_state: UnitState):
        self.state = to_state

    def merge_properties(self, incoming: dict):
        self.properties = {**incoming, **self.properties}

    def fail(self, message: str, exception: Exception = None):
        self.notices.add(message, exception)
        self.transition(UnitState.FAILED)

    def fail_on_modify(self, message: str, exception: Exception = None):
        self.notices.add(message, exception)
        self.transition(UnitState.ACTIVE)
        self.merge_properties(self.modified.properties)

    def set_state(self, state: UnitState):
        self.transition(state)

    def start_close(self):
        self.transition(UnitState.CLOSING)
        self.transfer_out_started = True

    def start_prime(self) -> bool:
        if self.state == UnitState.DEFAULT or self.state == UnitState.PRIMING:
            self.transition(UnitState.PRIMING)
            return True
        return False

    def start_modify(self) -> bool:
        if self.state == UnitState.ACTIVE or self.state == UnitState.MODIFYING:
            self.transition(UnitState.MODIFYING)
            return True
        return False

    def activate(self):
        self.state = UnitState.ACTIVE

    def close(self):
        self.state = UnitState.CLOSED

    def get_id(self) -> ID:
        return self.id

    def get_properties(self) -> dict:
        return self.properties

    def set_property(self, name: str, value: str):
        self.properties[name] = value

    def get_property(self, name: str) -> str:
        ret_val = None
        if name in self.properties:
            ret_val = self.properties[name]
        return ret_val

    def get_state(self) -> UnitState:
        return self.state

    def clone(self):
        ret_val = Unit(id=self.id, properties=self.properties, state=self.state)
        return ret_val

    def get_sequence(self) -> int:
        return self.sequence

    def get_sequence_increment(self) -> int:
        self.sequence += 1
        ret_val = self.sequence
        return ret_val

    def increment_sequence(self) -> int:
        self.sequence += 1
        ret_val = self.sequence
        return ret_val

    def decrement_sequence(self) -> int:
        self.sequence -= 1
        ret_val = self.sequence
        return ret_val

    def set_reservation(self, reservation: IReservation):
        self.reservation = reservation
        self.reservation_id = reservation.get_reservation_id()

    def set_slice_id(self, slice_id: ID):
        self.slice_id = slice_id

    def set_actor_id(self, actor_id: ID):
        self.actor_id = actor_id

    def is_failed(self) -> bool:
        ret_val = self.state == UnitState.FAILED
        return ret_val

    def is_closed(self) -> bool:
        ret_val = self.state == UnitState.CLOSED
        return ret_val

    def is_active(self) -> bool:
        ret_val = self.state == UnitState.ACTIVE
        return ret_val

    def has_pending_action(self) -> bool:
        ret_val = self.state == UnitState.MODIFYING or self.state == UnitState.PRIMING or self.state == UnitState.CLOSING
        return ret_val

    def set_modified(self, modified):
        self.modified = modified

    def get_modified(self):
        ret_val = self.modified
        return ret_val

    def get_slice_id(self) -> ID:
        ret_val = self.slice_id
        return ret_val

    def get_reservation_id(self) -> ID:
        ret_val = self.reservation_id
        return ret_val

    def get_reservation(self) -> IReservation:
        ret_val = self.reservation
        return ret_val

    def get_parent_id(self) -> ID:
        ret_val = self.parent_id
        return ret_val

    def set_parent_id(self, parent_id: ID):
        self.parent_id = parent_id

    def complete_modify(self):
        self.transition(UnitState.ACTIVE)
        self.merge_properties(self.modified.properties)

    def get_actor_id(self) -> ID:
        ret_val = self.actor_id
        return ret_val

    def get_resource_type(self) -> ResourceType:
        ret_val = self.rtype
        return ret_val

    def set_resource_type(self, rtype: ResourceType):
        self.rtype = rtype

    def __eq__(self, other):
        if not isinstance(other, Unit):
            return NotImplemented

        return self.id == other.id

    def get_notices(self) -> Notice:
        ret_val = self.notices
        return ret_val

    def add_notice(self, notice: str):
        self.notices.add(notice)

    def __str__(self):
        return "[unit: {} reservation: {} actor: {} state: {}]".format(self.id, self.reservation_id, self.actor_id, self.state)

    def __hash__(self):
        return self.id.__hash__()

    @staticmethod
    def create_instance(properties: dict):
        if Constants.PropertyPickleProperties not in properties:
            raise Exception("Invalid arguments")
        deserialized_unit = None
        try:
            serialized_unit = properties[Constants.PropertyPickleProperties]
            deserialized_unit = pickle.loads(serialized_unit)
        except Exception as e:
            raise e
        return deserialized_unit