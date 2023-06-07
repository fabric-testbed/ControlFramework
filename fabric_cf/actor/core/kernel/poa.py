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
from __future__ import annotations

import traceback
from typing import TYPE_CHECKING

from enum import Enum
from typing import List, Dict
from uuid import uuid4

from fabric_cf.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin


class PoaStates(Enum):
    """
    Reservation states
    """
    Nascent = 1
    Issued = 2
    Performing = 3
    AwaitingCompletion = 4
    Success = 5
    Failed = 6
    Unknown = 7
    None_ = 8

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


class PoaInfo:
    def __init__(self, host_name: str, host_info: dict, instance_name: str, instance_info: dict):
        self.host_name = host_name
        self.host_info = host_info
        self.instance_name = instance_name
        self.instance_info = instance_info


class Poa:
    def __init__(self, *, poa_id: str = uuid4().__str__(), operation: str, reservation: ABCReservationMixin = None,
                 sliver_id: ID = None, vcpu_cpu_map: List[Dict[str, str]] = None, node_set: List[str] = None):
        self.poa_id = poa_id
        self.operation = operation
        self.state = PoaStates.Nascent
        self.reservation = reservation
        self.sliver_id = sliver_id
        self.vcpu_cpu_map = vcpu_cpu_map
        self.node_set = node_set
        # Sequence number for outgoing poa messages. Increases with every new message.
        self.sequence_poa_out = 0
        # Sequence number for incoming poa messages.
        self.sequence_poa_in = 0
        self.service_pending = PoaStates.None_

        self.slice_object = None
        self.slice_id = None
        self.actor = None
        self.logger = None
        self.dirty = False

        if reservation is not None:
            self.sliver_id = reservation.get_reservation_id()
            if reservation.get_slice() is not None:
                self.slice_id = reservation.get_slice().get_slice_id()
                self.slice_object = reservation.get_slice()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['slice_object']
        del state['reservation']
        del state['actor']
        del state['logger']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.reservation = None
        self.slice_object = None
        self.actor = None
        self.logger = None

    def restore(self, *, actor: ABCActorMixin, reservation: ABCReservationMixin):
        self.reservation = reservation
        self.slice_object = self.reservation.get_slice()
        self.actor = actor
        self.logger = self.actor.get_logger()

        if reservation is not None:
            self.sliver_id = reservation.get_reservation_id()
            if reservation.get_slice() is not None:
                self.slice_id = reservation.get_slice().get_slice_id()

    def get_poa_id(self) -> str:
        return self.poa_id

    def get_operation(self) -> str:
        return self.operation

    def get_sliver_id(self) -> ID:
        return self.sliver_id

    def get_slice_id(self) -> ID:
        return self.slice_id

    def get_actor(self) -> ABCActorMixin:
        return self.actor

    def get_state(self) -> PoaStates:
        return self.state

    def get_poa_sequence_in(self) -> int:
        return self.sequence_poa_in

    def get_poa_sequence_out(self) -> int:
        return self.sequence_poa_out

    def get_slice(self) -> ABCSlice:
        return self.slice_object

    def get_reservation(self) -> ABCReservationMixin:
        return self.reservation

    def is_dirty(self) -> bool:
        return self.dirty

    def clear_dirty(self):
        self.dirty = False

    def set_dirty(self):
        self.dirty = True

    def transition(self, *, prefix: str, state: PoaStates):
        if self.state == PoaStates.Failed and self.logger is not None:
            self.logger.debug("failed")

        if self.logger is not None:
            self.logger.debug(f"POA #{self.poa_id} {prefix} transition: {self.get_state()} -> {state.name}")

        change = self.state != state

        if change:
            self.state = state
            self.set_dirty()

    def issue_poa(self):
        from fabric_cf.actor.core.kernel.reservation_client import ReservationClient
        if not isinstance(self.reservation, ReservationClient):
            raise Exception("POA can be triggered only from Orchestrator")

        self.sequence_poa_out += 1
        from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
        RPCManagerSingleton.get().poa(proxy=self.reservation.get_authority(), poa=self,
                                      callback=self.reservation.get_client_callback_proxy(),
                                      caller=self.slice_object.get_owner())
        self.transition(prefix=f"Issued POA to {self.reservation.get_authority()}", state=PoaStates.Issued)

    def send_poa_info(self):
        from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
        self.sequence_poa_out += 1
        RPCManagerSingleton.get().poa_info(reservation=self.reservation, poa=self)

    def fail(self, *, message: str, notify: bool = False):
        self.transition(prefix=message, state=PoaStates.Failed)
        if notify:
            self.send_poa_info()

    def process_poa(self):
        from fabric_cf.actor.core.kernel.authority_reservation import AuthorityReservation
        if not isinstance(self.reservation, AuthorityReservation):
            raise Exception("POA can be processed only at Authority")
        self.transition(prefix=f"Performing POA", state=PoaStates.Performing)

    def accept_poa_info(self, *, incoming: Poa):
        print(f"Accepting incoming POA at Orchestrator {incoming}")
        self.transition(prefix="done", state=PoaStates.Success)

    def process_poa_info(self, *, poa_info: dict):
        print(f"KOMAL --- accept {poa_info}")

    def is_failed(self):
        return self.state == PoaStates.Failed

    def is_issued(self):
        return self.state == PoaStates.Issued

    def is_performing(self):
        return self.state == PoaStates.Performing

    def is_awaiting_response(self):
        return self.state == PoaStates.AwaitingCompletion

    def service_poa(self):
        self.transition(prefix="Triggered POA to the Handler", state=PoaStates.AwaitingCompletion)

    def clone(self):
        result = Poa(operation=self.operation, poa_id=self.poa_id, reservation=self.reservation,
                     sliver_id=self.sliver_id)

        if self.vcpu_cpu_map is not None:
            result.vcpu_cpu_map = self.vcpu_cpu_map.copy()

        if self.node_set is not None:
            result.node_set = self.node_set.copy()

        # TODO info

    def to_dict(self) -> dict:
        result = {'operation': self.operation, 'poa_id': self.poa_id}
        if self.vcpu_cpu_map is not None:
            result['vcpu_cpu_map'] = self.vcpu_cpu_map
        if self.node_set is not None:
            result['node_set'] = self.node_set
        return result

    def __str__(self):
        return f"POA ID# {self.poa_id} operation# {self.operation} in state: {self.state}"


class PoaFactory:
    @staticmethod
    def create(*, poa_id: str, operation: str, sliver_id: ID, vcpu_cpu_map: List[Dict[str, str]] = None,
               node_set: List[str] = None) -> Poa:
        """
        Create POA
        :param poa_id:
        :param operation:
        :param sliver_id:
        :param vcpu_cpu_map:
        :param node_set:
        :return:
        """
        result = Poa(poa_id=poa_id, operation=operation, vcpu_cpu_map=vcpu_cpu_map, node_set=node_set,
                     sliver_id=sliver_id)
        return result
