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
from datetime import datetime

from fabric.orchestrator.core.slice_state_machine import SliceStateMachine, SliceState
from fabric.message_bus.messages.reservation_mng import ReservationMng
from fabric.message_bus.messages.ticket_reservation_avro import TicketReservationAvro

from fabric.orchestrator.core.request_workflow import RequestWorkflow
from fabric.orchestrator.core.reservation_converter import ReservationConverter
from fabric.actor.core.apis.i_mgmt_controller import IMgmtController
from fabric.actor.core.util.id import ID
from fabric.message_bus.messages.slice_avro import SliceAvro


class OrchestratorSlice:
    def __init__(self, controller: IMgmtController, slice_obj: SliceAvro, name: str, user_dn: str, ssh_credentials: list, recover: bool):
        self.slice_lock = threading.Lock()
        self.state_machine = SliceStateMachine(ID(slice_obj.get_slice_id()), recover)
        self.slice_urn = name
        self.user_dn = user_dn
        self.ssh_credentials = ssh_credentials
        self.computed_reservations = None
        self.slice_obj = slice_obj
        self.workflow = None
        self.reservation_converter = None
        self.first_delete_attempt = None
        self.close_executed = False
        self.global_assignment_cleared = False

        try:
            cloud_handler = None
            # TODO
            # initialize the cloud handler
            self.workflow = RequestWorkflow(cloud_handler)
            self.reservation_converter = ReservationConverter(self.ssh_credentials, controller, self)
        except Exception as e:
            raise e

    def lock(self):
        self.slice_lock.acquire()

    def unlock(self):
        self.slice_lock.release()

    def remove_computed_reservations(self, rid: str):
        if self.computed_reservations is not None:
            reservation_to_remove = None
            for reservation in self.computed_reservations:
                if reservation.get_reservation_id() == rid:
                    reservation_to_remove = reservation
                    break
            if reservation_to_remove is not None:
                self.computed_reservations.remove(reservation_to_remove)

    def add_computed_reservations(self, reservation: TicketReservationAvro):
        if self.computed_reservations is None:
            self.computed_reservations = []
        self.computed_reservations.append(reservation)

    def set_computed_reservations(self, reservations: list):
        self.computed_reservations = reservations

    def get_computed_reservations(self) -> list:
        return self.computed_reservations

    def get_all_reservations(self, controller: IMgmtController) -> list:
        if controller is None:
            return None

        return controller.get_reservations()

    def get_units(self, controller: IMgmtController, rid: ID):
        try:
            controller.get_reservation_units(rid)
        except Exception as e:
            raise e

    def get_reservation_states(self, controller: IMgmtController, rids: list):
        try:
            if rids is None:
                return None
            return controller.get_reservation_state_for_reservations(rids)
        except Exception as e:
            raise e

    def get_workflow(self) -> RequestWorkflow:
        return self.workflow

    def get_user_dn(self) -> str:
        return self.user_dn

    def match_user_dn(self, dn: str):
        if dn is None:
            return False
        return self.user_dn == dn

    def get_slice_urn(self) -> str:
        return self.slice_urn

    def get_slice_id(self) -> ID:
        return ID(self.slice_obj.get_slice_id())

    def get_reservation_converter(self) -> ReservationConverter:
        return self.reservation_converter

    def close(self):
        if not self.global_assignment_cleared and \
                (self.state_machine.get_state() == SliceState.Dead or self.state_machine.get_state() == SliceState.Closing):
            self.workflow.close()

        if self.computed_reservations is not None:
            self.computed_reservations.clear()
            self.computed_reservations = None

    def get_state_machine(self) -> SliceStateMachine:
        return self.state_machine

    def is_stable_ok(self) -> bool:
        slice_state = self.reevaluate()

        if slice_state == SliceState.StableOk:
            return True

        return False

    def is_stable_error(self) -> bool:
        slice_state = self.reevaluate()

        if slice_state == SliceState.StableError:
            return True

        return False

    def is_stable(self) -> bool:
        slice_state = self.reevaluate()

        if slice_state == SliceState.StableError or slice_state == SliceState.StableOk:
            return True

        return False

    def is_dead_or_closing(self) -> bool:
        slice_state = self.reevaluate()

        if slice_state == SliceState.Dead or slice_state == SliceState.Closing:
            return True

        return False

    def is_dead(self) -> bool:
        slice_state = self.reevaluate()

        if slice_state == SliceState.Dead:
            return True

        return False

    def all_failed(self):
        return self.state_machine.all_failed()

    def mark_delete_attempt(self):
        if self.first_delete_attempt is None:
            self.first_delete_attempt = datetime.utcnow()

    def get_delete_attempt(self) -> datetime:
        return self.first_delete_attempt

    def reevaluate(self) -> SliceState:
        return self.state_machine.transition_slice(SliceStateMachine.REEVALUATE)

    def recover(self):
        # TODO
        return

    def add_recover_reservation(self, r: ReservationMng):
        # TODO
        return

    def get_requested_entities(self) -> dict:
        ticketed_requested_entities = {}
        if self.get_computed_reservations() is not None:
            for reservation in self.get_computed_reservations():
                ticketed_requested_entities[reservation.get_reservation_id()] = reservation

        return ticketed_requested_entities

    def get_computed_reservation_summary(self) -> str:
        result = ""

        if self.get_computed_reservations() is not None:
            for reservation in self.get_computed_reservations():
                result += "[ Slice UID: {} | Reservation UID: {} | Resource Type: {} | Resource Units: {} ]\n".format(
                    reservation.get_slice_id(), reservation.get_reservation_id(), reservation.get_type(),
                    reservation.get_units())
        else:
            result += "No new reservations were computed\n"

        if len(result) == 0:
            result += "No result available"

        return result
