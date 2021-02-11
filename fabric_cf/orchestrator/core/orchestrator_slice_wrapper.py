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
from typing import List

from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fim.graph.neo4j_property_graph import Neo4jPropertyGraph

from fabric_cf.orchestrator.core.reservation_converter import ReservationConverter
from fabric_cf.orchestrator.core.request_workflow import RequestWorkflow
from fabric_cf.actor.core.apis.i_mgmt_controller import IMgmtController
from fabric_cf.actor.core.util.id import ID


class OrchestratorSlice:
    """
    Orchestrator Wrapper Around Slice to h
    """
    def __init__(self, *, controller: IMgmtController, broker: ID, slice_obj: SliceAvro, logger):
        self.controller = controller
        self.broker = broker
        self.slice_lock = threading.Lock()
        self.slice_obj = slice_obj
        self.logger = logger
        self.close_executed = False
        self.global_assignment_cleared = False

        self.reservation_converter = ReservationConverter(controller=controller, broker=broker)
        self.workflow = RequestWorkflow()

        self.computed_reservations = None
        self.first_delete_attempt = None

    def lock(self):
        self.slice_lock.acquire()

    def unlock(self):
        self.slice_lock.release()

    def remove_computed_reservation(self, *, rid: str):
        if self.computed_reservations is not None:
            reservation_to_remove = None
            for reservation in self.computed_reservations:
                if reservation.get_reservation_id() == rid:
                    reservation_to_remove = reservation
                    break
            if reservation_to_remove is not None:
                self.computed_reservations.remove(reservation_to_remove)

    def add_computed_reservation(self, *, reservation: TicketReservationAvro):
        if self.computed_reservations is None:
            self.computed_reservations = []
        self.computed_reservations.append(reservation)

    def set_computed_reservations(self, *, reservations: List[TicketReservationAvro]):
        self.computed_reservations = reservations

    def get_computed_reservations(self) -> List[TicketReservationAvro]:
        return self.computed_reservations

    def get_all_reservations(self) -> List[ReservationMng]:
        if self.controller is None:
            return None
        return self.controller.get_reservations()

    def get_units(self, *, rid: ID):
        return self.controller.get_reservation_units(rid=rid)

    def get_reservation_states(self, *, rids: List[ID]):
        if rids is None:
            return None
        return self.controller.get_reservation_state_for_reservations(reservation_list=rids)

    def get_workflow(self) -> RequestWorkflow:
        return self.workflow

    def get_reservation_converter(self) -> ReservationConverter:
        return self.reservation_converter

    def get_slice_name(self) -> str:
        return self.slice_obj.get_slice_name()

    def get_slice_id(self) -> ID:
        return ID(uid=self.slice_obj.get_slice_id())

    def get_user_dn(self) -> str:
        return self.slice_obj.get_owner().get_oidc_sub_claim()

    def mark_delete_attempt(self):
        if self.first_delete_attempt is None:
            self.first_delete_attempt = datetime.utcnow()

    def get_delete_attempt(self) -> datetime:
        return self.first_delete_attempt

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
                    reservation.get_slice_id(), reservation.get_reservation_id(), reservation.get_resource_type(),
                    reservation.get_units())
        else:
            result += "No new reservations were computed\n"

        if len(result) == 0:
            result += "No result available"

        return result

    def create(self, *, bqm_graph: Neo4jPropertyGraph, slice_graph: str) -> List[TicketReservationAvro]:
        try:
            slivers = self.workflow.run(bqm=bqm_graph, slice_graph=slice_graph)
            self.computed_reservations = self.reservation_converter.get_tickets(slivers=slivers,
                                                                                slice_id=self.slice_obj.get_slice_id())
            return self.computed_reservations
        except Exception as e:
            self.logger.error("Exception occurred while running embedding e: {}".format(e))
            raise e