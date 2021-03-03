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
from typing import List

from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fim.graph.neo4j_property_graph import Neo4jPropertyGraph

from fabric_cf.orchestrator.core.reservation_converter import ReservationConverter
from fabric_cf.orchestrator.core.request_workflow import RequestWorkflow
from fabric_cf.actor.core.apis.i_mgmt_controller import IMgmtController
from fabric_cf.actor.core.util.id import ID


class OrchestratorSliceWrapper:
    """
    Orchestrator Wrapper Around Slice to hold the computed reservations for processing by Slice Deferred Thread
    """
    def __init__(self, *, controller: IMgmtController, broker: ID, slice_obj: SliceAvro, logger):
        self.controller = controller
        self.broker = broker
        self.slice_obj = slice_obj
        self.logger = logger
        self.reservation_converter = ReservationConverter(controller=controller, broker=broker)
        self.workflow = RequestWorkflow(logger=logger)

        self.computed_reservations = None
        self.first_delete_attempt = None
        self.thread_lock = threading.Lock()

    def lock(self):
        self.thread_lock.acquire()

    def unlock(self):
        if self.thread_lock.locked():
            self.thread_lock.release()

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

    def get_slice_name(self) -> str:
        return self.slice_obj.get_slice_name()

    def get_slice_id(self) -> ID:
        return ID(uid=self.slice_obj.get_slice_id())

    def get_requested_entities(self) -> dict:
        ticketed_requested_entities = {}
        if self.get_computed_reservations() is not None:
            for reservation in self.get_computed_reservations():
                ticketed_requested_entities[reservation.get_reservation_id()] = reservation

        return ticketed_requested_entities

    def create(self, *, bqm_graph: Neo4jPropertyGraph, slice_graph: str) -> List[TicketReservationAvro]:
        try:
            slivers = self.workflow.run(bqm=bqm_graph, slice_graph=slice_graph)
            self.computed_reservations = self.reservation_converter.get_tickets(slivers=slivers,
                                                                                slice_id=self.slice_obj.get_slice_id())
            return self.computed_reservations
        except Exception as e:
            self.logger.error("Exception occurred while running embedding e: {}".format(e))
            raise e