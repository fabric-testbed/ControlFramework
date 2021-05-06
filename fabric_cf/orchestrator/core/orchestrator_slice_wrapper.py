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
from fim.graph.resources.neo4j_cbm import Neo4jCBMGraph
from fim.graph.slices.neo4j_asm import Neo4jASM
from fim.slivers.capacities_labels import Labels
from fim.user.topology import ExperimentTopology

from fabric_cf.orchestrator.core.reservation_converter import ReservationConverter
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.util.id import ID


class OrchestratorSliceWrapper:
    """
    Orchestrator Wrapper Around Slice to hold the computed reservations for processing by Slice Deferred Thread
    """
    def __init__(self, *, controller: ABCMgmtControllerMixin, broker: ID, slice_obj: SliceAvro, logger):
        self.controller = controller
        self.broker = broker
        self.slice_obj = slice_obj
        self.logger = logger
        self.reservation_converter = ReservationConverter(controller=controller, broker=broker)

        self.computed_reservations = None
        self.first_delete_attempt = None
        self.thread_lock = threading.Lock()

    def lock(self):
        """
        Lock slice
        :return:
        """
        self.thread_lock.acquire()

    def unlock(self):
        """
        Unlock slice
        :return:
        """
        if self.thread_lock.locked():
            self.thread_lock.release()

    def get_computed_reservations(self) -> List[TicketReservationAvro]:
        """
        Get computed reservations
        :return: computed reservations
        """
        return self.computed_reservations

    def get_all_reservations(self) -> List[ReservationMng]:
        """
        Get All reservations
        :return: all reservations
        """
        if self.controller is None:
            return None
        return self.controller.get_reservations(slice_id=ID(uid=self.slice_obj.get_slice_id()))

    def get_slice_name(self) -> str:
        """
        Get Slice name
        :return: slice name
        """
        return self.slice_obj.get_slice_name()

    def get_slice_id(self) -> ID:
        """
        Get Slice Id
        :return: slice id
        """
        return ID(uid=self.slice_obj.get_slice_id())

    def get_requested_entities(self) -> dict:
        """
        Get Requested reservations in a dictionary with reservation id as the key
        :return: dictionary reservation id -> reservation
        """
        ticketed_requested_entities = {}
        if self.get_computed_reservations() is not None:
            for reservation in self.get_computed_reservations():
                ticketed_requested_entities[reservation.get_reservation_id()] = reservation

        return ticketed_requested_entities

    def create(self, *, bqm_graph: Neo4jCBMGraph, slice_graph: Neo4jASM,
               end_time: datetime) -> List[TicketReservationAvro]:
        """
        Create a slice
        :param bqm_graph: BQM Graph
        :param slice_graph: Slice Graph
        :param end_time: End Time
        :return: List of computed reservations
        """
        try:
            # TODO - use BQM for shortest path search

            slivers = []
            for nn_id in slice_graph.get_all_network_nodes():
                sliver = slice_graph.build_deep_node_sliver(node_id=nn_id)
                slivers.append(sliver)

            self.computed_reservations = self.reservation_converter.get_tickets(slivers=slivers,
                                                                                slice_id=self.slice_obj.get_slice_id(),
                                                                                end_time=end_time)

            return self.computed_reservations
        except Exception as e:
            self.logger.error("Exception occurred while generating reservations for slivers: {}".format(e))
            raise e
