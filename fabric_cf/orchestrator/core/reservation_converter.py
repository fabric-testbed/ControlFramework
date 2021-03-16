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
from datetime import datetime, timedelta
from typing import List

from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fim.slivers.network_node import NodeSliver

from fabric_cf.actor.core.apis.i_mgmt_controller import IMgmtController
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.util.id import ID


class ReservationConverter:
    """
    Class responsible for computing reservations from slivers
    """
    def __init__(self, *, controller: IMgmtController, broker: ID):
        self.controller = controller
        self.broker = broker

    def get_tickets(self, *, slivers: List[NodeSliver], slice_id: str) -> List[TicketReservationAvro]:
        """
        Responsible to generate reservations from the slivers; Adds the reservation Orchestrator
        :param slivers list of slivers computed from the ASM (Slice graph)
        :param slice_id Slice Id

        :returns list of tickets
        """
        reservation_list = []
        for sliver in slivers:
            ticket = TicketReservationAvro()
            ticket.set_slice_id(slice_id)
            ticket.set_broker(str(self.broker))
            ticket.set_units(1)
            ticket.set_resource_type(str(sliver.get_type()))
            start = datetime.utcnow()
            end = start + timedelta(hours=24)
            ticket.set_start(ActorClock.to_milliseconds(when=start))
            ticket.set_end(ActorClock.to_milliseconds(when=end))
            ticket.set_state(ReservationStates.Unknown.value)
            ticket.set_pending_state(ReservationPendingStates.None_.value)
            ticket.set_sliver(sliver)

            # Add reservation to Orchestrator
            reservation_id = self.controller.add_reservation(reservation=ticket)
            ticket.set_reservation_id(str(reservation_id))
            reservation_list.append(ticket)
        return reservation_list
