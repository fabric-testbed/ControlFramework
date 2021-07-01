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
from typing import List, Tuple, Dict

from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric_mb.message_bus.messages.reservation_predecessor_avro import ReservationPredecessorAvro
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.network_node import NodeSliver

from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.util.id import ID


class ReservationConverter:
    """
    Class responsible for computing reservations from slivers
    """
    def __init__(self, *, controller: ABCMgmtControllerMixin, broker: ID):
        self.controller = controller
        self.broker = broker

    def generate_reservation(self, *, sliver: BaseSliver, slice_id: str, end_time: datetime,
                             pred_list: List[str] = None) -> TicketReservationAvro:
        """
        Responsible to generate reservation from the sliver
        :param sliver Network Service or Network Node Sliver
        :param slice_id Slice Id
        :param end_time End Time
        :param pred_list Predecessor Reservation Id List
        :returns list of tickets
        """
        ticket = None
        if pred_list is not None:
            ticket = LeaseReservationAvro()
            ticket.redeem_processors = []
            for rid in pred_list:
                pred = ReservationPredecessorAvro()
                pred.set_reservation_id(value=rid)
                ticket.redeem_processors.append(pred)
        else:
            ticket = TicketReservationAvro()
        ticket.set_slice_id(slice_id)
        ticket.set_broker(str(self.broker))
        ticket.set_units(1)
        ticket.set_resource_type(str(sliver.get_type()))
        start = datetime.utcnow()
        end = start + timedelta(hours=Constants.DEFAULT_LEASE_IN_HOURS)
        if end_time is not None:
            end = end_time
        ticket.set_start(ActorClock.to_milliseconds(when=start))
        ticket.set_end(ActorClock.to_milliseconds(when=end))
        ticket.set_state(ReservationStates.Unknown.value)
        ticket.set_pending_state(ReservationPendingStates.None_.value)
        ticket.set_sliver(sliver)
        ticket.set_reservation_id(value=str(ID()))
        return ticket

