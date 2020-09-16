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
from typing import TYPE_CHECKING

from fabric.actor.core.kernel.broker_reservation_factory import BrokerReservationFactory
from fabric.actor.core.kernel.incoming_reservation_rpc import IncomingReservationRPC
from fabric.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric.actor.core.proxies.kafka.translate import Translate
from fabric.actor.core.proxies.kafka.services.actor_service import ActorService
from fabric.actor.core.util.id import ID
from fabric.message_bus.messages.reclaim_avro import ReclaimAvro
from fabric.message_bus.messages.reservation_avro import ReservationAvro
from fabric.message_bus.messages.message import IMessageAvro

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
    from fabric.message_bus.messages.extend_ticket_avro import ExtendTicketAvro
    from fabric.message_bus.messages.relinquish_avro import RelinquishAvro
    from fabric.message_bus.messages.ticket_avro import TicketAvro
    from fabric.message_bus.messages.claim_avro import ClaimAvro
    from fabric.actor.core.apis.i_actor import IActor


class BrokerService(ActorService):
    def __init__(self, actor:IActor):
        super().__init__(actor)

    def pass_agent(self, reservation: ReservationAvro) -> IBrokerReservation:
        slice_obj = Translate.translate_slice(reservation.slice.guid, reservation.slice.slice_name)
        term = Translate.translate_term_from_avro(reservation.term)
        resource_set = Translate.translate_resource_set_from_avro(reservation.resource_set)
        rid = ID(reservation.reservation_id)

        result = BrokerReservationFactory.create(rid, resource_set, term, slice_obj)
        result.set_owner(self.actor.get_identity())
        result.set_sequence_in(reservation.sequence)

        return result

    def ticket(self, request: TicketAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(request.auth)
        try:
            rsvn = self.pass_agent(request.reservation)
            callback = self.get_callback(request.callback_topic, authToken)
            rpc = IncomingReservationRPC(request.message_id, RPCRequestType.Ticket, rsvn, callback, None, authToken)
        except Exception as e:
            self.logger.error("Invalid Ticket request: {}".format(e))
            raise e
        self.do_dispatch(rpc)

    def claim(self, request: ClaimAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(request.auth)
        try:
            rsvn = self.pass_agent(request.reservation)
            callback = self.get_callback(request.callback_topic, authToken)
            rpc = IncomingReservationRPC(request.message_id, RPCRequestType.Claim, rsvn, callback, None, authToken)
        except Exception as e:
            self.logger.error("Invalid Claim request: {}".format(e))
            raise e
        self.do_dispatch(rpc)

    def reclaim(self, request: ReclaimAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(request.auth)
        try:
            rsvn = self.pass_agent(request.reservation)
            callback = self.get_callback(request.callback_topic, authToken)
            rpc = IncomingReservationRPC(request.message_id, RPCRequestType.Reclaim, rsvn, callback, None, authToken)
        except Exception as e:
            self.logger.error("Invalid reclaim request: {}".format(e))
            raise e
        self.do_dispatch(rpc)

    def extend_ticket(self, request: ExtendTicketAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(request.auth)
        try:
            rsvn = self.pass_agent(request.reservation)
            callback = self.get_callback(request.callback_topic, authToken)
            rpc = IncomingReservationRPC(request.message_id, RPCRequestType.ExtendTicket, rsvn, None, None, authToken)
        except Exception as e:
            self.logger.error("Invalid extend_ticket request: {}".format(e))
            raise e
        self.do_dispatch(rpc)

    def relinquish(self, request: RelinquishAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(request.auth)
        try:
            rsvn = self.pass_agent(request.reservation)
            callback = self.get_callback(request.callback_topic, authToken)
            rpc = IncomingReservationRPC(request.message_id, RPCRequestType.Relinquish, rsvn, None, None, authToken)
        except Exception as e:
            self.logger.error("Invalid extend_ticket request: {}".format(e))
            raise e
        self.do_dispatch(rpc)

    def process(self, message: IMessageAvro):
        if message.get_message_name() == IMessageAvro.Ticket:
            self.ticket(message)
        elif message.get_message_name() == IMessageAvro.Claim:
            self.claim(message)
        elif message.get_message_name() == IMessageAvro.Reclaim:
            self.reclaim(message)
        elif message.get_message_name() == IMessageAvro.ExtendTicket:
            self.extend_ticket(message)
        elif message.get_message_name() == IMessageAvro.Relinquish:
            self.relinquish(message)
        else:
            super().process(message)