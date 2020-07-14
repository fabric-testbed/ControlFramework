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

from fabric.actor.core.apis.IBrokerProxy import IBrokerProxy
from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.kernel.RPCRequestType import RPCRequestType
from fabric.actor.core.proxies.kafka.KafkaProxy import KafkaProxy, KafkaProxyRequestState
from fabric.actor.core.proxies.kafka.Translate import Translate
from fabric.message_bus.messages.ClaimAvro import ClaimAvro
from fabric.message_bus.messages.ExtendTicketAvro import ExtendTicketAvro
from fabric.message_bus.messages.RelinquishAvro import RelinquishAvro
from fabric.message_bus.messages.ReservationAvro import ReservationAvro
from fabric.message_bus.messages.TicketAvro import TicketAvro

if TYPE_CHECKING:
    from fabric.actor.security.AuthToken import AuthToken
    from fabric.actor.core.apis.IRPCRequestState import IRPCRequestState
    from fabric.actor.core.apis.IClientCallbackProxy import IClientCallbackProxy
    from fabric.actor.core.apis.IReservation import IReservation


class KafkaBrokerProxy(KafkaProxy, IBrokerProxy):
    def __init__(self, kafka_topic: str, identity: AuthToken, logger):
        super().__init__(kafka_topic, identity, logger)
        self.type = KafkaProxy.TypeBroker

    def execute(self, request: IRPCRequestState):
        avro_message = None
        if request.get_type() == RPCRequestType.Ticket:
            avro_message = TicketAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.reservation = request.reservation
            avro_message.callback_topic = request.callback_topic
            avro_message.auth = Translate.translate_auth_to_avro(request.caller)

        elif request.get_type() == RPCRequestType.Claim:
            avro_message = ClaimAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.reservation = request.reservation
            avro_message.callback_topic = request.callback_topic
            avro_message.auth = Translate.translate_auth_to_avro(request.caller)

        elif request.get_type() == RPCRequestType.ExtendTicket:
            avro_message = ExtendTicketAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.reservation = request.reservation
            avro_message.callback_topic = request.callback_topic
            avro_message.auth = Translate.translate_auth_to_avro(request.caller)

        elif request.get_type() == RPCRequestType.Relinquish:
            avro_message = RelinquishAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.reservation = request.reservation
            avro_message.callback_topic = request.callback_topic
            avro_message.auth = Translate.translate_auth_to_avro(request.caller)
        else:
            return super().execute(request)

        if self.producer.produce_sync(self.kafka_topic, avro_message):
            self.logger.debug("Message {} written to {}".format(avro_message.name, self.kafka_topic))
        else:
            self.logger.error("Failed to send message {} to {}".format(avro_message.name, self.kafka_topic))

    def prepare_ticket(self, reservation: IReservation, callback: IClientCallbackProxy, caller: AuthToken) -> IRPCRequestState:
        request = KafkaProxyRequestState()
        request.reservation = self.pass_broker_reservation(reservation, caller)
        request.callback_topic = callback.get_kafka_topic()
        request.caller = caller
        return request

    def prepare_claim(self, reservation: IReservation, callback: IClientCallbackProxy, caller: AuthToken) -> IRPCRequestState:
        request = KafkaProxyRequestState()
        request.reservation = self.pass_broker_reservation(reservation, caller)
        request.callback_topic = callback.get_kafka_topic()
        request.caller = caller
        return request

    def prepare_extend_ticket(self, reservation: IReservation, callback: IClientCallbackProxy, caller: AuthToken) -> IRPCRequestState:
        request = KafkaProxyRequestState()
        request.reservation = self.pass_broker_reservation(reservation, caller)
        request.callback_topic = callback.get_kafka_topic()
        request.caller = caller
        return request

    def prepare_relinquish(self, reservation: IReservation, callback: IClientCallbackProxy, caller: AuthToken) -> IRPCRequestState:
        request = KafkaProxyRequestState()
        request.reservation = self.pass_broker_reservation(reservation, caller)
        request.callback_topic = callback.get_kafka_topic()
        request.caller = caller
        return request

    @staticmethod
    def pass_broker_reservation(reservation: IReservation, auth: AuthToken) -> ReservationAvro:
        avro_reservation = ReservationAvro()
        avro_reservation.slice = Translate.translate_slice_to_avro(reservation.get_slice())
        avro_reservation.term = Translate.translate_term(reservation.get_requested_term())
        avro_reservation.reservation_id = str(reservation.get_reservation_id())
        avro_reservation.sequence = reservation.get_ticket_sequence_out()

        rset = Translate.translate_resource_set(reservation.get_requested_resources(), Translate.DirectionAgent)
        cset = reservation.get_requested_resources().get_resources()

        encoded = None
        if cset is not None:
            encoded = cset.encode(Constants.ProtocolKafka)
            if encoded is None:
                raise Exception("Unsupported IConcreteSet: {}".format(type(cset)))

        rset.concrete = encoded

        avro_reservation.resource_set = rset

        return avro_reservation

