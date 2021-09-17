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

from fabric_mb.message_bus.messages.claim_delegation_avro import ClaimDelegationAvro
from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.extend_ticket_avro import ExtendTicketAvro
from fabric_mb.message_bus.messages.reclaim_delegation_avro import ReclaimDelegationAvro
from fabric_mb.message_bus.messages.relinquish_avro import RelinquishAvro
from fabric_mb.message_bus.messages.reservation_avro import ReservationAvro
from fabric_mb.message_bus.messages.ticket_avro import TicketAvro
from fabric_mb.message_bus.producer import AvroProducerApi

from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.core.ticket import Ticket
from fabric_cf.actor.core.core.unit_set import UnitSet
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.proxies.kafka.kafka_proxy import KafkaProxy, KafkaProxyRequestState
from fabric_cf.actor.core.proxies.kafka.translate import Translate

if TYPE_CHECKING:
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
    from fabric_cf.actor.core.apis.abc_client_callback_proxy import ABCClientCallbackProxy
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin


class KafkaBrokerProxy(KafkaProxy, ABCBrokerProxy):
    def __init__(self, *, kafka_topic: str, identity: AuthToken, logger):
        super().__init__(kafka_topic=kafka_topic, identity=identity, logger=logger)
        self.type = KafkaProxy.TypeBroker

    def execute(self, *, request: ABCRPCRequestState, producer: AvroProducerApi):
        avro_message = None
        if request.get_type() == RPCRequestType.Ticket:
            avro_message = TicketAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.reservation = request.reservation
            avro_message.callback_topic = request.callback_topic
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        elif request.get_type() == RPCRequestType.ClaimDelegation:
            avro_message = ClaimDelegationAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.delegation = request.delegation
            avro_message.callback_topic = request.callback_topic
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        elif request.get_type() == RPCRequestType.ReclaimDelegation:
            avro_message = ReclaimDelegationAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.delegation = request.delegation
            avro_message.callback_topic = request.callback_topic
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        elif request.get_type() == RPCRequestType.ExtendTicket:
            avro_message = ExtendTicketAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.reservation = request.reservation
            avro_message.callback_topic = request.callback_topic
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        elif request.get_type() == RPCRequestType.Relinquish:
            avro_message = RelinquishAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.reservation = request.reservation
            avro_message.callback_topic = request.callback_topic
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)
        else:
            super().execute(request=request, producer=producer)
            return

        if producer is not None and producer.produce(topic=self.kafka_topic, record=avro_message):
            self.logger.debug("Message {} written to {}".format(avro_message.name, self.kafka_topic))
        else:
            self.logger.error("Failed to send message {} to {} via producer {}".format(avro_message.name,
                                                                                       self.kafka_topic, producer))

    def _prepare_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy,
                            caller: AuthToken, id_token: str = None) -> ABCRPCRequestState:
        request = KafkaProxyRequestState()
        request.delegation = self.pass_broker_delegation(delegation=delegation, auth=caller)
        request.callback_topic = callback.get_kafka_topic()
        request.caller = caller
        request.id_token = id_token
        return request

    def prepare_claim_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy,
                                 caller: AuthToken, id_token: str = None) -> ABCRPCRequestState:
        return self._prepare_delegation(delegation=delegation, callback=callback, caller=caller, id_token=id_token)

    def prepare_reclaim_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy,
                                   caller: AuthToken, id_token: str = None) -> ABCRPCRequestState:
        return self._prepare_delegation(delegation=delegation, callback=callback, caller=caller, id_token=id_token)

    def _prepare(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy,
                 caller: AuthToken) -> ABCRPCRequestState:
        request = KafkaProxyRequestState()
        request.reservation = self.pass_broker_reservation(reservation=reservation, auth=caller)
        request.callback_topic = callback.get_kafka_topic()
        request.caller = caller
        return request

    def prepare_ticket(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy,
                       caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, callback=callback, caller=caller)

    def prepare_extend_ticket(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy,
                              caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, callback=callback, caller=caller)

    def prepare_relinquish(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy,
                           caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, callback=callback, caller=caller)

    @staticmethod
    def pass_broker_reservation(reservation: ABCReservationMixin, auth: AuthToken) -> ReservationAvro:
        avro_reservation = ReservationAvro()
        avro_reservation.slice = Translate.translate_slice_to_avro(slice_obj=reservation.get_slice())
        avro_reservation.term = Translate.translate_term(term=reservation.get_requested_term())
        avro_reservation.reservation_id = str(reservation.get_reservation_id())
        avro_reservation.sequence = reservation.get_ticket_sequence_out()

        rset = Translate.translate_resource_set(resource_set=reservation.get_requested_resources())

        if reservation.get_requested_resources() is not None:
            cset = reservation.get_requested_resources().get_resources()

            if cset is not None and isinstance(cset, Ticket):
                rset.ticket = Translate.translate_ticket(ticket=cset)

            if cset is not None and isinstance(cset, UnitSet):
                rset.units = Translate.translate_unit_set(unit_set=cset)

        avro_reservation.resource_set = rset

        return avro_reservation

    @staticmethod
    def pass_broker_delegation(delegation: ABCDelegation, auth: AuthToken) -> DelegationAvro:
        avro_delegation = Translate.translate_delegation_to_avro(delegation=delegation)
        return avro_delegation
