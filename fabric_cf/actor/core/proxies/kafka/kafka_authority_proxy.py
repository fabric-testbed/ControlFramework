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

from fabric_mb.message_bus.messages.close_avro import CloseAvro
from fabric_mb.message_bus.messages.extend_lease_avro import ExtendLeaseAvro
from fabric_mb.message_bus.messages.modify_lease_avro import ModifyLeaseAvro
from fabric_mb.message_bus.messages.redeem_avro import RedeemAvro
from fabric_mb.message_bus.messages.reservation_avro import ReservationAvro
from fabric_mb.message_bus.producer import AvroProducerApi

from fabric_cf.actor.core.apis.abc_authority_proxy import ABCAuthorityProxy
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ProxyException
from fabric_cf.actor.core.core.ticket import Ticket
from fabric_cf.actor.core.core.unit_set import UnitSet
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.proxies.kafka.kafka_broker_proxy import KafkaBrokerProxy
from fabric_cf.actor.core.proxies.kafka.kafka_proxy import KafkaProxyRequestState
from fabric_cf.actor.core.proxies.kafka.translate import Translate

if TYPE_CHECKING:
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
    from fabric_cf.actor.core.apis.abc_controller_callback_proxy import ABCControllerCallbackProxy
    from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin


class KafkaAuthorityProxy(KafkaBrokerProxy, ABCAuthorityProxy):
    def __init__(self, *, kafka_topic: str, identity: AuthToken, logger):
        super().__init__(kafka_topic=kafka_topic, identity=identity, logger=logger)
        self.type = self.TypeSite

    def execute(self, *, request: ABCRPCRequestState, producer: AvroProducerApi):
        avro_message = None
        if request.get_type() == RPCRequestType.Redeem:
            avro_message = RedeemAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.callback_topic = request.callback_topic
            avro_message.reservation = request.reservation
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        elif request.get_type() == RPCRequestType.ExtendLease:
            avro_message = ExtendLeaseAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.callback_topic = request.callback_topic
            avro_message.reservation = request.reservation
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        elif request.get_type() == RPCRequestType.ModifyLease:
            avro_message = ModifyLeaseAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.callback_topic = request.callback_topic
            avro_message.reservation = request.reservation
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        elif request.get_type() == RPCRequestType.Close:
            avro_message = CloseAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.callback_topic = request.callback_topic
            avro_message.reservation = request.reservation
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        else:
            super().execute(request=request, producer=producer)
            return

        if producer is not None and producer.produce_sync(topic=self.kafka_topic, record=avro_message):
            self.logger.debug("Message {} written to {}".format(avro_message.name, self.kafka_topic))
        else:
            self.logger.error("Failed to send message {} to {} via producer {}".format(avro_message.name,
                                                                                       self.kafka_topic, producer))

    def _prepare(self, *, reservation: ABCControllerReservation, callback: ABCControllerCallbackProxy,
                 caller: AuthToken) -> ABCRPCRequestState:
        request = KafkaProxyRequestState()
        request.callback_topic = callback.get_kafka_topic()
        request.reservation = self.pass_authority_reservation(reservation=reservation, caller=caller)
        request.caller = caller
        return request

    def prepare_redeem(self, *, reservation: ABCControllerReservation, callback: ABCControllerCallbackProxy,
                       caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, callback=callback, caller=caller)

    def prepare_extend_lease(self, *, reservation: ABCControllerReservation, callback: ABCControllerCallbackProxy,
                             caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, callback=callback, caller=caller)

    def prepare_modify_lease(self, *, reservation: ABCControllerReservation, callback: ABCControllerCallbackProxy,
                             caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, callback=callback, caller=caller)

    def prepare_close(self, *, reservation: ABCControllerReservation, callback: ABCControllerCallbackProxy,
                      caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, callback=callback, caller=caller)

    @staticmethod
    def pass_authority_reservation(reservation: ABCReservationMixin, caller: AuthToken) -> ReservationAvro:
        concrete = reservation.get_resources().get_resources()
        if concrete is None:
            raise ProxyException(Constants.NOT_SPECIFIED_PREFIX.format("ticket"))

        avro_reservation = ReservationAvro()
        avro_reservation.slice = Translate.translate_slice_to_avro(slice_obj=reservation.get_slice())
        avro_reservation.term = Translate.translate_term(term=reservation.get_requested_term())
        avro_reservation.reservation_id = str(reservation.get_reservation_id())
        avro_reservation.sequence = reservation.get_lease_sequence_out()

        rset = Translate.translate_resource_set(resource_set=reservation.get_resources())

        if concrete is not None and isinstance(concrete, Ticket):
            rset.ticket = Translate.translate_ticket(ticket=concrete)

        if concrete is not None and isinstance(concrete, UnitSet):
            rset.unit_set = Translate.translate_unit_set(unit_set=concrete)

        avro_reservation.resource_set = rset

        return avro_reservation
