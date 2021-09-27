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

from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.reservation_avro import ReservationAvro
from fabric_mb.message_bus.messages.update_delegation_avro import UpdateDelegationAvro
from fabric_mb.message_bus.messages.update_lease_avro import UpdateLeaseAvro
from fabric_mb.message_bus.messages.update_ticket_avro import UpdateTicketAvro
from fabric_mb.message_bus.producer import AvroProducerApi

from fabric_cf.actor.core.apis.abc_controller_callback_proxy import ABCControllerCallbackProxy
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.core.ticket import Ticket
from fabric_cf.actor.core.core.unit_set import UnitSet
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.proxies.kafka.kafka_proxy import KafkaProxy, KafkaProxyRequestState
from fabric_cf.actor.core.proxies.kafka.translate import Translate

if TYPE_CHECKING:
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
    from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
    from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
    from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
    from fabric_cf.actor.core.apis.abc_server_reservation import ABCServerReservation
    from fabric_cf.actor.core.util.update_data import UpdateData


class KafkaReturn(KafkaProxy, ABCControllerCallbackProxy):
    def __init__(self, *, kafka_topic: str, identity: AuthToken, logger):
        super().__init__(kafka_topic=kafka_topic, identity=identity, logger=logger)
        self.type = KafkaProxy.TypeReturn
        self.callback = True

    def execute(self, *, request: ABCRPCRequestState, producer: AvroProducerApi):
        avro_message = None
        if request.get_type() == RPCRequestType.UpdateTicket:
            avro_message = UpdateTicketAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.reservation = request.reservation
            avro_message.callback_topic = request.callback_topic
            avro_message.update_data = request.udd
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        elif request.get_type() == RPCRequestType.UpdateLease:
            avro_message = UpdateLeaseAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.reservation = request.reservation
            avro_message.callback_topic = request.callback_topic
            avro_message.update_data = request.udd
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        elif request.get_type() == RPCRequestType.UpdateDelegation:
            avro_message = UpdateDelegationAvro()
            avro_message.message_id = str(request.get_message_id())
            avro_message.delegation = request.delegation
            avro_message.callback_topic = request.callback_topic
            avro_message.update_data = request.udd
            avro_message.auth = Translate.translate_auth_to_avro(auth=request.caller)

        else:
            super().execute(request=request, producer=producer)
            return

        if producer is not None and producer.produce(topic=self.kafka_topic, record=avro_message):
            self.logger.debug("Message {} written to {}".format(avro_message.name, self.kafka_topic))
        else:
            self.logger.error("Failed to send message {} to {} via producer {}".format(avro_message.name,
                                                                                       self.kafka_topic, producer))

    def prepare_update_delegation(self, *, delegation: ABCDelegation, update_data: UpdateData,
                                  callback: ABCCallbackProxy, caller: AuthToken) -> ABCRPCRequestState:
        request = KafkaProxyRequestState()
        request.delegation = self.pass_delegation(delegation=delegation, auth=caller)
        request.udd = Translate.translate_udd(udd=update_data)
        request.callback_topic = callback.get_kafka_topic()
        request.caller = caller
        return request

    def _prepare(self, *, reservation: ABCServerReservation, update_data: UpdateData,
                 callback: ABCCallbackProxy, caller: AuthToken) -> ABCRPCRequestState:
        request = KafkaProxyRequestState()
        request.reservation = self.pass_reservation(reservation=reservation, auth=caller)
        request.udd = Translate.translate_udd(udd=update_data)
        request.callback_topic = callback.get_kafka_topic()
        request.caller = caller
        return request

    def prepare_update_ticket(self, *, reservation: ABCBrokerReservation, update_data: UpdateData,
                              callback: ABCCallbackProxy, caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, update_data=update_data, callback=callback, caller=caller)

    def prepare_update_lease(self, *, reservation: ABCAuthorityReservation, update_data: UpdateData,
                             callback: ABCCallbackProxy, caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, update_data=update_data, callback=callback, caller=caller)

    @staticmethod
    def pass_reservation(reservation: ABCServerReservation, auth: AuthToken) -> ReservationAvro:
        avro_reservation = ReservationAvro()
        avro_reservation.slice = Translate.translate_slice_to_avro(slice_obj=reservation.get_slice())
        term = None
        if reservation.get_term() is None:
            term = reservation.get_requested_term().clone()
        else:
            term = reservation.get_term().clone()

        avro_reservation.term = Translate.translate_term(term=term)
        avro_reservation.reservation_id = str(reservation.get_reservation_id())

        rset = None
        if reservation.get_resources() is None:
            from fabric_cf.actor.core.kernel.resource_set import ResourceSet
            rset = Translate.translate_resource_set(resource_set=ResourceSet(units=0,
                                                                             rtype=reservation.get_requested_type()))
        else:
            rset = Translate.translate_resource_set(resource_set=reservation.get_resources())
        if reservation.get_resources() is not None:
            cset = reservation.get_resources().get_resources()

            if cset is not None and isinstance(cset, Ticket):
                rset.ticket = Translate.translate_ticket(ticket=cset)

            if cset is not None and isinstance(cset, UnitSet):
                rset.unit_set = Translate.translate_unit_set(unit_set=cset)

        avro_reservation.resource_set = rset
        return avro_reservation

    @staticmethod
    def pass_delegation(delegation: ABCDelegation, auth: AuthToken) -> DelegationAvro:
        avro_delegation = Translate.translate_delegation_to_avro(delegation=delegation)
        avro_delegation.sequence = delegation.get_sequence_out()
        return avro_delegation
