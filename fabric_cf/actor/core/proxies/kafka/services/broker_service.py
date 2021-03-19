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
from fabric_mb.message_bus.messages.reclaim_delegation_avro import ReclaimDelegationAvro
from fabric_mb.message_bus.messages.reservation_avro import ReservationAvro
from fabric_mb.message_bus.messages.message import IMessageAvro

from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.delegation.delegation_factory import DelegationFactory
from fabric_cf.actor.core.kernel.broker_reservation import BrokerReservationFactory
from fabric_cf.actor.core.kernel.incoming_delegation_rpc import IncomingDelegationRPC
from fabric_cf.actor.core.kernel.incoming_reservation_rpc import IncomingReservationRPC
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.proxies.kafka.translate import Translate
from fabric_cf.actor.core.proxies.kafka.services.actor_service import ActorService
from fabric_cf.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.extend_ticket_avro import ExtendTicketAvro
    from fabric_mb.message_bus.messages.relinquish_avro import RelinquishAvro
    from fabric_mb.message_bus.messages.ticket_avro import TicketAvro
    from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation


class BrokerService(ActorService):
    def pass_agent(self, *, reservation: ReservationAvro) -> ABCBrokerReservation:
        slice_obj = Translate.translate_slice(slice_avro=reservation.slice)
        term = Translate.translate_term_from_avro(term=reservation.term)
        resource_set = Translate.translate_resource_set_from_avro(rset=reservation.resource_set)
        rid = ID(uid=reservation.reservation_id)

        result = BrokerReservationFactory.create(rid=rid, resources=resource_set, term=term, slice_obj=slice_obj,
                                                 actor=self.actor)
        result.set_owner(owner=self.actor.get_identity())
        result.set_sequence_in(sequence=reservation.sequence)

        return result

    def pass_agent_delegation(self, *, delegation: DelegationAvro) -> ABCDelegation:
        slice_obj = Translate.translate_slice(slice_avro=delegation.slice)

        result = DelegationFactory.create(did=delegation.get_delegation_id(), slice_id=slice_obj.get_slice_id())
        result.set_slice_object(slice_object=slice_obj)
        result.set_owner(owner=self.actor.get_identity())
        result.set_sequence_in(value=delegation.sequence)

        return result

    def ticket(self, *, request: TicketAvro):
        rpc = None
        auth_token = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            rsvn = self.pass_agent(reservation=request.reservation)
            callback = self.get_callback(kafka_topic=request.callback_topic, auth=auth_token)
            rpc = IncomingReservationRPC(message_id=ID(uid=request.message_id), request_type=RPCRequestType.Ticket,
                                         reservation=rsvn, callback=callback, caller=auth_token)
        except Exception as e:
            self.logger.error("Invalid Ticket request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def claim_delegation(self, *, request: ClaimDelegationAvro):
        rpc = None
        auth_token = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            dlg = self.pass_agent_delegation(delegation=request.delegation)
            callback = self.get_callback(kafka_topic=request.callback_topic, auth=auth_token)
            rpc = IncomingDelegationRPC(message_id=ID(uid=request.message_id),
                                        request_type=RPCRequestType.ClaimDelegation,
                                        delegation=dlg, callback=callback, caller=auth_token)
        except Exception as e:
            self.logger.error("Invalid Claim request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def reclaim_delegation(self, *, request: ReclaimDelegationAvro):
        rpc = None
        auth_token = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            dlg = self.pass_agent_delegation(delegation=request.delegation)
            callback = self.get_callback(kafka_topic=request.callback_topic, auth=auth_token)
            rpc = IncomingDelegationRPC(message_id=ID(uid=request.message_id),
                                        request_type=RPCRequestType.ReclaimDelegation,
                                        delegation=dlg, callback=callback, caller=auth_token)
        except Exception as e:
            self.logger.error("Invalid reclaim request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def extend_ticket(self, *, request: ExtendTicketAvro):
        rpc = None
        auth_token = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            rsvn = self.pass_agent(reservation=request.reservation)
            callback = self.get_callback(kafka_topic=request.callback_topic, auth=auth_token)
            rpc = IncomingReservationRPC(message_id=ID(uid=request.message_id), request_type=RPCRequestType.ExtendTicket,
                                         reservation=rsvn, caller=auth_token, callback=callback)
        except Exception as e:
            self.logger.error("Invalid extend_ticket request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def relinquish(self, *, request: RelinquishAvro):
        rpc = None
        auth_token = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            rsvn = self.pass_agent(reservation=request.reservation)
            callback = self.get_callback(kafka_topic=request.callback_topic, auth=auth_token)
            rpc = IncomingReservationRPC(message_id=ID(uid=request.message_id), request_type=RPCRequestType.Relinquish,
                                         reservation=rsvn, caller=auth_token, callback=callback)
        except Exception as e:
            self.logger.error("Invalid extend_ticket request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def process(self, *, message: IMessageAvro):
        if message.get_message_name() == IMessageAvro.ticket:
            self.ticket(request=message)
        elif message.get_message_name() == IMessageAvro.claim_delegation:
            self.claim_delegation(request=message)
        elif message.get_message_name() == IMessageAvro.reclaim_delegation:
            self.reclaim_delegation(request=message)
        elif message.get_message_name() == IMessageAvro.extend_ticket:
            self.extend_ticket(request=message)
        elif message.get_message_name() == IMessageAvro.relinquish:
            self.relinquish(request=message)
        else:
            super().process(message=message)
