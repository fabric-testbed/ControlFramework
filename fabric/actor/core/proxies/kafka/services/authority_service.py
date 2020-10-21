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

from fabric.actor.core.kernel.authority_reservation_factory import AuthorityReservationFactory
from fabric.actor.core.kernel.incoming_reservation_rpc import IncomingReservationRPC
from fabric.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric.actor.core.proxies.kafka.translate import Translate
from fabric.actor.core.proxies.kafka.services.broker_service import BrokerService
from fabric.actor.core.util.id import ID
from fabric.message_bus.messages.close_avro import CloseAvro
from fabric.message_bus.messages.extend_lease_avro import ExtendLeaseAvro
from fabric.message_bus.messages.modify_lease_avro import ModifyLeaseAvro
from fabric.message_bus.messages.redeem_avro import RedeemAvro
from fabric.message_bus.messages.reservation_avro import ReservationAvro
from fabric.message_bus.messages.message import IMessageAvro

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
    from fabric.actor.core.apis.i_actor import IActor


class AuthorityService(BrokerService):
    def __init__(self, *, actor:IActor):
        super().__init__(actor=actor)

    def pass_authority(self, *, reservation: ReservationAvro) -> IAuthorityReservation:
        slice_obj = Translate.translate_slice(slice_id=reservation.slice.guid, slice_name=reservation.slice.slice_name)
        term = Translate.translate_term_from_avro(term=reservation.term)

        resource_set = Translate.translate_resource_set_from_avro(rset=reservation.resource_set)
        cset = self.get_concrete(reservation=reservation)
        if cset is None:
            raise Exception("Unsupported Concrete type")

        resource_set.set_resources(cset=cset)
        rid = ID(id=reservation.reservation_id)

        result = AuthorityReservationFactory.create(resources=resource_set, term=term, slice_obj=slice_obj, rid=rid)
        result.set_owner(owner=self.actor.get_identity())
        result.set_sequence_in(sequence=reservation.sequence)

        return result

    def close(self, *, request: CloseAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            rsvn = self.pass_authority(reservation=request.reservation)
            rpc = IncomingReservationRPC(message_id=ID(id=request.message_id), request_type=RPCRequestType.Close,
                                         reservation=rsvn, caller=authToken)
        except Exception as e:
            self.logger.error("Invalid close request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def redeem(self, *, request: RedeemAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            rsvn = self.pass_authority(reservation=request.reservation)
            callback = self.get_callback(kafka_topic=request.callback_topic, auth=authToken)
            rpc = IncomingReservationRPC(message_id=ID(id=request.message_id), request_type=RPCRequestType.Redeem,
                                         reservation=rsvn, callback=callback, caller=authToken)
        except Exception as e:
            self.logger.error("Invalid redeem request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def extend_lease(self, *, request: ExtendLeaseAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            rsvn = self.pass_authority(reservation=request.reservation)
            rpc = IncomingReservationRPC(message_id=ID(id=request.message_id),
                                         request_type=RPCRequestType.ExtendLease, reservation=rsvn, caller=authToken)
        except Exception as e:
            self.logger.error("Invalid extend_lease request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def modify_lease(self, *, request: ModifyLeaseAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            rsvn = self.pass_authority(reservation=request.reservation)
            rpc = IncomingReservationRPC(message_id=ID(id=request.message_id),
                                         request_type=RPCRequestType.ModifyLease, reservation=rsvn, caller=authToken)
        except Exception as e:
            self.logger.error("Invalid modify_lease request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def process(self, *, message: IMessageAvro):
        if message.get_message_name() == IMessageAvro.Close:
            self.close(request=message)
        elif message.get_message_name() == IMessageAvro.Redeem:
            self.redeem(request=message)
        elif message.get_message_name() == IMessageAvro.ExtendLease:
            self.extend_lease(request=message)
        elif message.get_message_name() == IMessageAvro.ModifyLease:
            self.modify_lease(request=message)
        elif message.get_message_name() == IMessageAvro.ResultReservation:
            self.logger.debug("Claim Resources Response receieved: {}".format(message))
        elif message.get_message_name() == IMessageAvro.ResultDelegation:
            self.logger.debug("Claim Delegation Response receieved: {}".format(message))
        else:
            super().process(message=message)