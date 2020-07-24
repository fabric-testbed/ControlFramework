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

from fabric.actor.core.kernel.AuthorityReservationFactory import AuthorityReservationFactory
from fabric.actor.core.kernel.IncomingReservationRPC import IncomingReservationRPC
from fabric.actor.core.kernel.RPCRequestType import RPCRequestType
from fabric.actor.core.proxies.kafka.Translate import Translate
from fabric.actor.core.proxies.kafka.services.BrokerService import BrokerService
from fabric.actor.core.util.ID import ID
from fabric.message_bus.messages.CloseAvro import CloseAvro
from fabric.message_bus.messages.ExtendLeaseAvro import ExtendLeaseAvro
from fabric.message_bus.messages.ModifyLeaseAvro import ModifyLeaseAvro
from fabric.message_bus.messages.RedeemAvro import RedeemAvro
from fabric.message_bus.messages.ReservationAvro import ReservationAvro
from fabric.message_bus.messages.message import IMessageAvro

if TYPE_CHECKING:
    from fabric.actor.core.apis.IAuthorityReservation import IAuthorityReservation
    from fabric.actor.core.apis.IActor import IActor


class AuthorityService(BrokerService):
    def __init__(self, actor:IActor):
        super().__init__(actor)

    def pass_authority(self, reservation: ReservationAvro) -> IAuthorityReservation:
        slice_obj = Translate.translate_slice(reservation.slice.guid, reservation.slice.slice_name)
        term = Translate.translate_term_from_avro(reservation.term)

        resource_set = Translate.translate_resource_set_from_avro(reservation.resource_set)
        cset = self.get_concrete(reservation)
        if cset is None:
            raise Exception("Unsupported Concrete type")

        resource_set.set_resources(cset)
        rid = ID(reservation.reservation_id)

        result = AuthorityReservationFactory.create(resource_set, term, slice_obj, rid)
        result.set_owner(self.actor.get_identity())
        result.set_sequence_in(reservation.sequence)

        return result

    def close(self, request: CloseAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(request.auth)
        try:
            rsvn = self.pass_authority(request.reservation)
            rpc = IncomingReservationRPC(request.message_id, RPCRequestType.Close, rsvn, None, None, authToken)
        except Exception as e:
            self.logger.error("Invalid close request: {}".format(e))
            raise e
        self.do_dispatch(rpc)

    def redeem(self, request: RedeemAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(request.auth)
        try:
            rsvn = self.pass_authority(request.reservation)
            callback = self.get_callback(request.callback_topic, authToken)
            rpc = IncomingReservationRPC(request.message_id, RPCRequestType.Redeem, rsvn, callback, None, authToken)
        except Exception as e:
            self.logger.error("Invalid redeem request: {}".format(e))
            raise e
        self.do_dispatch(rpc)

    def extend_lease(self, request: ExtendLeaseAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(request.auth)
        try:
            rsvn = self.pass_authority(request.reservation)
            rpc = IncomingReservationRPC(request.message_id, RPCRequestType.ExtendLease, rsvn, None, None, authToken)
        except Exception as e:
            self.logger.error("Invalid extend_lease request: {}".format(e))
            raise e
        self.do_dispatch(rpc)

    def modify_lease(self, request: ModifyLeaseAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(request.auth)
        try:
            rsvn = self.pass_authority(request.reservation)
            rpc = IncomingReservationRPC(request.message_id, RPCRequestType.ModifyLease, rsvn, None, None, authToken)
        except Exception as e:
            self.logger.error("Invalid modify_lease request: {}".format(e))
            raise e
        self.do_dispatch(rpc)

    def process(self, message: IMessageAvro):
        if message.get_message_name() == IMessageAvro.Close:
            self.close(message)
        elif message.get_message_name() == IMessageAvro.Redeem:
            self.redeem(message)
        elif message.get_message_name() == IMessageAvro.ExtendLease:
            self.extend_lease(message)
        elif message.get_message_name() == IMessageAvro.ModifyLease:
            self.modify_lease(message)
        elif message.get_message_name() == IMessageAvro.ClaimResourcesResponse:
            self.logger.debug("Claim Resources Response receieved: {}".format(message))
        else:
            super().process(message)