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

import pickle
from typing import TYPE_CHECKING

from fabric.actor.core.apis.i_concrete_set import IConcreteSet
from fabric.actor.core.kernel.client_reservation_factory import ClientReservationFactory
from fabric.actor.core.kernel.incoming_failed_rpc import IncomingFailedRPC
from fabric.actor.core.kernel.incoming_query_rpc import IncomingQueryRPC
from fabric.actor.core.kernel.incoming_rpc import IncomingRPC
from fabric.actor.core.kernel.incoming_reservation_rpc import IncomingReservationRPC
from fabric.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
from fabric.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric.actor.core.proxies.kafka.kafka_retun import KafkaReturn
from fabric.actor.core.proxies.kafka.translate import Translate
from fabric.actor.core.util.id import ID
from fabric.message_bus.messages.reservation_avro import ReservationAvro
from fabric.message_bus.messages.message import IMessageAvro

if TYPE_CHECKING:
    from fabric.actor.security.auth_token import AuthToken
    from fabric.actor.core.apis.i_client_reservation import IClientReservation
    from fabric.message_bus.messages.failed_rpc_avro import FailedRPCAvro
    from fabric.message_bus.messages.query_avro import QueryAvro
    from fabric.message_bus.messages.query_result_avro import QueryResultAvro
    from fabric.message_bus.messages.update_lease_avro import UpdateLeaseAvro
    from fabric.message_bus.messages.update_ticket_avro import UpdateTicketAvro
    from fabric.actor.core.apis.i_actor import IActor


class ActorService:
    def __init__(self, *, actor: IActor):
        self.actor = actor
        self.logger = self.actor.get_logger()

    def get_callback(self, *, kafka_topic: str, auth: AuthToken):
        return KafkaReturn(kafka_topic=kafka_topic, identity=auth, logger=self.actor.get_logger())

    def get_concrete(self, *, reservation:ReservationAvro) -> IConcreteSet:
        encoded = reservation.resource_set.concrete
        try:
            decoded = pickle.loads(encoded)
            return decoded
        except Exception as e:
            self.logger.error("Exception occurred while decoding {}".format(e))
        return None

    def pass_client(self, *, reservation:ReservationAvro) -> IClientReservation:
        slice_obj = Translate.translate_slice(slice_id=reservation.slice.guid, slice_name=reservation.slice.slice_name)
        term = Translate.translate_term_from_avro(term=reservation.term)

        resource_set = Translate.translate_resource_set_from_avro(rset=reservation.resource_set)
        resource_set.set_resources(cset=self.get_concrete(reservation=reservation))

        return ClientReservationFactory.create(rid=ID(id=reservation.reservation_id), resources=resource_set, term=term,
                                               slice_object=slice_obj, actor=self.actor)

    def do_dispatch(self, *, rpc: IncomingRPC):
        try:
            RPCManagerSingleton.get().dispatch_incoming(actor=self.actor, rpc=rpc)
        except Exception as e:
            msg = "An error occurred while dispatching request"
            self.logger.error("{} e={}".format(msg, e))
            raise e

    def query(self, *, request: QueryAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            query = request.properties
            callback = self.get_callback(kafka_topic=request.callback_topic, auth=authToken)
            rpc = IncomingQueryRPC(request_type=RPCRequestType.Query, message_id=ID(id=request.get_message_id()),
                                   query=query, caller=authToken, callback=callback)
        except Exception as e:
            self.logger.error("Invalid query request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def query_result(self, *, request: QueryResultAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            query = request.properties
            rpc = IncomingQueryRPC(request_type=RPCRequestType.QueryResult, message_id=ID(id=request.get_message_id()),
                                   query=query, caller=authToken,request_id=ID(id=request.request_id))
        except Exception as e:
            self.logger.error("Invalid queryResult request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def update_lease(self, *, request: UpdateLeaseAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            rsvn = self.pass_client(reservation=request.reservation)
            udd = Translate.translate_udd_from_avro(udd=request.update_data)
            rpc = IncomingReservationRPC(message_id=ID(id=request.message_id), request_type=RPCRequestType.UpdateLease,
                                         reservation=rsvn, update_data=udd, caller=authToken)
        except Exception as e:
            self.logger.error("Invalid updateLease request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def update_ticket(self, *, request: UpdateTicketAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            rsvn = self.pass_client(reservation=request.reservation)
            udd = Translate.translate_udd_from_avro(udd=request.update_data)
            rpc = IncomingReservationRPC(message_id=ID(id=request.message_id),
                                         request_type=RPCRequestType.UpdateTicket, reservation=rsvn, update_data=udd,
                                         caller=authToken)
        except Exception as e:
            self.logger.error("Invalid updateTicket request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def failed_rpc(self, *, request: FailedRPCAvro):
        rpc = None
        authToken = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            failed_request_type = RPCRequestType(request.request_type)
            if request.reservation_id is not None and request.reservation_id != "":
                rpc = IncomingFailedRPC(message_id=ID(id=request.message_id), failed_request_type=failed_request_type,
                                        request_id=request.request_id, failed_reservation_id=ID(id=request.reservation_id),
                                        error_details=request.error_details, caller=authToken)
            else:
                rpc = IncomingFailedRPC(message_id=ID(id=request.message_id), failed_request_type=failed_request_type,
                                        request_id=request.request_id, failed_reservation_id=None,
                                        error_details=request.error_details, caller=authToken)
        except Exception as e:
            self.logger.error("Invalid failedRequest request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def process(self, *, message: IMessageAvro):
        if message.get_message_name() == IMessageAvro.Query:
            self.query(request=message)
        elif message.get_message_name() == IMessageAvro.QueryResult:
            self.query_result(request=message)
        elif message.get_message_name() == IMessageAvro.UpdateLease:
            self.update_lease(request=message)
        elif message.get_message_name() == IMessageAvro.UpdateTicket:
            self.update_ticket(request=message)
        else:
            self.logger.error("Unsupported message {}".format(message))
            raise Exception("Unsupported message {}".format(message.get_message_name()))
