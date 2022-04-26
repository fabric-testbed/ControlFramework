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
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.update_delegation_avro import UpdateDelegationAvro

from fabric_cf.actor.core.apis.abc_concrete_set import ABCConcreteSet
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ProxyException
from fabric_cf.actor.core.delegation.broker_delegation_factory import BrokerDelegationFactory
from fabric_cf.actor.core.kernel.incoming_delegation_rpc import IncomingDelegationRPC
from fabric_cf.actor.core.kernel.incoming_failed_rpc import IncomingFailedRPC
from fabric_cf.actor.core.kernel.incoming_query_rpc import IncomingQueryRPC
from fabric_cf.actor.core.kernel.incoming_rpc import IncomingRPC
from fabric_cf.actor.core.kernel.incoming_reservation_rpc import IncomingReservationRPC
from fabric_cf.actor.core.kernel.reservation_client import ClientReservationFactory
from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.proxies.kafka.kafka_retun import KafkaReturn
from fabric_cf.actor.core.proxies.kafka.translate import Translate
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.util.id import ID


if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.failed_rpc_avro import FailedRpcAvro
    from fabric_mb.message_bus.messages.query_avro import QueryAvro
    from fabric_mb.message_bus.messages.query_result_avro import QueryResultAvro
    from fabric_mb.message_bus.messages.update_lease_avro import UpdateLeaseAvro
    from fabric_mb.message_bus.messages.update_ticket_avro import UpdateTicketAvro

    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin


class ActorService:
    def __init__(self, *, actor: ABCActorMixin):
        self.actor = actor
        self.logger = self.actor.get_logger()

    def get_callback(self, *, kafka_topic: str, auth: AuthToken):
        return KafkaReturn(kafka_topic=kafka_topic, identity=auth, logger=self.actor.get_logger())

    def get_concrete(self, *, reservation: ReservationAvro) -> ABCConcreteSet:
        ticket = reservation.resource_set.ticket
        if reservation.resource_set.ticket is not None:
            return Translate.translate_ticket_from_avro(avro_ticket=ticket)

        unit_set = reservation.resource_set.unit_set
        if unit_set is not None:
            return Translate.translate_unit_set_from_avro(unit_list=unit_set)

        return None

    def pass_client(self, *, reservation: ReservationAvro) -> ABCClientReservation:
        slice_obj = Translate.translate_slice(slice_avro=reservation.slice)
        term = Translate.translate_term_from_avro(term=reservation.term)

        resource_set = Translate.translate_resource_set_from_avro(rset=reservation.resource_set)
        resource_set.set_resources(cset=self.get_concrete(reservation=reservation))

        return ClientReservationFactory.create(rid=ID(uid=reservation.reservation_id), resources=resource_set,
                                               term=term, slice_object=slice_obj, actor=self.actor)

    def pass_client_delegation(self, *, delegation: DelegationAvro, caller: AuthToken) -> ABCDelegation:
        slice_obj = Translate.translate_slice(slice_avro=delegation.slice)

        dlg = BrokerDelegationFactory.create(did=delegation.get_delegation_id(),
                                             slice_id=slice_obj.get_slice_id(),
                                             broker=None)
        dlg.restore(actor=self.actor, slice_obj=slice_obj)

        site_proxy = ActorRegistrySingleton.get().get_proxy(protocol=Constants.PROTOCOL_KAFKA,
                                                            actor_name=caller.get_name())
        dlg.set_site_proxy(site_proxy=site_proxy)

        if delegation.graph is not None:
            dlg.load_graph(graph_str=delegation.graph)
        return dlg

    def do_dispatch(self, *, rpc: IncomingRPC):
        try:
            RPCManagerSingleton.get().dispatch_incoming(actor=self.actor, rpc=rpc)
        except Exception as e:
            msg = "An error occurred while dispatching request"
            self.logger.error("{} e={}".format(msg, e))
            raise e

    def query(self, *, request: QueryAvro):
        rpc = None
        auth_token = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            query = request.properties
            callback = self.get_callback(kafka_topic=request.callback_topic, auth=auth_token)
            rpc = IncomingQueryRPC(request_type=RPCRequestType.Query, message_id=ID(uid=request.get_message_id()),
                                   query=query, caller=auth_token, callback=callback)
        except Exception as e:
            self.logger.error("Invalid query request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def query_result(self, *, request: QueryResultAvro):
        rpc = None
        auth_token = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            query = request.properties
            rpc = IncomingQueryRPC(request_type=RPCRequestType.QueryResult, message_id=ID(uid=request.get_message_id()),
                                   query=query, caller=auth_token, request_id=ID(uid=request.request_id))
        except Exception as e:
            self.logger.error("Invalid query_result request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def update_lease(self, *, request: UpdateLeaseAvro):
        rpc = None
        auth_token = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            rsvn = self.pass_client(reservation=request.reservation)
            udd = Translate.translate_udd_from_avro(udd=request.update_data)
            rpc = IncomingReservationRPC(message_id=ID(uid=request.message_id), request_type=RPCRequestType.UpdateLease,
                                         reservation=rsvn, update_data=udd, caller=auth_token)
        except Exception as e:
            self.logger.error("Invalid update_lease request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def update_ticket(self, *, request: UpdateTicketAvro):
        rpc = None
        auth_token = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            rsvn = self.pass_client(reservation=request.reservation)
            udd = Translate.translate_udd_from_avro(udd=request.update_data)
            rpc = IncomingReservationRPC(message_id=ID(uid=request.message_id),
                                         request_type=RPCRequestType.UpdateTicket, reservation=rsvn, update_data=udd,
                                         caller=auth_token)
        except Exception as e:
            self.logger.error("Invalid update_ticket request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def update_delegation(self, *, request: UpdateDelegationAvro):
        rpc = None
        auth_token = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            dlg = self.pass_client_delegation(delegation=request.delegation, caller=auth_token)
            udd = Translate.translate_udd_from_avro(udd=request.update_data)
            rpc = IncomingDelegationRPC(message_id=ID(uid=request.message_id),
                                        request_type=RPCRequestType.UpdateDelegation, delegation=dlg, update_data=udd,
                                        caller=auth_token)
        except Exception as e:
            self.logger.error("Invalid update_delegation request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def failed_rpc(self, *, request: FailedRpcAvro):
        rpc = None
        auth_token = Translate.translate_auth_from_avro(auth_avro=request.auth)
        try:
            failed_request_type = RPCRequestType(request.request_type)
            if request.reservation_id is not None and request.reservation_id != "":
                rpc = IncomingFailedRPC(message_id=ID(uid=request.message_id), failed_request_type=failed_request_type,
                                        request_id=request.request_id,
                                        failed_reservation_id=ID(uid=request.reservation_id),
                                        error_details=request.error_details, caller=auth_token)
            else:
                rpc = IncomingFailedRPC(message_id=ID(uid=request.message_id), failed_request_type=failed_request_type,
                                        request_id=request.request_id, failed_reservation_id=None,
                                        error_details=request.error_details, caller=auth_token)
        except Exception as e:
            self.logger.error("Invalid failedRequest request: {}".format(e))
            raise e
        self.do_dispatch(rpc=rpc)

    def process(self, *, message: AbcMessageAvro):
        if message.get_message_name() == AbcMessageAvro.query:
            self.query(request=message)
        elif message.get_message_name() == AbcMessageAvro.query_result:
            self.query_result(request=message)
        elif message.get_message_name() == AbcMessageAvro.update_lease:
            self.update_lease(request=message)
        elif message.get_message_name() == AbcMessageAvro.update_ticket:
            self.update_ticket(request=message)
        elif message.get_message_name() == AbcMessageAvro.update_delegation:
            self.update_delegation(request=message)
        else:
            self.logger.error("Unsupported message {}".format(message))
            raise ProxyException("Unsupported message {}".format(message.get_message_name()))
