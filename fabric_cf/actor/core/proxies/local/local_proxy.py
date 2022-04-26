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
from fabric_mb.message_bus.producer import AvroProducerApi

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ProxyException
from fabric_cf.actor.core.core.rpc_request_state import RPCRequestState
from fabric_cf.actor.core.kernel.incoming_failed_rpc import IncomingFailedRPC
from fabric_cf.actor.core.kernel.incoming_query_rpc import IncomingQueryRPC
from fabric_cf.actor.core.kernel.incoming_reservation_rpc import IncomingReservationRPC
from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.proxies.proxy import Proxy
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.util.rpc_exception import RPCError
from fabric_cf.actor.security.auth_token import AuthToken


class LocalProxy(Proxy, ABCCallbackProxy):
    class LocalProxyRequestState(RPCRequestState):
        def __init__(self):
            super().__init__()
            self.reservation = None
            self.update_data = None
            self.callback = None
            self.query = None
            self.request_id = None
            self.failed_reservation_id = None
            self.failed_request_type = None
            self.error_detail = None
            self.delegation = None

    def __init__(self, *, actor: ABCActorMixin):
        super().__init__(auth=actor.get_identity())
        self.logger = actor.get_logger()
        self.proxy_type = Constants.PROTOCOL_LOCAL

    def execute(self, *, request: ABCRPCRequestState, producer: AvroProducerApi):
        try:
            incoming = None
            if request.get_type() == RPCRequestType.Query:
                incoming = IncomingQueryRPC(request_type=RPCRequestType.Query, message_id=request.get_message_id(),
                                            query=request.query, caller=request.get_caller(), callback=request.callback)

            elif request.get_type() == RPCRequestType.QueryResult:
                incoming = IncomingQueryRPC(request_type=RPCRequestType.QueryResult,
                                            message_id=request.get_message_id(), query=request.query,
                                            caller=request.get_caller(), request_id=request.request_id)

            elif request.get_type() == RPCRequestType.Ticket or request.get_type() == RPCRequestType.Redeem or \
                    request.get_type() == RPCRequestType.ExtendTicket or \
                    request.get_type() == RPCRequestType.ExtendLease or \
                    request.get_type() == RPCRequestType.Close or request.get_type() == RPCRequestType.Relinquish:
                incoming = IncomingReservationRPC(message_id=request.get_message_id(), request_type=request.get_type(),
                                                  reservation=request.reservation,
                                                  callback=request.callback, caller=request.get_caller())

            elif request.get_type() == RPCRequestType.UpdateTicket or request.get_type() == RPCRequestType.UpdateLease:
                incoming = IncomingReservationRPC(message_id=request.get_message_id(), request_type=request.get_type(),
                                                  reservation=request.reservation,
                                                  update_data=request.update_data, caller=request.get_caller())

            elif request.get_type() == RPCRequestType.FailedRPC:
                incoming = IncomingFailedRPC(message_id=request.get_message_id(),
                                             failed_request_type=request.failed_request_type,
                                             request_id=request.request_id,
                                             failed_reservation_id=request.failed_reservation_id,
                                             error_details=request.error_detail, caller=request.get_caller())
            else:
                raise ProxyException("Unsupported RPC type: {}".format(request.get_type()))
            RPCManagerSingleton.get().dispatch_incoming(actor=self.get_actor(), rpc=incoming)

        except Exception as e:
            raise ProxyException("Error while processing RPC request{} {}".format(RPCError.InvalidRequest, e))

    def prepare_query(self, *, callback: ABCCallbackProxy, query: dict, caller: AuthToken):
        state = self.LocalProxyRequestState()
        state.query = query
        state.callback = callback
        return state

    def prepare_query_result(self, *, request_id: str, response, caller: AuthToken) -> ABCRPCRequestState:
        state = self.LocalProxyRequestState()
        state.query = response
        state.request_id = request_id
        return state

    def prepare_failed_request(self, *, request_id: str, failed_request_type,
                               failed_reservation_id, error: str, caller: AuthToken) -> ABCRPCRequestState:
        state = self.LocalProxyRequestState()
        state.request_id = request_id
        state.failed_request_type = failed_request_type
        state.failed_reservation_id = failed_reservation_id
        state.error_detail = error
        return state

    def get_actor(self) -> ABCActorMixin:
        result = ActorRegistrySingleton.get().get_actor(self.get_name())
        if result is None:
            raise ProxyException("Actor does not exist.")
        return result
