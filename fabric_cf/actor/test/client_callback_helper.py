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

from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
from fabric_cf.actor.core.apis.abc_client_callback_proxy import ABCClientCallbackProxy
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.rpc_request_state import RPCRequestState
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.proxies.local.local_return import LocalReturn
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.security.auth_token import AuthToken


class ClientCallbackHelper(ABCClientCallbackProxy):
    class MyRequestState(RPCRequestState):
        def __init__(self):
            super().__init__()
            self.reservation = None
            self.update_data = None

    def __init__(self, *, name: str, guid: ID):
        self.token = AuthToken(name=name, guid=guid)
        self.called = 0
        self.prepared = 0
        self.reservation = None

    def get_called(self) -> int:
        return self.called

    def get_guid(self) -> ID:
        return self.token.get_guid()

    def get_identity(self) -> AuthToken:
        return self.token

    def get_name(self) -> str:
        return self.token.get_name()

    def get_reservation(self) -> ABCReservationMixin:
        return self.reservation

    def get_type(self):
        return Constants.PROTOCOL_LOCAL

    def prepare_update_delegation(self, *, delegation: ABCDelegation, update_data: UpdateData,
                                  callback: ABCCallbackProxy, caller: AuthToken) -> ABCRPCRequestState:
        raise NotImplementedError

    def set_logger(self, *, logger):
        pass

    def prepare_update_ticket(self, *, reservation: ABCBrokerReservation, update_data: UpdateData,
                              callback: ABCCallbackProxy, caller: AuthToken) -> ABCRPCRequestState:
        state = self.MyRequestState()
        state.reservation = LocalReturn.pass_reservation(reservation=reservation,
                                                         plugin=ActorRegistrySingleton.get().get_actor(
                                                             actor_name_or_guid=self.token.get_name()).get_plugin())

        self.prepared += 1
        return state

    def execute(self, *, request: ABCRPCRequestState, producer: AvroProducerApi):
        if request.get_type() == RPCRequestType.UpdateTicket:
            self.called += 1
            self.reservation = request.reservation

    def get_logger(self):
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        return GlobalsSingleton.get().get_logger()

    def prepare_query_result(self, *, request_id: str, response, caller: AuthToken) -> ABCRPCRequestState:
        raise NotImplementedError

    def prepare_failed_request(self, *, request_id: str, failed_request_type,
                               failed_reservation_id: ID, error: str, caller: AuthToken) -> ABCRPCRequestState:
        raise NotImplementedError