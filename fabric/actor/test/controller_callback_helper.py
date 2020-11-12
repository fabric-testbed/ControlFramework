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
from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric.actor.core.apis.i_callback_proxy import ICallbackProxy
from fabric.actor.core.apis.i_controller_callback_proxy import IControllerCallbackProxy
from fabric.actor.core.apis.i_rpc_request_state import IRPCRequestState
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric.actor.core.proxies.local.local_return import LocalReturn
from fabric.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.update_data import UpdateData
from fabric.actor.security.auth_token import AuthToken
from fabric.actor.test.client_callback_helper import ClientCallbackHelper


class ControllerCallbackHelper(ClientCallbackHelper, IControllerCallbackProxy):
    class IUpdateLeaseHandler:
        def handle_update_lease(self, *, reservation: IReservation, update_data: UpdateData, caller: AuthToken):
            pass

        def check_termination(self):
            pass

    def __init__(self, *, name: str, guid: ID):
        super().__init__(name=name, guid=guid)
        self.called_for_lease = 0
        self.lease = None
        self.updateLeaseHandler = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['updateLeaseHandler']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.updateLeaseHandler = None

    def prepare_update_lease(self, *, reservation: IAuthorityReservation, update_data: UpdateData,
                             callback: ICallbackProxy, caller: AuthToken) -> IRPCRequestState:
        state = ClientCallbackHelper.MyRequestState()
        state.reservation = LocalReturn.pass_reservation(reservation=reservation,
                                                         plugin=ActorRegistrySingleton.get().get_actor(
                                                             actor_name_or_guid=self.token.get_name()).get_plugin())
        state.update_data = UpdateData()
        state.update_data.absorb(other=update_data)
        return state

    def execute(self, *, request: IRPCRequestState):
        if request.get_type() == RPCRequestType.UpdateLease:
            self.lease = request.reservation
            self.called_for_lease += 1
            if self.updateLeaseHandler is not None:
                self.updateLeaseHandler.handle_update_lease(reservation=self.lease, update_data=request.update_data,
                                                            caller=request.get_caller())
            else:
                super().execute(request=request)

    def get_lease(self) -> IReservation:
        return self.lease

    def get_called_for_lease(self):
        return self.called_for_lease

    def set_update_lease_handler(self, *, handler: IUpdateLeaseHandler):
        self.updateLeaseHandler = handler

    def get_called(self) -> int:
        return self.called

    def get_guid(self) -> ID:
        return self.token.get_guid()

    def get_identity(self) -> AuthToken:
        return self.token

    def get_name(self) -> str:
        return self.token.get_name()

    def get_reservation(self) -> IReservation:
        return self.reservation

    def get_type(self):
        return Constants.ProtocolLocal

    def prepare_query_result(self, *, request_id: str, response, caller: AuthToken) -> IRPCRequestState:
        raise Exception("Not implemented")

    def prepare_failed_request(self, *, request_id: str, failed_request_type,
                               failed_reservation_id: ID, error: str, caller: AuthToken) -> IRPCRequestState:
        raise Exception("Not implemented")