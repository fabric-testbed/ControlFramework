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
from fabric_cf.actor.core.apis.i_actor import IActor
from fabric_cf.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric_cf.actor.core.apis.i_base_plugin import IBasePlugin
from fabric_cf.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric_cf.actor.core.apis.i_callback_proxy import ICallbackProxy
from fabric_cf.actor.core.apis.i_controller_callback_proxy import IControllerCallbackProxy
from fabric_cf.actor.core.apis.i_delegation import IDelegation
from fabric_cf.actor.core.apis.i_rpc_request_state import IRPCRequestState
from fabric_cf.actor.core.apis.i_reservation import IReservation
from fabric_cf.actor.core.apis.i_server_reservation import IServerReservation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ProxyException
from fabric_cf.actor.core.delegation.delegation_factory import DelegationFactory
from fabric_cf.actor.core.kernel.client_reservation_factory import ClientReservationFactory
from fabric_cf.actor.core.kernel.controller_reservation_factory import ControllerReservationFactory
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.proxies.local.local_proxy import LocalProxy
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.security.auth_token import AuthToken


class LocalReturn(LocalProxy, IControllerCallbackProxy):
    def __init__(self, *, actor: IActor):
        super().__init__(actor=actor)
        self.callback = True

    def prepare_update_delegation(self, *, delegation: IDelegation, update_data: UpdateData,
                                  callback: ICallbackProxy, caller: AuthToken) -> IRPCRequestState:

        state = LocalProxy.LocalProxyRequestState()
        state.delegation = LocalReturn.pass_delegation(delegation=delegation)
        state.update_data = UpdateData()
        state.update_data.absorb(other=update_data)
        state.callback = callback
        return state

    def _prepare(self, *, reservation: IServerReservation, update_data: UpdateData,
                 callback: ICallbackProxy, caller: AuthToken) -> IRPCRequestState:

        state = LocalProxy.LocalProxyRequestState()
        state.reservation = LocalReturn.pass_reservation(reservation=reservation, plugin=self.get_actor().get_plugin())
        state.update_data = UpdateData()
        state.update_data.absorb(other=update_data)
        state.callback = callback
        return state

    def prepare_update_ticket(self, *, reservation: IBrokerReservation, update_data: UpdateData,
                              callback: ICallbackProxy, caller: AuthToken) -> IRPCRequestState:
        return self._prepare(reservation=reservation, update_data=update_data, callback=callback, caller=caller)

    def prepare_update_lease(self, *, reservation: IAuthorityReservation, update_data, callback: ICallbackProxy,
                             caller: AuthToken) -> IRPCRequestState:
        return self._prepare(reservation=reservation, update_data=update_data, callback=callback, caller=caller)

    @staticmethod
    def pass_reservation(*, reservation: IServerReservation, plugin: IBasePlugin) -> IReservation:
        slice_obj = reservation.get_slice().clone_request()
        term = None

        if reservation.get_term() is None:
            term = reservation.get_requested_term().clone()
        else:
            term = reservation.get_term().clone()

        rset = None

        if reservation.get_resources() is None:
            rset = ResourceSet(units=0, rtype=reservation.get_requested_type())
        else:
            rset = LocalReturn.abstract_clone_return(rset=reservation.get_resources())

            concrete = reservation.get_resources().get_resources()

            if concrete is not None:
                cset = concrete.clone()
                rset.set_resources(cset=cset)

        if isinstance(reservation, IBrokerReservation):
            client_reservation = ClientReservationFactory.create(rid=reservation.get_reservation_id(),
                                                                 resources=rset, term=term, slice_object=slice_obj)
            client_reservation.set_ticket_sequence_in(sequence=reservation.get_sequence_out())
            return client_reservation
        else:
            controller_reservation = ControllerReservationFactory.create(rid=reservation.get_reservation_id(),
                                                                         resources=rset, term=term,
                                                                         slice_object=slice_obj)
            controller_reservation.set_lease_sequence_in(sequence=reservation.get_sequence_out())
            return controller_reservation

    @staticmethod
    def pass_delegation(*, delegation: IDelegation) -> IDelegation:
        slice_obj = delegation.get_slice_object().clone_request()

        delegation_new = DelegationFactory.create(did=delegation.get_delegation_id(),
                                                  slice_id=delegation.get_slice_id())
        delegation_new.set_slice_object(slice_object=slice_obj)
        # TODO
        delegation_new.set_graph(delegation.get_graph())
        return delegation_new
