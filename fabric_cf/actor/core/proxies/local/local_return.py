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
from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin
from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
from fabric_cf.actor.core.apis.abc_controller_callback_proxy import ABCControllerCallbackProxy
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation, DelegationState
from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.apis.abc_server_reservation import ABCServerReservation
from fabric_cf.actor.core.delegation.delegation_factory import DelegationFactory
from fabric_cf.actor.core.kernel.reservation_client import ClientReservationFactory
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.proxies.local.local_proxy import LocalProxy
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.security.auth_token import AuthToken


class LocalReturn(LocalProxy, ABCControllerCallbackProxy):
    def __init__(self, *, actor: ABCActorMixin):
        super().__init__(actor=actor)
        self.callback = True

    def prepare_update_delegation(self, *, delegation: ABCDelegation, update_data: UpdateData,
                                  callback: ABCCallbackProxy, caller: AuthToken) -> ABCRPCRequestState:

        state = LocalProxy.LocalProxyRequestState()
        state.delegation = LocalReturn.pass_delegation(delegation=delegation)
        state.update_data = UpdateData()
        state.update_data.absorb(other=update_data)
        state.callback = callback
        return state

    def _prepare(self, *, reservation: ABCServerReservation, update_data: UpdateData,
                 callback: ABCCallbackProxy, caller: AuthToken) -> ABCRPCRequestState:

        state = LocalProxy.LocalProxyRequestState()
        state.reservation = LocalReturn.pass_reservation(reservation=reservation, plugin=self.get_actor().get_plugin())
        state.update_data = UpdateData()
        state.update_data.absorb(other=update_data)
        state.callback = callback
        return state

    def prepare_update_ticket(self, *, reservation: ABCBrokerReservation, update_data: UpdateData,
                              callback: ABCCallbackProxy, caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, update_data=update_data, callback=callback, caller=caller)

    def prepare_update_lease(self, *, reservation: ABCAuthorityReservation, update_data, callback: ABCCallbackProxy,
                             caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, update_data=update_data, callback=callback, caller=caller)

    @staticmethod
    def pass_reservation(*, reservation: ABCServerReservation, plugin: ABCBasePlugin) -> ABCReservationMixin:
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

        if isinstance(reservation, ABCBrokerReservation):
            client_reservation = ClientReservationFactory.create(rid=reservation.get_reservation_id(),
                                                                 resources=rset, term=term, slice_object=slice_obj)
            client_reservation.set_ticket_sequence_in(sequence=reservation.get_sequence_out())
            return client_reservation
        else:
            controller_reservation = ClientReservationFactory.create(rid=reservation.get_reservation_id(),
                                                                         resources=rset, term=term,
                                                                         slice_object=slice_obj)
            controller_reservation.set_lease_sequence_in(sequence=reservation.get_sequence_out())
            return controller_reservation

    @staticmethod
    def pass_delegation(*, delegation: ABCDelegation) -> ABCDelegation:
        slice_obj = delegation.get_slice_object().clone_request()

        delegation_new = DelegationFactory.create(did=delegation.get_delegation_id(),
                                                  slice_id=delegation.get_slice_id())
        delegation_new.set_slice_object(slice_object=slice_obj)
        # TODO
        if not delegation.is_reclaimed():
            delegation_new.set_graph(delegation.get_graph())
        return delegation_new
