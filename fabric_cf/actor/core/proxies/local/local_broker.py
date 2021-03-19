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
from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
from fabric_cf.actor.core.apis.abc_client_callback_proxy import ABCClientCallbackProxy
from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.delegation.broker_delegation_factory import BrokerDelegationFactory
from fabric_cf.actor.core.kernel.broker_reservation import BrokerReservationFactory
from fabric_cf.actor.core.proxies.local.local_proxy import LocalProxy
from fabric_cf.actor.security.auth_token import AuthToken


class LocalBroker(LocalProxy, ABCBrokerProxy):
    """
    Local proxy for Broker. Allows communication with a Broker in the same container as the caller.
    """
    def __init__(self, *, actor: ABCActorMixin):
        super().__init__(actor=actor)

    def _prepare_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy,
                            caller: AuthToken, id_token: str = None) -> ABCRPCRequestState:
        state = LocalProxy.LocalProxyRequestState()
        state.delegation = self.pass_delegation(delegation=delegation, auth=caller)
        state.callback = callback
        return state

    def prepare_claim_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy,
                                 caller: AuthToken, id_token: str = None) -> ABCRPCRequestState:
        return self._prepare_delegation(delegation=delegation, callback=callback, caller=caller, id_token=id_token)

    def prepare_reclaim_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy,
                                   caller: AuthToken, id_token: str = None) -> ABCRPCRequestState:
        return self._prepare_delegation(delegation=delegation, callback=callback, caller=caller, id_token=id_token)

    def _prepare(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy,
                 caller: AuthToken) -> ABCRPCRequestState:
        state = LocalProxy.LocalProxyRequestState()
        state.reservation = self.pass_reservation(reservation=reservation, auth=caller)
        state.callback = callback
        return state

    def prepare_ticket(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy,
                       caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, callback=callback, caller=caller)

    def prepare_extend_ticket(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy,
                              caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, callback=callback, caller=caller)

    def prepare_relinquish(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy,
                           caller: AuthToken) -> ABCRPCRequestState:
        return self._prepare(reservation=reservation, callback=callback, caller=caller)

    def pass_reservation(self, *, reservation: ABCClientReservation, auth: AuthToken) -> ABCBrokerReservation:
        slice_obj = reservation.get_slice().clone_request()

        rset = self.abstract_clone_broker(rset=reservation.get_requested_resources())

        term = reservation.get_requested_term().clone()

        broker_reservation = BrokerReservationFactory.create(rid=reservation.get_reservation_id(), resources=rset,
                                                             term=term, slice_obj=slice_obj)
        broker_reservation.set_sequence_in(sequence=reservation.get_ticket_sequence_out())
        broker_reservation.set_owner(owner=self.get_identity())

        return broker_reservation

    def pass_delegation(self, *, delegation: ABCDelegation, auth: AuthToken) -> ABCDelegation:
        slice_obj = delegation.get_slice_object().clone_request()

        broker_delegation = BrokerDelegationFactory.create(did=str(delegation.get_delegation_id()),
                                                           slice_id=slice_obj.get_slice_id(),
                                                           broker=self)
        broker_delegation.set_sequence_in(sequence=delegation.get_sequence_out())
        broker_delegation.set_owner(owner=self.get_identity())

        return broker_delegation
