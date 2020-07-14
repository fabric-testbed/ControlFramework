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
from fabric.actor.core.apis.IActor import IActor
from fabric.actor.core.apis.IBrokerProxy import IBrokerProxy
from fabric.actor.core.apis.IBrokerReservation import IBrokerReservation
from fabric.actor.core.apis.IClientCallbackProxy import IClientCallbackProxy
from fabric.actor.core.apis.IClientReservation import IClientReservation
from fabric.actor.core.apis.IRPCRequestState import IRPCRequestState
from fabric.actor.core.apis.IReservation import IReservation
from fabric.actor.core.kernel.BrokerReservationFactory import BrokerReservationFactory
from fabric.actor.core.proxies.local.LocalProxy import LocalProxy
from fabric.actor.core.util.ResourceData import ResourceData
from fabric.actor.security.AuthToken import AuthToken


class LocalBroker(LocalProxy, IBrokerProxy):
    """
    Local proxy for Broker. Allows communication with a Broker in the same container as the caller.
    """
    def __init__(self, actor: IActor):
        super().__init__(actor)

    def prepare_ticket(self, reservation: IReservation, callback: IClientCallbackProxy,
                       caller: AuthToken) -> IRPCRequestState:
        state = LocalProxy.LocalProxyRequestState()
        state.reservation = self.pass_reservation(reservation, caller)
        state.callback = callback
        return state

    def prepare_claim(self, reservation: IReservation, callback: IClientCallbackProxy,
                      caller: AuthToken) -> IRPCRequestState:
        state = LocalProxy.LocalProxyRequestState()
        state.reservation = self.pass_reservation(reservation, caller)
        state.callback = callback
        return state

    def prepare_extend_ticket(self, reservation: IReservation, callback: IClientCallbackProxy,
                              caller: AuthToken) -> IRPCRequestState:
        state = LocalProxy.LocalProxyRequestState()
        state.reservation = self.pass_reservation(reservation, caller)
        state.callback = callback
        return state

    def prepare_relinquish(self, reservation: IReservation, callback: IClientCallbackProxy,
                           caller: AuthToken) -> IRPCRequestState:
        state = LocalProxy.LocalProxyRequestState()
        state.reservation = self.pass_reservation(reservation, caller)
        state.callback = callback
        return state

    def pass_reservation(self, reservation: IClientReservation, auth: AuthToken) -> IBrokerReservation:
        slice_obj = reservation.get_slice().clone_request()

        rset = self.abstract_clone_broker(reservation.get_requested_resources())
        rset.get_resource_data().request_properties = ResourceData.merge_properties(
            reservation.get_slice().get_request_properties(), rset.get_resource_data().get_request_properties())

        term = reservation.get_requested_term().clone()

        broker_reservation = BrokerReservationFactory.create(reservation.get_reservation_id(), rset, term, slice_obj)
        broker_reservation.set_sequence_in(reservation.get_ticket_sequence_out())
        broker_reservation.set_owner(self.get_identity())

        return broker_reservation