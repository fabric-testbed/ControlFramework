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
from fabric.actor.core.apis.IAuthorityReservation import IAuthorityReservation
from fabric.actor.core.apis.IBasePlugin import IBasePlugin
from fabric.actor.core.apis.IBrokerReservation import IBrokerReservation
from fabric.actor.core.apis.ICallbackProxy import ICallbackProxy
from fabric.actor.core.apis.IControllerCallbackProxy import IControllerCallbackProxy
from fabric.actor.core.apis.IRPCRequestState import IRPCRequestState
from fabric.actor.core.apis.IReservation import IReservation
from fabric.actor.core.apis.IServerReservation import IServerReservation
from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.kernel.ClientReservationFactory import ClientReservationFactory
from fabric.actor.core.kernel.ControllerReservationFactory import ControllerReservationFactory
from fabric.actor.core.kernel.ResourceSet import ResourceSet
from fabric.actor.core.proxies.Proxy import Proxy
from fabric.actor.core.proxies.local.LocalProxy import LocalProxy
from fabric.actor.core.util.ResourceData import ResourceData
from fabric.actor.core.util.UpdateData import UpdateData
from fabric.actor.security.AuthToken import AuthToken


class LocalReturn(LocalProxy, IControllerCallbackProxy):
    def __init__(self, actor: IActor):
        super().__init__(actor)
        self.callback = True

    def prepare_update_ticket(self, reservation: IBrokerReservation, update_data: UpdateData,
                              callback: ICallbackProxy, caller: AuthToken) -> IRPCRequestState:

        state = LocalProxy.LocalProxyRequestState()
        state.reservation = LocalReturn.pass_reservation(reservation, self.get_actor().get_plugin())
        state.update_data = UpdateData()
        state.update_data.absorb(update_data)
        state.callback = callback
        return state

    def prepare_update_lease(self, reservation: IAuthorityReservation,  update_data, callback: ICallbackProxy,
                             caller: AuthToken) -> IRPCRequestState:
        state = LocalProxy.LocalProxyRequestState()
        state.reservation = LocalReturn.pass_reservation(reservation, self.get_actor().get_plugin())
        state.update_data = UpdateData()
        state.update_data.absorb(update_data)
        state.callback = callback
        return state

    @staticmethod
    def pass_reservation(reservation: IServerReservation, plugin: IBasePlugin) -> IReservation:
        slice_obj = reservation.get_slice().clone_request()
        term = None

        if reservation.get_term() is None:
            term = reservation.get_requested_term().clone()
        else:
            term = reservation.get_term().clone()

        rset = None

        if reservation.get_resources() is None:
            rset = ResourceSet(units=0, rtype=reservation.get_requested_type(), rdata=ResourceData())
        else:
            rset = LocalReturn.abstract_clone_return(reservation.get_resources())

            concrete = reservation.get_resources().get_resources()

            if concrete is not None:
                cset = None
                try:
                    encoded = concrete.encode(Constants.ProtocolLocal)
                    cset = Proxy.decode(encoded, plugin)
                except Exception as e:
                    raise Exception("Error while encoding concrete set {}".format(e))

                if cset is None:
                    raise Exception("Unsupported ConcreteSet type: {}".format(type(concrete)))

                rset.set_resources(cset)

        if isinstance(reservation, IBrokerReservation):
            client_reservation = ClientReservationFactory.create(rid=reservation.get_reservation_id(),
                                                                 resources=rset, term=term, slice_object=slice_obj)
            client_reservation.set_ticket_sequence_in(reservation.get_sequence_out())
            return client_reservation
        else:
            controller_reservation = ControllerReservationFactory.create(rid=reservation.get_reservation_id(),
                                                                         resources=rset, term=term,
                                                                         slice_object=slice_obj)
            controller_reservation.set_lease_sequence_in(reservation.get_sequence_out())
            return controller_reservation
