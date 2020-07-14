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
from fabric.actor.core.apis.IClientReservation import IClientReservation
from fabric.actor.core.apis.IClientReservationFactory import IClientReservationFactory
from fabric.actor.core.apis.ISlice import ISlice
from fabric.actor.core.kernel.ReservationClient import ReservationClient
from fabric.actor.core.kernel.ReservationFactory import ReservationFactory
from fabric.actor.core.kernel.ReservationStates import ReservationStates, ReservationPendingStates
from fabric.actor.core.kernel.ResourceSet import ResourceSet
from fabric.actor.core.time.Term import Term
from fabric.actor.core.util.ID import ID


class ClientReservationFactory(ReservationFactory, IClientReservationFactory):
    @staticmethod
    def create(rid: ID, resources: ResourceSet = None, term: Term = None, slice_object: ISlice = None,
               broker: IBrokerProxy = None, actor: IActor = None):
        result = ReservationClient(rid=rid, resources=resources, term=term, slice_object=slice_object, broker=broker)
        if actor is not None:
            result.restore(actor, slice_object, actor.get_logger())
        return result

    @staticmethod
    def set_as_source(reservation: IClientReservation):
        reservation.transition("[source]", ReservationStates.Ticketed, ReservationPendingStates.None_)