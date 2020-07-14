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

import threading
from typing import TYPE_CHECKING

from fabric.actor.core.apis.IQueryResponseHandler import IQueryResponseHandler
from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.apis.IMgmtContainer import IMgmtContainer
from fabric.actor.core.manage.local.LocalContainer import LocalContainer
from fabric.actor.core.proxies.kafka.Translate import Translate
from fabric.actor.core.util.ID import ID
from fabric.actor.core.util.RPCException import RPCException, RPCError
from fabric.message_bus.messages.SliceAvro import SliceAvro
from fabric.actor.core.apis.IClientReservation import IClientReservation

if TYPE_CHECKING:
    from fabric.actor.core.apis.ISlice import ISlice
    from fabric.actor.core.apis.IReservation import IReservation
    from fabric.message_bus.messages.ReservationMng import ReservationMng
    from fabric.actor.core.apis.IActor import IActor
    from fabric.actor.core.apis.IActorProxy import IActorProxy
    from fabric.actor.security.AuthToken import AuthToken

from fabric.actor.core.manage.Converter import Converter


class MyQueryResponseHandler(IQueryResponseHandler):
    def __init__(self):
        self.result = None
        self.error = None
        self.condition = threading.Condition()

    def handle(self, status, result):
        with self.condition:
            self.error = status
            self.result = result
            self.condition.notify()


class ManagementUtils:
    @staticmethod
    def update_slice(slice_obj: ISlice, slice_mng: SliceAvro) -> ISlice:
        return Translate.absorb_properties(slice_mng, slice_obj)

    @staticmethod
    def update_reservation(res_obj: IReservation, rsv_mng: ReservationMng) -> IReservation:
        if isinstance(res_obj, IClientReservation):
            res_obj.set_renewable(rsv_mng.is_renewable())

        return Converter.absorb_res_properties(rsv_mng, res_obj)

    @staticmethod
    def query(actor: IActor, actor_proxy: IActorProxy, query: dict):
        handler = MyQueryResponseHandler()

        actor.query(query=query, actor_proxy=actor_proxy, handler=handler)

        with handler.condition:
            try:
                handler.condition.wait()
                # TODO InterruptedException
            except Exception as e:
                raise RPCException(str(e), RPCError.LocalError)

        if handler.error is not None:
            raise handler.error

        return handler.result

    @staticmethod
    def get_local_container(caller: AuthToken = None):
        from fabric.actor.core.container.Globals import GlobalsSingleton
        manager = GlobalsSingleton.get().get_container().get_management_object_manager().get_management_object(
            ID(Constants.ContainerManagmentObjectID))
        proxy = LocalContainer(manager, caller)
        return proxy

    @staticmethod
    def connect(caller: AuthToken = None) -> IMgmtContainer():
        return ManagementUtils.get_local_container(caller)

