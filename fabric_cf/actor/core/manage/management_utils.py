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

from fabric_mb.message_bus.messages.slice_avro import SliceAvro

from fabric_cf.actor.core.apis.abc_query_response_handler import ABCQueryResponseHandler
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.apis.abc_mgmt_container import ABCMgmtContainer
from fabric_cf.actor.core.manage.local.local_container import LocalContainer
from fabric_cf.actor.core.proxies.kafka.translate import Translate
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.rpc_exception import RPCException, RPCError
from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.reservation_mng import ReservationMng

    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_actor_proxy import ABCActorProxy
    from fabric_cf.actor.security.auth_token import AuthToken

from fabric_cf.actor.core.manage.converter import Converter


class MyQueryResponseHandler(ABCQueryResponseHandler):
    def __init__(self):
        self.result = None
        self.error = None
        self.condition = threading.Condition()

    def handle(self, *, status: RPCException, result: dict):
        with self.condition:
            self.error = status
            self.result = result
            self.condition.notify()


class ManagementUtils:
    @staticmethod
    def update_slice(*, slice_obj: ABCSlice, slice_mng: SliceAvro) -> ABCSlice:
        slice_obj.set_graph_id(graph_id=slice_mng.get_graph_id())
        slice_obj.set_lease_end(lease_end=slice_mng.get_lease_end())
        slice_obj.set_config_properties(value=slice_mng.get_config_properties())
        return slice_obj

    @staticmethod
    def update_reservation(*, res_obj: ABCReservationMixin, rsv_mng: ReservationMng) -> ABCReservationMixin:
        if isinstance(res_obj, ABCClientReservation):
            res_obj.set_renewable(renewable=rsv_mng.is_renewable())

        return Converter.absorb_res_properties(rsv_mng=rsv_mng, res_obj=res_obj)

    @staticmethod
    def query(*, actor: ABCActorMixin, actor_proxy: ABCActorProxy, query: dict):
        handler = MyQueryResponseHandler()

        actor.query(query=query, actor_proxy=actor_proxy, handler=handler)

        with handler.condition:
            try:
                handler.condition.wait()
                # TODO InterruptedException
            except Exception as e:
                raise RPCException(message=str(e), error=RPCError.LocalError)

        if handler.error is not None:
            raise handler.error

        return handler.result

    @staticmethod
    def get_local_actor():
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        local_actor = GlobalsSingleton.get().get_container().get_actor()
        local_mgmt_container = ManagementUtils.get_local_container(caller=local_actor.get_identity())
        return local_mgmt_container.get_actor(guid=local_actor.get_guid())

    @staticmethod
    def get_local_container(*, caller: AuthToken):
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        manager = GlobalsSingleton.get().get_container().get_management_object_manager().get_management_object(
            key=ID(uid=Constants.CONTAINER_MANAGMENT_OBJECT_ID))
        proxy = LocalContainer(manager=manager, auth=caller)
        return proxy

    @staticmethod
    def connect(*, caller: AuthToken) -> ABCMgmtContainer():
        return ManagementUtils.get_local_container(caller=caller)
