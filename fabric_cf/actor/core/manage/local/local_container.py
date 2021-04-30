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

import traceback
from typing import List

from fabric_mb.message_bus.messages.actor_avro import ActorAvro
from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.container_management_object import ContainerManagementObject
from fabric_cf.actor.core.manage.management_object import ManagementObject
from fabric_cf.actor.core.apis.abc_mgmt_actor import ABCMgmtActor
from fabric_cf.actor.core.apis.abc_mgmt_authority import ABCMgmtAuthority
from fabric_cf.actor.core.apis.abc_mgmt_broker_mixin import ABCMgmtBrokerMixin
from fabric_cf.actor.core.apis.abc_mgmt_container import ABCMgmtContainer
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.manage.local.local_proxy import LocalProxy
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reflection_utils import ReflectionUtils
from fabric_cf.actor.security.auth_token import AuthToken


class LocalContainer(LocalProxy, ABCMgmtContainer):
    def __init__(self, *, manager: ManagementObject, auth: AuthToken):
        super().__init__(manager=manager, auth=auth)
        if not isinstance(manager, ContainerManagementObject):
            raise ManageException("Invalid manager object. Required: {}".format(type(ContainerManagementObject)))

    def get_management_object(self, *, key: ID):
        try:
            obj = self.manager.get_management_object(key=key)

            if obj is None:
                return None

            desc_list = obj.get_proxies()

            if desc_list is None:
                raise ManageException("Management object did not specify any proxies")
            desc = None
            for d in desc_list:
                if d.get_protocol() == Constants.PROTOCOL_LOCAL:
                    desc = d
                    break

            if desc is None or desc.get_proxy_class() is None or desc.get_proxy_module() is None:
                raise ManageException("Manager object did not specify local proxy")

            try:
                mgmt_obj = ReflectionUtils.create_instance_with_params(module_name=desc.get_proxy_module(),
                                                                       class_name=desc.get_proxy_class())(manager=obj,
                                                                                                          auth=self.auth)
                return mgmt_obj
            except Exception as e:
                traceback.print_exc()
                raise ManageException("Could not instantiate proxy {}".format(e))
        except Exception as e:
            traceback.print_exc()
            raise e

    def get_actor(self, *, guid: ID):
        try:
            component = self.get_management_object(key=guid)

            if component is not None:
                if isinstance(component, ABCMgmtActor):
                    return component
                else:
                    self.last_exception = Exception(Constants.INVALID_MANAGEMENT_OBJECT_TYPE.format(type(component)))

            return None
        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())
            traceback.print_exc()

    def get_actors(self) -> List[ActorAvro]:
        self.clear_last()
        try:
            result = self.manager.get_actors(caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.actors

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())
            traceback.print_exc()

        return None

    def get_actors_from_database(self) -> List[ActorAvro]:
        self.clear_last()
        try:
            result = self.manager.get_actors_from_database(caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.actors

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())
            traceback.print_exc()

        return None

    def get_actors_from_database_name_type_status(self, *, name: str, actor_type: int, status: int) -> List[ActorAvro]:
        self.clear_last()
        try:
            result = self.manager.get_actors_from_database_name_type_status(name=name, actor_type=actor_type,
                                                                            status=status, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.actors

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())
            traceback.print_exc()

        return None

    def get_authorities(self) -> List[ActorAvro]:
        self.clear_last()
        try:
            result = self.manager.get_authorities(caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.actors

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())
            traceback.print_exc()

        return None

    def get_brokers(self) -> List[ActorAvro]:
        self.clear_last()
        try:
            result = self.manager.get_brokers(caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.actors

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())
            traceback.print_exc()

        return None

    def get_controllers(self) -> List[ActorAvro]:
        self.clear_last()
        try:
            result = self.manager.get_controllers(caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.actors

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())
            traceback.print_exc()

        return None

    def get_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        self.clear_last()
        try:
            result = self.manager.get_proxies_by_protocol(protocol=protocol, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.proxies

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())
            traceback.print_exc()

        return None

    def get_broker_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        self.clear_last()
        try:
            result = self.manager.get_broker_proxies(protocol=protocol, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.proxies

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())
            traceback.print_exc()

        return None

    def get_authority_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        self.clear_last()
        try:
            result = self.manager.get_site_proxies(protocol=protocol, caller=self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.proxies

        except Exception as e:
            self.on_exception(e=e, traceback_str=traceback.format_exc())
            traceback.print_exc()

        return None

    def get_controller(self, *, guid: ID) -> ABCMgmtControllerMixin:
        component = self.get_management_object(key=guid)

        if component is None:
            return None

        self.clear_last()

        if not isinstance(component, ABCMgmtControllerMixin):
            self.last_exception = Exception("Invalid type")
        else:
            return component

        return None

    def get_broker(self, *, guid: ID) -> ABCMgmtBrokerMixin:
        component = self.get_management_object(key=guid)

        if component is not None:
            if isinstance(component, ABCMgmtBrokerMixin):
                return component
            else:
                self.last_exception = Exception(Constants.INVALID_MANAGEMENT_OBJECT_TYPE.format(type(component)))

        return None

    def get_authority(self, *, guid: ID) -> ABCMgmtAuthority:
        component = self.get_management_object(key=guid)

        if component is not None:
            if isinstance(component, ABCMgmtAuthority):
                return component
            else:
                self.last_exception = Exception(Constants.INVALID_MANAGEMENT_OBJECT_TYPE.format(type(component)))

        return None