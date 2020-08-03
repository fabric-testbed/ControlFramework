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

from fabric.actor.core.common.constants import Constants
from fabric.actor.core.manage.container_management_object import ContainerManagementObject
from fabric.actor.core.manage.management_object import ManagementObject
from fabric.actor.core.apis.i_mgmt_actor import IMgmtActor
from fabric.actor.core.apis.i_mgmt_authority import IMgmtAuthority
from fabric.actor.core.apis.i_mgmt_broker import IMgmtBroker
from fabric.actor.core.apis.i_mgmt_container import IMgmtContainer
from fabric.actor.core.apis.i_mgmt_controller import IMgmtController
from fabric.actor.core.manage.local.local_proxy import LocalProxy
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.reflection_utils import ReflectionUtils
from fabric.actor.security.auth_token import AuthToken


class LocalContainer(LocalProxy, IMgmtContainer):
    def __init__(self, manager: ManagementObject, auth: AuthToken):
        super().__init__(manager, auth)
        if not isinstance(manager, ContainerManagementObject):
            raise Exception("Invalid manager object. Required: {}".format(type(ContainerManagementObject)))

    def get_management_object(self, key: ID):
        try:
            obj = self.manager.get_management_object(key)

            if obj is None:
                return None

            desc_list = obj.get_proxies()

            if desc_list is None:
                raise Exception("Management object did not specify any proxies")
            desc = None
            for d in desc_list:
                if d.get_protocol() == Constants.ProtocolLocal:
                    desc = d
                    break

            if desc is None or desc.get_proxy_class() is None or desc.get_proxy_module() is None:
                raise Exception("Manager object did not specify local proxy")

            try:
                mgmt_obj = ReflectionUtils.create_instance_with_params(desc.get_proxy_module(), desc.get_proxy_class())(obj, self.auth)
                return mgmt_obj
            except Exception as e:
                raise Exception("Could not instantiate proxy {}".format(e))
        except Exception as e:
            raise e

    def get_actor(self, guid: ID):
        try:
            component = self.get_management_object(guid)

            if component is not None:
                if isinstance(component, IMgmtActor):
                    return component
                else:
                    self.last_exception = Exception("Invalid Management Object type")

            return None
        except Exception as e:
            traceback.print_exc()

    def get_actors(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_actors(self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_actors_from_database(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_actors_from_database(self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_actors_from_database_(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_actors_from_database(self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_actors_from_database_name_type_status(self, name: str, actor_type: int, status: int) -> list:
        self.clear_last()
        try:
            result = self.manager.get_actors_from_database_name_type_status(name, actor_type, status, self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_authorities(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_authorities(self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_brokers(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_brokers(self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_controllers(self) -> list:
        self.clear_last()
        try:
            result = self.manager.get_controllers(self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_proxies(self, protocol: str) -> list:
        self.clear_last()
        try:
            result = self.manager.get_proxies_by_protocol(protocol, self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_broker_proxies(self, protocol: str) -> list:
        self.clear_last()
        try:
            result = self.manager.get_broker_proxies(protocol, self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_authority_proxies(self, protocol: str) -> list:
        self.clear_last()
        try:
            result = self.manager.get_site_proxies(protocol, self.auth)
            self.last_status = result.status

            if result.status.get_code() == 0:
                return result.result

        except Exception as e:
            self.last_exception = e

        return None

    def get_controller(self, guid: ID):
        component = self.get_management_object(guid)

        if component is None:
            return None

        self.clear_last()

        if not isinstance(component, IMgmtController):
            self.last_exception = Exception("Invalid type")
        else:
            return component

        return None

    def get_broker(self, guid: ID):
        component = self.get_management_object(guid)

        if component is not None:
            if isinstance(component, IMgmtBroker):
                return component
            else:
                self.last_exception = Exception("Invalid Management Object type")

        return None

    def get_authority(self, guid: ID):
        component = self.get_management_object(guid)

        if component is not None:
            if isinstance(component, IMgmtAuthority):
                return component
            else:
                self.last_exception = Exception("Invalid Management Object type")

        return None