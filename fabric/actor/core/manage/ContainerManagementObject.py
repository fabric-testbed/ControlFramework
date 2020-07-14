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
from typing import TYPE_CHECKING

from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.manage.Converter import Converter
from fabric.actor.core.manage.ManagementObject import ManagementObject
from fabric.actor.core.manage.ProxyProtocolDescriptor import ProxyProtocolDescriptor
from fabric.actor.core.apis.IManagementObject import IManagementObject
from fabric.actor.core.manage.messages.ResultProxyMng import ResultProxyMng
from fabric.actor.core.registry.ActorRegistry import ActorRegistrySingleton
from fabric.actor.core.util.ID import ID
from fabric.actor.core.manage.messages.ResultActorMng import ResultActorMng
from fabric.message_bus.messages.ResultAvro import ResultAvro

if TYPE_CHECKING:
    from fabric.actor.core.apis.IContainerDatabase import IContainerDatabase
    from fabric.actor.security.AuthToken import AuthToken


class ContainerManagementObject(ManagementObject):
    def __init__(self):
        super().__init__()
        self.id = ID(Constants.ContainerManagmentObjectID)

    def register_protocols(self):
        from fabric.actor.core.manage.local.LocalContainer import LocalContainer
        local = ProxyProtocolDescriptor(Constants.ProtocolLocal, LocalContainer.__name__, LocalContainer.__module__)

        from fabric.actor.core.manage.kafka.KafkaContainer import KafkaContainer
        kakfa = ProxyProtocolDescriptor(Constants.ProtocolKafka, KafkaContainer.__name__, KafkaContainer.__module__)

        self.proxies = []
        self.proxies.append(local)
        self.proxies.append(kakfa)

    def save(self) -> dict:
        properties = super().save()
        properties[Constants.PropertyClassName] = ContainerManagementObject.__name__,
        properties[Constants.PropertyModuleName] = ContainerManagementObject.__name__

        return properties

    def get_container_management_database(self) -> IContainerDatabase:
        from fabric.actor.core.container.Globals import GlobalsSingleton
        return GlobalsSingleton.get().get_container().get_database()

    def get_actors_from_registry(self, atype: int, user: AuthToken):
        result = []
        actors = ActorRegistrySingleton.get().get_actors()
        if actors is not None:
            for a in actors:
                if atype == Constants.ActorTypeAll or atype == a.get_type():
                    result.append(a)
        return result

    def get_actors(self, caller: AuthToken) -> ResultActorMng:
        result = ResultActorMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            act_list = self.get_actors_from_registry(Constants.ActorTypeAll, caller)
            result.result = Converter.fill_actors(act_list)
        except Exception as e:
            self.logger.error("get_actors {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_actors_from_database(self, caller: AuthToken) -> ResultActorMng:
        result = ResultActorMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            act_list = None
            try:
                act_list = self.get_container_management_database().get_actors()
            except Exception as e:
                self.logger.error("get_actors_from_database {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)
                return result

            if act_list is not None:
                result.result = Converter.fill_actors_from_db(act_list)

        except Exception as e:
            self.logger.error("get_actors_from_database {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_actors_from_database_name_type_status(self, name: str, actor_type: int, status: int, caller: AuthToken) -> ResultActorMng:
        result = ResultActorMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            act_list = None
            try:
                act_list = self.get_container_management_database().get_actors(name=name, actor_type=actor_type)
            except Exception as e:
                self.logger.error("get_actors_from_database {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)
                return result

            if act_list is not None:
                result.result = Converter.fill_actors_from_db_status(act_list, status)

        except Exception as e:
            self.logger.error("get_actors_from_database {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_controllers(self, caller: AuthToken) -> ResultActorMng:
        result = ResultActorMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            act_list = self.get_actors_from_registry(Constants.ActorTypeController, caller)
            result.result = Converter.fill_actors(act_list)
        except Exception as e:
            self.logger.error("get_controllers {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

    def get_brokers(self, caller: AuthToken) -> ResultActorMng:
        result = ResultActorMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            act_list = self.get_actors_from_registry(Constants.ActorTypeBroker, caller)
            result.result = Converter.fill_actors(act_list)
        except Exception as e:
            self.logger.error("get_brokers {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

    def get_authorities(self, caller: AuthToken) -> ResultActorMng:
        result = ResultActorMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            act_list = self.get_actors_from_registry(Constants.ActorTypeSiteAuthority, caller)
            result.result = Converter.fill_actors(act_list)
        except Exception as e:
            self.logger.error("get_authorities {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

    def get_management_object(self, guid: ID) -> IManagementObject:
        from fabric.actor.core.container.Globals import GlobalsSingleton
        return GlobalsSingleton.get().get_container().get_management_object_manager().get_management_object(guid)

    def get_broker_proxies(self, protocol: str, caller: AuthToken) -> ResultProxyMng:
        result = ResultProxyMng()
        result.status = ResultAvro()

        if caller is None or protocol is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            proxies = ActorRegistrySingleton.get().get_broker_proxies(protocol)
            result.result = Converter.fill_proxies(proxies)
        except Exception as e:
            self.logger.error("get_broker_proxies {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

    def get_site_proxies(self, protocol: str, caller: AuthToken) -> ResultProxyMng:
        result = ResultProxyMng()
        result.status = ResultAvro()

        if caller is None or protocol is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            proxies = ActorRegistrySingleton.get().get_site_proxies(protocol)
            result.result = Converter.fill_proxies(proxies)
        except Exception as e:
            self.logger.error("get_site_proxies {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

    def get_proxies_by_protocol(self, protocol: str, caller: AuthToken) -> ResultProxyMng:
        result = ResultProxyMng()
        result.status = ResultAvro()

        if caller is None or protocol is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            proxies = ActorRegistrySingleton.get().get_proxies(protocol)
            result.result = Converter.fill_proxies(proxies)
        except Exception as e:
            self.logger.error("get_proxies {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)