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

from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.manage.converter import Converter
from fabric_cf.actor.core.manage.management_object import ManagementObject
from fabric_cf.actor.core.manage.proxy_protocol_descriptor import ProxyProtocolDescriptor
from fabric_cf.actor.core.apis.abc_management_object import ABCManagementObject
from fabric_mb.message_bus.messages.result_proxy_avro import ResultProxyAvro
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.util.id import ID
from fabric_mb.message_bus.messages.result_actor_avro import ResultActorAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_container_database import ABCContainerDatabase
    from fabric_cf.actor.security.auth_token import AuthToken


class ContainerManagementObject(ManagementObject):
    def __init__(self):
        super().__init__()
        self.id = ID(uid=Constants.CONTAINER_MANAGMENT_OBJECT_ID)

    def register_protocols(self):
        from fabric_cf.actor.core.manage.local.local_container import LocalContainer
        local = ProxyProtocolDescriptor(protocol=Constants.PROTOCOL_LOCAL,
                                        proxy_class=LocalContainer.__name__,
                                        proxy_module=LocalContainer.__module__)

        from fabric_cf.actor.core.manage.kafka.kafka_container import KafkaContainer
        kakfa = ProxyProtocolDescriptor(protocol=Constants.PROTOCOL_KAFKA,
                                        proxy_class=KafkaContainer.__name__,
                                        proxy_module=KafkaContainer.__module__)

        self.proxies = []
        self.proxies.append(local)
        self.proxies.append(kakfa)

    def save(self) -> dict:
        properties = super().save()
        properties[Constants.PROPERTY_CLASS_NAME] = ContainerManagementObject.__name__
        properties[Constants.PROPERTY_MODULE_NAME] = ContainerManagementObject.__module__

        return properties

    @staticmethod
    def get_container_management_database() -> ABCContainerDatabase:
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        return GlobalsSingleton.get().get_container().get_database()

    @staticmethod
    def get_actors_from_registry(*, atype: int, caller: AuthToken):
        result = []
        actors = ActorRegistrySingleton.get().get_actors()
        if actors is not None:
            for a in actors:
                if atype == ActorType.All.value or atype == a.get_type():
                    result.append(a)
        return result

    def do_get_actors(self, *, atype: int, caller: AuthToken) -> ResultActorAvro:
        result = ResultActorAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            act_list = self.get_actors_from_registry(atype=atype, caller=caller)
            result.actors = Converter.fill_actors(act_list=act_list)
        except Exception as e:
            self.logger.error("get_actors {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_actors(self, *, caller: AuthToken) -> ResultActorAvro:
        return self.do_get_actors(atype=ActorType.All.value, caller=caller)

    def do_get_actors_from_database(self, *, caller: AuthToken, status: int = None) -> ResultActorAvro:
        result = ResultActorAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            act_list = None
            try:
                act_list = self.get_container_management_database().get_actors()
            except Exception as e:
                self.logger.error("get_actors_from_database {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if act_list is not None:
                if status is None:
                    result.actors = Converter.fill_actors_from_db(act_list=act_list)
                else:
                    result.actors = Converter.fill_actors_from_db_status(act_list=act_list, status=status)

        except Exception as e:
            self.logger.error("get_actors_from_database {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_actors_from_database(self, *, caller: AuthToken) -> ResultActorAvro:
        return self.do_get_actors_from_database(caller=caller)

    def get_actors_from_database_name_type_status(self, *, name: str, actor_type: int, status: int,
                                                  caller: AuthToken) -> ResultActorAvro:
        return self.do_get_actors_from_database(caller=caller, status=status)

    def get_controllers(self, *, caller: AuthToken) -> ResultActorAvro:
        return self.do_get_actors(atype=ActorType.Orchestrator.value, caller=caller)

    def get_brokers(self, *, caller: AuthToken) -> ResultActorAvro:
        return self.do_get_actors(atype=ActorType.Broker.value, caller=caller)

    def get_authorities(self, *, caller: AuthToken) -> ResultActorAvro:
        return self.do_get_actors(atype=ActorType.Authority.value, caller=caller)

    def get_management_object(self, *, key: ID) -> ABCManagementObject:
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        return GlobalsSingleton.get().get_container().get_management_object_manager().get_management_object(key=key)

    def do_get_proxies(self, *, atype: ActorType, protocol: str, caller: AuthToken) -> ResultProxyAvro:
        result = ResultProxyAvro()
        result.status = ResultAvro()

        if caller is None or protocol is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            proxies = None
            if atype == ActorType.Broker:
                proxies = ActorRegistrySingleton.get().get_broker_proxies(protocol=protocol)
            elif atype == ActorType.Authority:
                proxies = ActorRegistrySingleton.get().get_site_proxies(protocol=protocol)
            else:
                proxies = ActorRegistrySingleton.get().get_proxies(protocol=protocol)

            result.proxies = Converter.fill_proxies(proxies=proxies)
        except Exception as e:
            self.logger.error("get_broker_proxies {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_broker_proxies(self, *, protocol: str, caller: AuthToken) -> ResultProxyAvro:
        return self.do_get_proxies(atype=ActorType.Broker, protocol=protocol, caller=caller)

    def get_site_proxies(self, *, protocol: str, caller: AuthToken) -> ResultProxyAvro:
        return self.do_get_proxies(atype=ActorType.Authority, protocol=protocol, caller=caller)

    def get_proxies_by_protocol(self, *, protocol: str, caller: AuthToken) -> ResultProxyAvro:
        return self.do_get_proxies(atype=ActorType.All, protocol=protocol, caller=caller)
