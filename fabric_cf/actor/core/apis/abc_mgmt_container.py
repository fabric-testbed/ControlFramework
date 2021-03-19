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

from abc import abstractmethod
from typing import TYPE_CHECKING, List

from fabric_cf.actor.core.apis.abc_component import ABCComponent


if TYPE_CHECKING:
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.apis.abc_mgmt_actor import ABCMgmtActor
    from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
    from fabric_cf.actor.core.apis.abc_mgmt_broker_mixin import ABCMgmtBrokerMixin
    from fabric_cf.actor.core.apis.abc_mgmt_authority import ABCMgmtAuthority
    from fabric_mb.message_bus.messages.actor_avro import ActorAvro
    from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro


class ABCMgmtContainer(ABCComponent):
    """
    Interface for Management Container
    """
    @abstractmethod
    def get_actor(self, *, guid: ID) -> ABCMgmtActor:
        """
        Obtains the specified actor.
        @param guid
                   actor guid
        @return proxy to the actor on success, null otherwise. The returned proxy
        """

    @abstractmethod
    def get_controller(self, *, guid: ID) -> ABCMgmtControllerMixin:
        """
        Obtains the specified orchestrator.
        @param guid guid
        @return specified orchestrator
        """

    @abstractmethod
    def get_broker(self, *, guid: ID) -> ABCMgmtBrokerMixin:
        """
        Obtains the specified broker
        @param guid guid
        @return specified broker
        """

    @abstractmethod
    def get_authority(self, *, guid: ID) -> ABCMgmtAuthority:
        """
        Obtains the specified authority
        @param guid guid
        @return specified authority
        """

    @abstractmethod
    def get_actors(self) -> List[ActorAvro]:
        """
        Obtains a list of all active actors in the container.
        @return list of actors. Always non-null.
        """

    @abstractmethod
    def get_actors_from_database(self) -> List[ActorAvro]:
        """
        Obtains a list of all active and suspended actors in the container.
        @return list of actors. Always non-null.
        """

    @abstractmethod
    def get_authorities(self) -> List[ActorAvro]:
        """
        Obtains a list of all active site authorities.
        @return list of actors. Always non-null.
        """

    @abstractmethod
    def get_brokers(self) -> List[ActorAvro]:
        """
        Obtains a list of all active brokers.
        @return list of actors. Always non-null.
        """

    @abstractmethod
    def get_controllers(self) -> List[ActorAvro]:
        """
        Obtains a list of all active controllers.
        @return list of actors. Always non-null.
        """

    @abstractmethod
    def get_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        """
        Obtains a list of all proxies to actors using the specified protocol
        @param protocol protocol
        @return list of proxies. Always non-null.
        """

    @abstractmethod
    def get_broker_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        """
        Obtains a list of all proxies to brokers using the specified protocol
        @param protocol protocol
        @return list of proxies. Always non-null.
        """

    @abstractmethod
    def get_authority_proxies(self, *, protocol: str) -> List[ProxyAvro]:
        """
        Obtains a list of all proxies to brokers using the specified protocol
        @param protocol protocol
        @return list of proxies. Always non-null.
        """

    @abstractmethod
    def get_management_object(self, *, key: ID) -> ABCComponent:
        """
        Obtains the specified management object
        @param key management object id
        @return management object proxy on success, null otherwise. The returned
                proxy will share the same credentials.
        """
