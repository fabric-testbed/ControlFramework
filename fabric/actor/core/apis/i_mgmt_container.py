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

from fabric.actor.core.apis.i_component import IComponent

if TYPE_CHECKING:
    from fabric.actor.core.util.id import ID


class IMgmtContainer(IComponent):
    def get_actor(self, guid: ID):
        """
        Obtains the specified actor.
        @param guid
                   actor guid
        @return proxy to the actor on success, null otherwise. The returned proxy
        """
        raise NotImplementedError

    def get_controller(self, guid: ID):
        """
        Obtains the specified controller.
        @param guid guid
        @return specified controller
        """
        raise NotImplementedError

    def get_broker(self, guid: ID):
        """
        Obtains the specified broker
        @param guid guid
        @return specified broker
        """
        raise NotImplementedError

    def get_authority(self, guid: ID):
        """
        Obtains the specified authority
        @param guid guid
        @return specified authority
        """
        raise NotImplementedError

    def get_actors(self) -> list:
        """
        Obtains a list of all active actors in the container.
        @return list of actors. Always non-null.
        """
        raise NotImplementedError

    def get_actors_from_database(self) -> list:
        """
        Obtains a list of all active and suspended actors in the container.
        @return list of actors. Always non-null.
        """
        raise NotImplementedError

    def get_authorities(self) -> list:
        """
        Obtains a list of all active site authorities.
        @return list of actors. Always non-null.
        """
        raise NotImplementedError

    def get_brokers(self) -> list:
        """
        Obtains a list of all active brokers.
        @return list of actors. Always non-null.
        """
        raise NotImplementedError

    def get_controllers(self) -> list:
        """
        Obtains a list of all active controllers.
        @return list of actors. Always non-null.
        """
        raise NotImplementedError

    def get_proxies(self, protocol: str) -> list:
        """
        Obtains a list of all proxies to actors using the specified protocol
        @param protocol protocol
        @return list of proxies. Always non-null.
        """
        raise NotImplementedError

    def get_broker_proxies(self, protocol: str) -> list:
        """
        Obtains a list of all proxies to brokers using the specified protocol
        @param protocol protocol
        @return list of proxies. Always non-null.
        """
        raise NotImplementedError

    def get_authority_proxies(self, protocol: str) -> list:
        """
        Obtains a list of all proxies to brokers using the specified protocol
        @param protocol protocol
        @return list of proxies. Always non-null.
        """
        raise NotImplementedError

    def get_management_object(self, key: ID):
        """
        Obtains the specified management object
        @param key management object id
        @return management object proxy on success, null otherwise. The returned
                proxy will share the same credentials.
        """
        raise NotImplementedError

