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
import pickle
import traceback

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin
from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
from fabric_cf.actor.core.apis.abc_concrete_set import ABCConcreteSet
from fabric_cf.actor.core.apis.abc_proxy import ABCProxy
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ProxyException
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.security.auth_token import AuthToken


class Proxy(ABCProxy):
    """
    Proxy class represents a stub to an actor. Proxies define a general interface, which is implementation
    independent and enables easy implementation of new communication protocols.
    """
    PropertyProxyActorName = "prx_name"

    @staticmethod
    def get_callback(*, actor: ABCActorMixin, protocol: str) -> ABCCallbackProxy:
        """
        Obtains a callback for the specified actor
        @param actor actor
        @param protocol protocol
        @return ICallbackProxy
        """
        if actor is None:
            raise ProxyException(Constants.NOT_SPECIFIED_PREFIX.format("actor"))

        callback = ActorRegistrySingleton.get().get_callback(protocol=protocol, actor_name=actor.get_name())
        if callback is None:
            raise ProxyException("Could not obtain callback proxy: protocol={}".format(protocol))
        return callback

    @staticmethod
    def get_proxy(*, proxy_reload_from_db) -> ABCProxy:
        """
        Obtains a proxy object from the specified properties list. If a suitable
        proxy object has already been created and registered with the
        ActorRegistry, the already existing object is returned and
        no new object is created. Otherwise, the method creates the proxy object
        and registers it with the ActorRegistry
        @param proxy_reload_from_db proxy_reload_from_db
        @return IProxy
        @throws Exception in case of error
        """
        proxy_type = proxy_reload_from_db.get_type()
        name = proxy_reload_from_db.get_name()

        is_callback = proxy_reload_from_db.callback

        proxy = None
        if is_callback:
            proxy = ActorRegistrySingleton.get().get_callback(protocol=proxy_type, actor_name=name)
        else:
            proxy = ActorRegistrySingleton.get().get_proxy(protocol=proxy_type, actor_name=name)

        if proxy is None:
            proxy = Proxy.recover_proxy(proxy_reload_from_db=proxy_reload_from_db, register=True)
        else:
            proxy = Proxy.recover_proxy(proxy_reload_from_db=proxy_reload_from_db, register=False)
        return proxy

    @staticmethod
    def recover_proxy(*, proxy_reload_from_db: ABCProxy, register: bool) -> ABCProxy:
        """
        Creates a proxy list from a properties list representing the
        serialization of the proxy. Optionally, the resulting object may be
        registered with the ActorRegistry so that it becomes visible
        to the rest of the system.
        @param proxy_reload_from_db proxy_reload_from_db
        @param register If true, the resulting proxy is registered with the
                   container's ActorRegistry
        @return Proxy
        @throws Exception in case of error
        """

        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        proxy_reload_from_db.set_logger(logger=GlobalsSingleton.get().get_logger())

        if register:
            if proxy_reload_from_db.callback:
                ActorRegistrySingleton.get().register_callback(callback=proxy_reload_from_db)
            else:
                ActorRegistrySingleton.get().register_proxy(proxy=proxy_reload_from_db)
        return proxy_reload_from_db

    @staticmethod
    def decode(*, encoded, plugin: ABCBasePlugin) -> ABCConcreteSet:
        try:
            decoded_resource = pickle.loads(encoded)
            print("Decoded object is of type={}".format(type(decoded_resource)))
            decoded_resource.restore(plugin=plugin, reservation=None)
            return decoded_resource
        except Exception as e:
            traceback.print_exc()
            print("Exception occurred while decoding {}".format(e))
        return None

    def __init__(self, *, auth: AuthToken = None):
        self.logger = None
        self.proxy_type = None
        self.callback = False
        self.actor_name = None
        if auth is not None:
            self.actor_name = auth.get_name()
        self.actor_guid = None
        if auth is not None:
            self.actor_guid = auth.get_guid()
        self.auth = auth

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None

    def get_guid(self) -> ID:
        return self.actor_guid

    def get_identity(self) -> AuthToken:
        return self.auth

    def get_name(self) -> str:
        return self.actor_name

    def get_type(self) -> str:
        return self.proxy_type

    def set_logger(self, *, logger):
        self.logger = logger

    def get_logger(self):
        return self.logger

    def abstract_clone_authority(self, *, rset: ResourceSet) -> ResourceSet:
        """
        Clones the resource set, but without any of the concrete sets. Preserves
        only the configuration properties. This method should be used when
        sending a redeem/extend/close request to an authority.
        @param rset resource set
        @return a resources set that is a copy of the current but without any
                concrete sets.
        """
        return ResourceSet(units=rset.get_units(), rtype=rset.get_type(), sliver=rset.get_sliver())

    def abstract_clone_broker(self, *, rset: ResourceSet) -> ResourceSet:
        """
        Clones the resource set, but without any of the concrete sets. Preserves
        only the configuration properties. This method should be used when
        sending a redeem/extend/close request to an authority.
        @param rset resource set
        @return a resources set that is a copy of the current but without any
                concrete sets.
        """
        return ResourceSet(units=rset.get_units(), rtype=rset.get_type(), sliver=rset.get_sliver())

    @staticmethod
    def abstract_clone_return(rset: ResourceSet) -> ResourceSet:
        """
        Clones the resource set, but without any of the concrete sets. Preserves
        only the configuration properties. This method should be used when
        sending a redeem/extend/close request to an authority.
        @param rset resource set
        @return a resources set that is a copy of the current but without any
                concrete sets.
        """
        return ResourceSet(units=rset.get_units(), rtype=rset.get_type(), sliver=rset.get_sliver())
