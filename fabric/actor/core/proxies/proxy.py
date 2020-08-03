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

from fabric.actor.core.apis.i_actor import IActor
from fabric.actor.core.apis.i_base_plugin import IBasePlugin
from fabric.actor.core.apis.i_callback_proxy import ICallbackProxy
from fabric.actor.core.apis.i_concrete_set import IConcreteSet
from fabric.actor.core.apis.i_proxy import IProxy
from fabric.actor.core.kernel.sesource_set import ResourceSet
from fabric.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.security.auth_token import AuthToken


class Proxy(IProxy):
    """
    Proxy class represents a stub to an actor. Proxies define a general interface, which is implementation
    independent and enables easy implementation of new communication protocols.
    """
    PropertyProxyType = "ProxyType"
    PropertyProxyActorAuth = "ProxyActorAuth"
    PropertyProxyActorName = "ProxyActorName"
    PropertyProxyActorGuid = "ProxyActorGuid"
    PropertyProxyCallback = "ProxyCallback"

    @staticmethod
    def get_callback(actor: IActor, protocol: str) -> ICallbackProxy:
        """
        Obtains a callback for the specified actor
        @param actor actor
        @param protocol protocol
        @return ICallbackProxy
        """
        if actor is None:
            raise Exception("actor cannot be None")

        callback = ActorRegistrySingleton.get().get_callback(protocol, actor.get_name())
        if callback is None:
            raise Exception("Could not obtain callback proxy: protocol={}".format(protocol))
        return callback

    @staticmethod
    def get_proxy(properties: dict) -> IProxy:
        """
        Obtains a proxy object from the specified properties list. If a suitable
        proxy object has already been created and registered with the
        ActorRegistry, the already existing object is returned and
        no new object is created. Otherwise, the method creates the proxy object
        and registers it with the ActorRegistry
        @param properties Properties list representing the proxy
        @return IProxy
        @throws Exception in case of error
        """
        name = properties[Proxy.PropertyProxyActorName]
        type = properties[Proxy.PropertyProxyType]
        is_callback = properties[Proxy.PropertyProxyCallback]
        proxy = None
        if is_callback:
            proxy = ActorRegistrySingleton.get().get_callback(type, name)
        else:
            proxy = ActorRegistrySingleton.get().get_proxy(type, name)

        if proxy is None:
            Proxy.recover_proxy(properties, True)
        else:
            # TODO
            proxy = Proxy.recover_proxy(properties, False)
        return proxy

    @staticmethod
    def recover_proxy(properties: dict, register: bool):
        """
        Creates a proxy list from a properties list representing the
        serialization of the proxy. Optionally, the resulting object may be
        registered with the ActorRegistry so that it becomes visible
        to the rest of the system.
        @param properties Properties dict representing the proxy
        @param register If true, the resulting proxy is registered with the
                   container's ActorRegistry
        @return Proxy
        @throws Exception in case of error
        """
        # TODO restore
        proxy = None
        name = "unknown actor"
        if Proxy.PropertyProxyActorName in properties:
            name = properties[Proxy.PropertyProxyActorName]

        from fabric.actor.core.container.globals import GlobalsSingleton
        proxy.set_logger(GlobalsSingleton.get().get_logger())

        if register:
            if proxy.callback:
                ActorRegistrySingleton.get().register_callback(proxy)
            else:
                ActorRegistrySingleton.get().register_proxy(proxy)
        return proxy

    @staticmethod
    def decode(encoded, plugin: IBasePlugin) -> IConcreteSet:
        try:
            decoded_resource = pickle.loads(encoded)
            decoded_resource.restore(plugin, None)
            return decoded_resource
        except Exception as e:
            print("Exception occurred while decoding {}".format(e))
        return None

    def __init__(self, auth: AuthToken = None):
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

    def set_logger(self, logger):
        self.logger = logger

    def get_logger(self):
        return self.logger

    def abstract_clone_authority(self, rset: ResourceSet) -> ResourceSet:
        """
        Clones the resource set, but without any of the concrete sets. Preserves
        only the configuration properties. This method should be used when
        sending a redeem/extend/close request to an authority.
        @param rset resource set
        @return a resources set that is a copy of the current but without any
                concrete sets.
        """
        new_resource_data = ResourceData()
        properties = rset.get_config_properties()

        if properties is None:
            properties = {}
        else:
            properties = properties.copy()

        temp = ResourceData.merge_properties(properties, new_resource_data.get_configuration_properties())
        new_resource_data.configuration_properties = temp
        return ResourceSet(units=rset.get_units(), rtype=rset.get_type(), rdata=new_resource_data)

    def abstract_clone_broker(self, rset: ResourceSet) -> ResourceSet:
        """
        Clones the resource set, but without any of the concrete sets. Preserves
        only the configuration properties. This method should be used when
        sending a redeem/extend/close request to an authority.
        @param rset resource set
        @return a resources set that is a copy of the current but without any
                concrete sets.
        """
        new_resource_data = ResourceData()
        properties = rset.get_request_properties()

        if properties is None:
            properties = {}
        else:
            properties = properties.copy()

        temp = ResourceData.merge_properties(properties, new_resource_data.get_request_properties())
        new_resource_data.request_properties = temp
        return ResourceSet(units=rset.get_units(), rtype=rset.get_type(), rdata=new_resource_data)

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
        new_resource_data = ResourceData()
        properties = rset.get_resource_properties()

        if properties is None:
            properties = {}
        else:
            properties = properties.copy()

        temp = ResourceData.merge_properties(properties, new_resource_data.get_resource_properties())
        new_resource_data.resource_properties = temp
        return ResourceSet(units=rset.get_units(), rtype=rset.get_type(), rdata=new_resource_data)
