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
from fabric_cf.actor.core.apis.abc_authority_proxy import ABCAuthorityProxy
from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
from fabric_cf.actor.core.apis.abc_proxy import ABCProxy


class ProxyRegistry:
    class ProtocolEntry:
        def __init__(self):
            # all proxies
            self.proxies = {}
            # all proxies to brokers
            self.broker_proxies = []
            # all proxies to sites
            self.site_proxies = []

        def clear(self):
            self.proxies.clear()
            self.broker_proxies.clear()
            self.site_proxies.clear()

    def __init__(self):
        self.protocols = {}

    def clear(self):
        for protocol in self.protocols.values():
            protocol.clear()

    def get_broker_proxies(self, *, protocol: str):
        if protocol not in self.protocols:
            return None

        entry = self.protocols[protocol]
        return entry.broker_proxies

    def get_site_proxies(self, *, protocol: str):
        if protocol not in self.protocols:
            return None

        entry = self.protocols[protocol]
        return entry.site_proxies

    def get_proxies(self, *, protocol: str):
        if protocol not in self.protocols:
            return None

        entry = self.protocols[protocol]
        return entry.proxies.values()

    def get_proxy(self, *, protocol: str, actor_name: str):
        if protocol not in self.protocols:
            return None
        entry = self.protocols[protocol]
        if actor_name not in entry.proxies:
            return None
        return entry.proxies[actor_name]

    def register_proxy(self, *, proxy: ABCProxy):
        protocol = proxy.get_type()

        entry = None
        if protocol not in self.protocols:
            entry = self.ProtocolEntry()
            self.protocols[protocol] = entry
        else:
            entry = self.protocols[protocol]

        name = proxy.get_identity().get_name()
        if name not in entry.proxies:
            entry.proxies[name] = proxy

            if isinstance(proxy, ABCAuthorityProxy):
                entry.site_proxies.append(proxy)

            if isinstance(proxy, ABCBrokerProxy):
                entry.broker_proxies.append(proxy)

    def unregister(self, *, actor_name: str):
        for protocol in self.protocols.values():
            if actor_name in protocol.proxies:
                proxy = protocol.proxies[actor_name]
                protocol.proxies.pop(actor_name)
                if isinstance(proxy, ABCAuthorityProxy):
                    protocol.site_proxies.remove(proxy)

                if isinstance(proxy, ABCBrokerProxy):
                    protocol.broker_proxies.remove(proxy)
