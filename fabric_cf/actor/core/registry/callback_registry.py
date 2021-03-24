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
from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy


class CallbackRegistry:
    def __init__(self):
        self.protocols = {}

    def clear(self):
        for protocol in self.protocols.values():
            protocol.clear()
        self.protocols.clear()

    def get_callback(self, *, protocol: str, actor_name: str):
        protocol_table = self.protocols.get(protocol, None)

        if protocol_table is None:
            return None

        return protocol_table.get(actor_name, None)

    def register_callback(self, *, callback: ABCCallbackProxy):
        protocol = callback.get_type()

        entry = self.protocols.get(protocol, None)

        if entry is None:
            entry = {}

        entry[callback.get_identity().get_name()] = callback
        self.protocols[protocol] = entry

    def unregister(self, *, actor_name: str):
        for protocol in self.protocols:
            if actor_name in protocol:
                protocol.pop(actor_name)
