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

from fabric_cf.actor.core.apis.i_authority import IAuthority
from fabric_cf.actor.core.apis.i_broker import IBroker
from fabric_cf.actor.core.proxies.local.local_authority import LocalAuthority
from fabric_cf.actor.core.proxies.local.local_broker import LocalBroker
from fabric_cf.actor.core.proxies.local.local_return import LocalReturn
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.apis.i_proxy_factory import IProxyFactory

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_actor_identity import IActorIdentity
    from fabric_cf.actor.core.proxies.actor_location import ActorLocation
    from fabric_cf.actor.core.apis.i_callback_proxy import ICallbackProxy
    from fabric_cf.actor.core.apis.i_proxy import IProxy


class LocalProxyFactory(IProxyFactory):
    def new_callback(self, *, identity: IActorIdentity, location: ActorLocation) -> ICallbackProxy:
        actor = ActorRegistrySingleton.get().get_actor(actor_name_or_guid=identity.get_name())
        if actor is not None:
            return LocalReturn(actor=actor)
        return None

    def new_proxy(self, *, identity: IActorIdentity, location: ActorLocation, proxy_type: str = None) -> IProxy:
        actor = ActorRegistrySingleton.get().get_actor(actor_name_or_guid=identity.get_name())
        if actor is not None:
            if isinstance(actor, IAuthority):
                return LocalAuthority(actor=actor)
            elif isinstance(actor, IBroker):
                return LocalBroker(actor=actor)
        return None
