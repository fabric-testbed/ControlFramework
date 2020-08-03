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

from fabric.actor.core.apis.i_authority import IAuthority
from fabric.actor.core.apis.i_broker import IBroker
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.proxies.kafka.kafka_authority_proxy import KafkaAuthorityProxy
from fabric.actor.core.proxies.kafka.kafka_broker_proxy import KafkaBrokerProxy
from fabric.actor.core.proxies.kafka.kafka_retun import KafkaReturn
from fabric.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric.actor.core.proxies.i_proxy_factory import IProxyFactory

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_actor_identity import IActorIdentity
    from fabric.actor.core.apis.i_proxy import IProxy
    from fabric.actor.core.proxies.actor_location import ActorLocation
    from fabric.actor.core.apis.i_callback_proxy import ICallbackProxy


class KafkaProxyFactory(IProxyFactory):
    def new_proxy(self, identity: IActorIdentity, location: ActorLocation, actor_type: str = None) -> IProxy:
        result = None
        actor = ActorRegistrySingleton.get().get_actor(identity.get_name())

        if actor is not None:
            descriptor = location.get_descriptor()
            if descriptor is not None and descriptor.get_location() is not None:
                if isinstance(actor, IAuthority):
                    result = KafkaAuthorityProxy(descriptor.get_location(), actor.get_identity(), actor.get_logger())

                elif isinstance(actor, IBroker):
                    result = KafkaBrokerProxy(descriptor.get_location(), actor.get_identity(), actor.get_logger())
        else:
            kafka_topic = location.get_location()

            if actor_type is not None:
                from fabric.actor.core.container.globals import GlobalsSingleton
                if actor_type.lower() == Constants.AUTHORITY or actor_type.lower() == Constants.SITE:
                    result = KafkaAuthorityProxy(kafka_topic, identity.get_identity(), GlobalsSingleton.get().get_logger())

                elif actor_type.lower() == Constants.BROKER:
                    result = KafkaBrokerProxy(kafka_topic, identity.get_identity(), GlobalsSingleton.get().get_logger())
                else:
                    raise Exception("Unsupported proxy type: {}".format(actor_type))
            else:
                raise Exception("Missing proxy type")
        return result

    def new_callback(self, identity: IActorIdentity, location: ActorLocation) -> ICallbackProxy:
        result = None
        actor = ActorRegistrySingleton.get().get_actor(identity.get_name())
        if actor is not None:
            descriptor = location.get_descriptor()
            if descriptor is not None and descriptor.get_location() is not None:
                kafka_topic = descriptor.get_location()
                result = KafkaReturn(kafka_topic, actor.get_identity(), actor.get_logger())
        return result
