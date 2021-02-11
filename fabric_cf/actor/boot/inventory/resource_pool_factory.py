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

from fabric_cf.actor.boot.inventory.i_resource_pool_factory import IResourcePoolFactory, ResourcePoolException
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_substrate import ISubstrate


class ResourcePoolFactory(IResourcePoolFactory):
    def __init__(self):
        # The resource pool descriptor. Its initial version is passed during initialization.
        # The factory can manipulate it as it sees fit and returns it back the the PoolCreator.
        self.desc = None
        # The actor's substrate
        self.substrate = None
        # The authority proxy for this actor.
        self.proxy = None
        # Slice representing the resource pool.
        self.slice_obj = None

    def set_substrate(self, *, substrate: ISubstrate):
        self.substrate = substrate
        auth = self.substrate.get_actor().get_identity()
        try:
            self.proxy = ActorRegistrySingleton.get().get_proxy(protocol=Constants.PROTOCOL_KAFKA,
                                                                actor_name=auth.get_name())
            if self.proxy is None:
                raise ResourcePoolException("Missing proxy")
        except Exception as e:
            raise ResourcePoolException("Could not obtain authority proxy: {} {}".format(auth.get_name(), e))
