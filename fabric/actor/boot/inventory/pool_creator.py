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

from fabric.actor.boot.inventory.resource_pool_factory import ResourcePoolFactory
from fabric.actor.core.plugins.config.configuration_mapping import ConfigurationMapping
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.reflection_utils import ReflectionUtils
from fabric.actor.core.util.resource_data import ResourceData

if TYPE_CHECKING:
    from fabric.actor.core.common.resource_pool_descriptor import ResourcePoolDescriptor
    from fabric.actor.boot.inventory.i_resource_pool_factory import IResourcePoolFactory
    from fabric.actor.core.plugins.substrate.AuthoritySubstrate import AuthoritySubstrate


class PoolCreator:
    def __init__(self, substrate: AuthoritySubstrate = None, pools: dict = None):
        self.substrate = substrate
        self.pools = pools
        self.container = None
        from fabric.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()

    def get_factory(self, rd: ResourcePoolDescriptor) -> IResourcePoolFactory:
        factory = None
        if rd.get_pool_factory_module() is None or rd.get_pool_factory_class() is None:
            factory = ResourcePoolFactory()
        else:
            try:
                factory = ReflectionUtils.create_instance(rd.get_pool_factory_module(), rd.get_pool_factory_class())
            except Exception as e:
                raise Exception("Could not instantiate class= {}.{} {}".format(rd.get_pool_factory_module(),
                                                                               rd.get_pool_factory_class(), e))

        factory.set_substrate(self.substrate)
        factory.set_descriptor(rd)
        return factory

    def process(self):
        from fabric.actor.core.container.globals import GlobalsSingleton
        self.container = GlobalsSingleton.get().get_container()
        for pool in self.pools.values():
            self.logger.debug("Creating resource pool {} of Actor {}".format(
                pool.get_resource_type_label(), self.substrate.get_actor().get_name()))

            factory = self.get_factory(pool)
            pool = factory.get_descriptor()
            rd = ResourceData()
            rd.resource_properties = pool.save(rd.resource_properties, None)

            rd.local_properties = ResourceData.merge_properties(pool.pool_properties, rd.local_properties)

            create_pool_result = self.substrate.get_pool_manager().create_pool(ID(), pool.get_resource_type_label(),
                                                                               pool.get_resource_type(), rd)

            if create_pool_result.code != 0:
                raise Exception("Could not create resource pool: {}. error={}".format(pool.get_resource_type_label(),
                                                                                      create_pool_result.code))

            self.register_handler(pool)
            source = factory.create_source_reservation(create_pool_result.slice)

            try:
                self.logger.debug("Adding source reservation to database {}".format(source))
                self.logger.debug("Source reservation has resources of type {}"
                                  .format(source.get_resources().get_resources().__class__.__name__))
                self.logger.debug("Source reservation has delegation of type {}"
                                  .format(source.get_resources().get_resources().get_ticket().get_delegation().__class__.__name__))

                self.substrate.get_database().add_reservation(source)
            except Exception as e:
                raise Exception("Could not add source reservation to database {}".format(e))

    def register_handler(self, pool: ResourcePoolDescriptor):
        handler_module = pool.get_handler_module()
        handler_class = pool.get_handler_class()

        if handler_class is None or handler_module is None:
            return

        config = self.substrate.get_config()
        config_map = ConfigurationMapping()
        config_map.set_key(str(pool.get_resource_type()))
        config_map.set_class_name(handler_class)
        config_map.set_module_name(handler_module)
        config_map.set_properties(pool.get_handler_properties())

        config.add_config_mapping(config_map)