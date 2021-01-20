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
from typing import TYPE_CHECKING, Dict

from fabric_cf.actor.boot.inventory.neo4j_resource_pool_factory import Neo4jResourcePoolFactory
from fabric_cf.actor.boot.inventory.resource_pool_factory import ResourcePoolFactory
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.pool_manager import PoolManagerError
from fabric_cf.actor.core.plugins.config.configuration_mapping import ConfigurationMapping
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reflection_utils import ReflectionUtils
from fabric_cf.actor.core.util.resource_data import ResourceData
from fabric_cf.actor.core.util.resource_type import ResourceType

if TYPE_CHECKING:
    from fabric_cf.actor.core.common.resource_pool_descriptor import ResourcePoolDescriptor
    from fabric_cf.actor.boot.inventory.i_resource_pool_factory import IResourcePoolFactory
    from fabric_cf.actor.core.plugins.substrate.authority_substrate import AuthoritySubstrate


class PoolCreator:
    """
    Responsible for setting up inventory pools on startup
    """
    def __init__(self, *, substrate: AuthoritySubstrate = None, pools: dict = None, neo4j_config: dict = None):
        self.substrate = substrate
        self.pools = pools
        self.neo4j_config = neo4j_config
        self.container = None
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()

    def get_factory2(self):
        """
        Create Neo4j ResourcePool Factory instance
        """
        factory = Neo4jResourcePoolFactory()
        factory.set_substrate(substrate=self.substrate)
        return factory

    def get_factory(self, *, rd: ResourcePoolDescriptor) -> IResourcePoolFactory:
        """
        Create ResourcePool Factory instance
        @param rd resource pool descriptor
        """
        factory = None
        if rd.get_pool_factory_module() is None or rd.get_pool_factory_class() is None:
            factory = ResourcePoolFactory()
        else:
            try:
                factory = ReflectionUtils.create_instance_with_params(
                    module_name=rd.get_pool_factory_module(), class_name=rd.get_pool_factory_class())(self.neo4j_config)
            except Exception as e:
                raise PoolCreatorException("Could not instantiate class= {}.{} {}".format(
                    rd.get_pool_factory_module(), rd.get_pool_factory_class(), e))

        factory.set_substrate(substrate=self.substrate)
        factory.set_descriptor(descriptor=rd)
        return factory

    def process(self):
        """
        Create Pools
        """
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.container = GlobalsSingleton.get().get_container()
        for pool in self.pools.values():
            self.logger.debug("Creating resource pool {} of Actor {}".format(
                pool.get_resource_type_label(), self.substrate.get_actor().get_name()))

            factory = self.get_factory(rd=pool)
            pool = factory.get_descriptor()
            rd = ResourceData()
            rd.resource_properties = pool.save(properties=rd.resource_properties, prefix=None)

            rd.local_properties = ResourceData.merge_properties(from_props=pool.pool_properties,
                                                                to_props=rd.local_properties)

            create_pool_result = self.substrate.get_pool_manager().create_pool(slice_id=ID(),
                                                                               name=pool.get_resource_type_label(),
                                                                               rtype=pool.get_resource_type(),
                                                                               resource_data=rd)

            if create_pool_result.code != PoolManagerError.ErrorNone:
                raise PoolCreatorException("Could not create resource pool: {}. error={}".format(
                    pool.get_resource_type_label(), create_pool_result.code))

            self.register_handler(pool=pool)
            source = factory.create_source_reservation(slice_obj=create_pool_result.slice)

            try:
                self.logger.debug("Adding source reservation to database {}".format(source))
                self.logger.debug("Source reservation has resources of type {}"
                                  .format(source.get_resources().get_resources().__class__.__name__))
                self.logger.debug("Source reservation has delegation of type {}".format(
                    source.get_resources().get_resources().get_ticket().get_delegation().__class__.__name__))

                self.substrate.get_database().add_reservation(reservation=source)
            except Exception as e:
                raise PoolCreatorException("Could not add source reservation to database {}".format(e))

    def process_neo4j(self, substrate_file: str, actor_name: str) -> Dict:
        """
        Create Pools for Neo4j
        """
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.container = GlobalsSingleton.get().get_container()
        factory = self.get_factory2()

        rd = ResourceData()
        create_pool_result = self.substrate.get_pool_manager().create_pool(
            slice_id=ID(), name=actor_name,
            rtype=ResourceType(resource_type=Constants.property_aggregate_resource_model),
            resource_data=rd)

        if create_pool_result.code != PoolManagerError.ErrorNone:
            raise PoolCreatorException("Could not create resource pool: {}. error={}".format(
                actor_name, create_pool_result.code))

        self.logger.debug("Created aggregate manager resource slice# {}".format(create_pool_result.slice))

        arm_graph = None
        if create_pool_result.slice.get_graph_id() is not None:
            # load the graph from Neo4j database
            self.logger.debug("Reloading an existing graph for resource slice# {}".format(create_pool_result.slice))
            arm_graph = factory.get_arm_graph(graph_id=create_pool_result.slice.get_graph_id())
            create_pool_result.slice.set_graph(graph=arm_graph)
        else:
            arm_graph = factory.get_arm_graph_from_file(filename=substrate_file)
            create_pool_result.slice.set_graph(graph=arm_graph)
            self.substrate.get_pool_manager().update_pool(slice_obj=create_pool_result.slice)
            self.logger.debug("Created new graph for resource slice# {}".format(create_pool_result.slice))

        for pool in self.pools.values():
            self.logger.debug("Creating resource pool {} of Actor {}".format(
                pool.get_resource_type_label(), self.substrate.get_actor().get_name()))

            factory.set_descriptor(descriptor=pool)
            pool = factory.get_descriptor()

            self.register_handler(pool=pool)

        return arm_graph.generate_adms()

    def register_handler(self, *, pool: ResourcePoolDescriptor):
        """
        Register Handlers
        @param pool Resource pool descriptor
        """
        handler_module = pool.get_handler_module()
        handler_class = pool.get_handler_class()

        if handler_class is None or handler_module is None:
            return

        config = self.substrate.get_config()
        config_map = ConfigurationMapping()
        config_map.set_key(key=str(pool.get_resource_type()))
        config_map.set_class_name(class_name=handler_class)
        config_map.set_module_name(module_name=handler_module)
        config_map.set_properties(properties=pool.get_handler_properties())

        config.add_config_mapping(mapping=config_map)


class PoolCreatorException(Exception):
    """
    Pool Creator Exception
    """
