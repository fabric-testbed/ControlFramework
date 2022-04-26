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

from fim.graph.resources.neo4j_arm import Neo4jARMGraph

from fabric_cf.actor.fim.fim_helper import FimHelper
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.inventory_slice_manager import InventorySliceManagerError
from fabric_cf.actor.core.plugins.handlers.configuration_mapping import ConfigurationMapping
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType

if TYPE_CHECKING:
    from fabric_cf.actor.core.common.resource_config import ResourceConfig
    from fabric_cf.actor.core.plugins.substrate.authority_substrate import AuthoritySubstrate


class AggregateResourceModelCreator:
    """
    Responsible for setting up inventory slices on startup
    """
    def __init__(self, *, substrate: AuthoritySubstrate = None, resources: dict = None, neo4j_config: dict = None):
        self.substrate = substrate
        self.resources = resources
        self.neo4j_config = neo4j_config
        self.container = None
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.arm_graph = None

    def process_neo4j(self, substrate_file: str, actor_name: str) -> Dict:
        """
        Create ARM and Inventory Slices
        """
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.container = GlobalsSingleton.get().get_container()

        result = self.substrate.get_inventory_slice_manager().create_inventory_slice(
            slice_id=ID(), name=actor_name,
            rtype=ResourceType(resource_type=Constants.PROPERTY_AGGREGATE_RESOURCE_MODEL))

        if result.code != InventorySliceManagerError.ErrorNone:
            raise AggregateResourceModelCreatorException(f"Could not create ARM: {actor_name}. error={result.code}")

        self.logger.debug(f"Created aggregate manager resource slice# {result.slice}")

        if result.slice.get_graph_id() is not None:
            # load the graph from Neo4j database
            self.logger.debug(f"Reloading an existing graph for resource slice# {result.slice}")
            self.arm_graph = FimHelper.get_arm_graph(graph_id=result.slice.get_graph_id())
            result.slice.set_graph(graph=self.arm_graph)
        else:
            self.arm_graph = FimHelper.get_arm_graph_from_file(filename=substrate_file)
            result.slice.set_graph(graph=self.arm_graph)
            self.substrate.get_inventory_slice_manager().update_inventory_slice(slice_obj=result.slice)
            self.logger.debug(f"Created new graph for resource slice# {result.slice}")

        for r in self.resources.values():
            self.logger.debug(f"Registering resource_handler for resource_type: {r.get_resource_type_label()} "
                              f"for Actor {actor_name}")
            self.register_handler(resource_config=r)

        return self.arm_graph.generate_adms()

    def register_handler(self, *, resource_config: ResourceConfig):
        """
        Register Handlers for each Resource Type and Save it Plugin
        @param resource_config Resource Config
        """
        handler_module = resource_config.get_handler_module()
        handler_class = resource_config.get_handler_class()

        if handler_class is None or handler_module is None:
            return

        config_map = ConfigurationMapping()
        config_map.set_key(key=str(resource_config.get_resource_type()))
        config_map.set_class_name(class_name=handler_class)
        config_map.set_module_name(module_name=handler_module)
        config_map.set_properties(properties=resource_config.get_handler_properties())

        self.substrate.handler_processor.add_config_mapping(mapping=config_map)
        print(f"Added handler for  {resource_config.get_resource_type()} config_map: {config_map}")

    def get_arm_graph(self) -> Neo4jARMGraph:
        return self.arm_graph


class AggregateResourceModelCreatorException(Exception):
    """
    ARM Creator Exception
    """
