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
from typing import Dict

from fabric_cf.actor.boot.configuration import ActorConfig
from fabric_cf.actor.core.plugins.handlers.configuration_mapping import ConfigurationMapping
from fabric_cf.actor.core.util.resource_type import ResourceType


class ResourceConfig:
    def __init__(self):
        self.handler_module = None
        self.handler_class = None
        self.handler_properties = {}
        self.resource_type = None
        self.resource_label = None
        self.description = None

    def __str__(self):
        return f"handler_module:{self.handler_module} handler_class:{self.handler_class} " \
               f"handler_properties:{self.handler_properties} " \
               f"resource_type:{self.resource_type} resource_label:{self.resource_label} description:{self.description}"

    def get_resource_type(self) -> ResourceType:
        """
        Get resource type
        @return resource type
        """
        return self.resource_type

    def set_resource_type(self, *, rtype: ResourceType):
        """
        Set resource type
        @param rtype resource type
        """
        self.resource_type = rtype

    def get_handler_module(self) -> str:
        """
        Get Handler Module
        @return handler module
        """
        return self.handler_module

    def set_handler_module(self, *, module: str):
        """
        Set Handler module
        @param module Handler module
        """
        self.handler_module = module

    def set_handler_class(self, *, handler_class: str):
        """
        Set Handler class
        @param handler_class Handler class
        """
        self.handler_class = handler_class

    def get_handler_class(self) -> str:
        """
        Get Handler class
        @return handler class
        """
        return self.handler_class

    def get_handler_properties(self) -> dict:
        """
        Get Handler Properties
        @return handler Properties
        """
        return self.handler_properties

    def set_handler_properties(self, *, properties: dict):
        """
        Set handler Properties
        @param properties handler Properties
        """
        self.handler_properties = properties

    def get_resource_type_label(self) -> str:
        """
        Get resource type label
        @return resource type label
        """
        return self.resource_label

    def set_resource_type_label(self, *, rtype_label: str):
        """
        Set resource type label
        @param rtype_label resource type label
        """
        self.resource_label = rtype_label

    def get_description(self) -> str:
        """
        Get Description
        @return description
        """
        return self.description

    def set_description(self, *, description: str):
        """
        Set description
        @param description description
        """
        self.description = description


class ResourceConfigBuilder:
    @staticmethod
    def build_resource_config(*, config: ActorConfig) -> Dict[ResourceType, ResourceConfig]:
        """
        Read resource config and create ARM and inventory slices
        @param config actor config
        @raises ConfigurationException in case of error
        """
        result = {}
        resources = config.get_resources()
        if resources is None or len(resources) == 0:
            return result

        for r in resources:
            for resource_type in r.get_type():
                descriptor = ResourceConfig()
                descriptor.set_resource_type(rtype=ResourceType(resource_type=resource_type))
                descriptor.set_resource_type_label(rtype_label=r.get_label())

                handler = r.get_handler()
                if handler is not None:
                    descriptor.set_handler_class(handler_class=handler.get_class_name())
                    descriptor.set_handler_module(module=handler.get_module_name())
                    descriptor.set_handler_properties(properties=handler.get_properties())

                result[descriptor.get_resource_type()] = descriptor
        return result

    @staticmethod
    def build_config_mapping(*, resource_config: ResourceConfig) -> ConfigurationMapping or None:
        """
        Register Handlers for each Resource Type and Save it Plugin
        @param resource_config Resource Config
        """
        handler_module = resource_config.get_handler_module()
        handler_class = resource_config.get_handler_class()

        if handler_class is None or handler_module is None:
            return None

        config_map = ConfigurationMapping()
        config_map.set_key(key=str(resource_config.get_resource_type()))
        config_map.set_class_name(class_name=handler_class)
        config_map.set_module_name(module_name=handler_module)
        config_map.set_properties(properties=resource_config.get_handler_properties())

        return config_map
