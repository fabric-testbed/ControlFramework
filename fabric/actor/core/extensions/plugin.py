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


class Plugin:
    """
    Plugin class describes metadatata about an plugin supplied
    """
    PluginId = 'plg_id'
    PluginLocalId = 'plg_local_id'
    PluginType = 'plg_type'
    PluginActorType = 'plg_actor_type'
    # Plugin types.
    TypeAll = 0
    # Specifies a plugin that provides a new actor implementation.
    TypeActorObject = 1
    # Specified a plugin that provides a new policy implementation.
    TypePolicy = 2
    # Specifies a plugin that provides a new manager object implementation.
    TypeManagerObject = 3
    # Specifies a plugin that provides a new portal plugin implementation.
    TypePortalPlugin = 4
    # Specifies a plugin that provides a new actor orchestrator implementation.
    TypeActorController = 5
    # Specified a new plugin that provides an new application implementation.
    TypeApplicationController = 6
    # Specified a new plugin that provides a new workload implementation.
    TypeWorkloadController = 7
    # Specified a new plugin that provides a new site control implementation.
    TypeSiteControl = 8
    # Specifies a handler support library: not a real handler
    TypeHandlerSupportLibrary = 9
    # Specifies a configuration handler
    TypeHandler = 10
    PluginPropertyHandlerFile = "handler.file"

    def __init__(self):
        self.id = None
        self.plugin_type = None
        self.factory = False
        self.name = None
        self.description = None
        self.class_name = None
        self.config_properties = None
        self.config_template = None
        self.actor_type = 0

    def is_factory(self) -> bool:
        """
        Checks if this plugin is a factory.
        @return true if the plugin represents a factory
        """
        return self.factory

    def set_factory(self, factory: bool):
        """
        Sets the factory flag. A plugin is a factory if it is used to create the
        actual plugin rather than it being the plugin itself.
        @param factory true|false
        """
        self.factory = factory
        
    def get_id(self) -> str:
        """
        Returns the plugin identifier.
        @return the id
        """
        return self.id

    def set_id(self, id: str):
        """
        @param id the id to set
        """
        self.id = id

    def get_plugin_type(self) -> int:
        """
        @return the pluginType
        """
        return self.plugin_type

    def set_plugin_type(self, plugin_type: int):
        """
        @param pluginType the pluginType to set
        """
        self.plugin_type = plugin_type

    def get_class_name(self) -> str:
        """
        @return the className
        """
        return self.class_name

    def set_class_name(self, class_name: str):
        """
        @param class_name the className to set
        """
        self.class_name = class_name

    def get_config_properties(self) -> dict:
        return self.config_properties

    def set_config_properties(self, config_properties: dict):
        self.config_properties = config_properties

    def get_config_template(self) -> str:
        return self.config_template

    def set_config_template(self, config_template: str):
        self.config_template = config_template

    def get_name(self) -> str:
        return self.name

    def set_name(self, name: str):
        self.name = name

    def get_actor_type(self) -> int:
        self.actor_type

    def set_actor_type(self, actor_type: int):
        self.actor_type = actor_type
