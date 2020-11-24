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

from fabric.actor.core.extensions.plugin import Plugin

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_container_database import IContainerDatabase


class PluginManager:
    """
    The PluginManager is responsible for registering/unregistering
    plugin descriptors. The PluginManager registers each installed
    plugin with the backend database.
    """
    def __init__(self):
        from fabric.actor.core.container.globals import GlobalsSingleton
        self.db = None
        self.logger = GlobalsSingleton.get().get_logger()
        self.initialized = False

    def initialize(self, *, db: IContainerDatabase):
        """
        Initializes the plugin manager. Called by the management layer.
        @param db ip database
        @throws Exception in case of error
        """
        if db is None:
            raise Exception("database cannot be null")
        if not self.initialized:
            self.db = db
            self.initialized = True

    def is_registered(self, *, plugin_id: str):
        """
        Checks if the specified plugin has been already registered.
        @param plugin_id plugin id
        @return true | false
        @throws Exception in case of error
        """
        try:
            if self.db.get_plugin(plugin_id) is not None:
                return True
        except Exception as e:
            self.logger.error(e)
        return False

    def register(self, *, plugin: Plugin):
        """
        Registers this plugin. Throws an exception if an instance of this plugin
        has already been registered. Registration involves a database access.
        @param plugin plugin instance
        @throws Exception in case of error
        """
        if self.is_registered(plugin_id=plugin.get_id()):
            raise Exception("A plugin with this id/package id already exists")
        else:
            self.db.add_plugin(plugin=plugin)

    def unregister(self, *, plugin_id: str):
        """
        Unregisters this plugin.
        @param plugin_id plugin identifier
        @throws Exception in case of error
        """
        self.db.remove_plugin(plugin_id=plugin_id)









