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

from abc import abstractmethod
from typing import TYPE_CHECKING

from fabric_cf.actor.core.apis.abc_container_clock import ABCContainerClock

if TYPE_CHECKING:
    from fabric_cf.actor.boot.configuration import Configuration
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.container.protocol_descriptor import ProtocolDescriptor
    from fabric_cf.actor.core.manage.management_object_manager import ManagementObjectManager
    from fabric_cf.actor.core.util.id import ID


class ABCActorContainer(ABCContainerClock):
    """
    IActorContainer is the public interface for container
    """
    @abstractmethod
    def initialize(self, *, config: Configuration):
        """
        Initializes the container manager.
        @param config container configuration
        @throws ContainerInitializationException if the configuration is invalid
        """

    @abstractmethod
    def get_guid(self) -> ID:
        """
        Returns the container GUID.
        @return container GUID
        """

    @abstractmethod
    def get_config(self) -> Configuration:
        """
        Return the container configuration
        @return container configuration
        """

    @abstractmethod
    def get_database(self):
        """
        Returns the container database.
        @return container database
        """

    @abstractmethod
    def is_recovered(self):
        """
        Checks if the container has completed recovery.
        @return TRUE if recovery is complete, FALSE otherwise
        """

    @abstractmethod
    def shutdown(self):
        """
        Shuts down the container.
        """

    @abstractmethod
    def register_actor(self, *, actor: ABCActorMixin):
        """
        Registers a new actor: adds the actor to the database, deploys services
        required by the actor, registers actor proxies and callbacks. Must not
        register the actor with the clock! Clock registration is a separate
        phase.
        @param actor actor to register
        @throws Exception in case of error
        """

    @abstractmethod
    def register_protocol(self, *, protocol: ProtocolDescriptor):
        """
        Registers a communication protocol with the container. This protocol
        applies only for internal communication among actors; this is not a
        protocol used for managing the container.
        @param protocol protocol to register
        """

    @abstractmethod
    def get_management_object_manager(self) -> ManagementObjectManager:
        """
        Returns Management Object Manager
        @returns ManagementObjectManager
        """

    @abstractmethod
    def get_protocol_descriptor(self, *, protocol: str) -> ProtocolDescriptor:
        """
        Return Protocol Descriptor for a specific Protocol
        @returns ProtocolDescriptor
        """

    @abstractmethod
    def is_fresh(self) -> bool:
        """
        Returns true if container was started fresh; false otherwise
        @returns bool
        """

    @abstractmethod
    def recover_actor(self, *, properties: dict):
        """
        Recover an actor
        @params properties: properties
        """

    @abstractmethod
    def unregister_actor(self, *, actor: ABCActorMixin):
        """
        Un-register an actor
        @params actor: actor
        """

    @abstractmethod
    def remove_actor(self, *, actor_name: str):
        """
        Remove an actor
        @params actor_name: actor_name
        """

    @abstractmethod
    def remove_actor_database(self, *, actor_name: str):
        """
        Remove Actor Database
        @params actor_name: actor name
        """
