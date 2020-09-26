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

from fabric.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_actor import IActor
    from fabric.actor.core.apis.i_database import IDatabase
    from fabric.actor.core.apis.i_resource_ticket_factory import IResourceTicketFactory
    from fabric.actor.core.util.resource_data import ResourceData
    from fabric.actor.security.auth_token import AuthToken
    from fabric.actor.core.apis.i_reservation import IReservation
    from fabric.actor.core.apis.i_slice import ISlice

from yapsy.IPlugin import IPlugin


class IBasePlugin(IPlugin):
    """
    IActorPlugin defines the interface for linking/injecting functionality to the leasing code.
    This interface can be used to link other systems to the core, for example to add support for leasing
    to a cluster management system.

    These methods are called as various events occur. All implementations of this class must have a
    constructor that takes no arguments, and set methods for their attributes.
    """
    def __init__(self):
        # Make sure to call the parent class (`IPlugin`) methods when
        # overriding them.
        super(IBasePlugin, self).__init__()

    def activate(self):
        """
        Activate the plugin
        """
        # Make sure to call `activate()` on the parent class to ensure that the
        # `is_activated` property gets set.
        super(IBasePlugin, self).activate()

    def deactivate(self):
        """
        Deactivate the plugin
        """
        # Make sure to call `deactivate()` on the parent class to ensure that
        # the `is_activated` property gets set.
        super(IBasePlugin, self).deactivate()

    @abstractmethod
    def configure(self, *, properties):
        """
        Processes a list of configuration properties. This method is called by the configuration engine.

        Args:
            properties: properties

        Raises:
            Exception in case of error
        """

    @abstractmethod
    def actor_added(self):
        """
        Performs initialization steps that require that the actor has been added

        Raises:
            Exception in case of error
        """

    @abstractmethod
    def recovery_starting(self):
        """
        Informs the plugin that recovery is about to start.
        """

    @abstractmethod
    def initialize(self):
        """
        Initializes the actor. Called early in the initialization process.

        Raises:
            Exception in case of error
        """

    @abstractmethod
    def revisit(self, *, slice: ISlice = None, reservation: IReservation = None):
        """
        Rebuilds plugin state associated with a restored slice/reservation. Called once for each restored slice/reservation.

        Args:
            slice: restored slice
            reservation: restored reservation
        Raises:
            Exception if rebuilding state fails
        """

    @abstractmethod
    def recovery_ended(self):
        """
        Informs the plugin that recovery has completed.
        """

    @abstractmethod
    def restart_configuration_actions(self, *, reservation: IReservation):
        """
        Restarts any pending configuration actions for the specified reservation

        Args:
            reservation: reservation
        Raises:
            Exception if restarting actions fails
        """

    @abstractmethod
    def create_slice(self, *, slice_id: ID, name: str, properties: ResourceData):
        """
        Creates a new slice.

        Args:
            slice_id: guid for the slice
            name: slice name
            properties: properties for the slice
        Returns:
            a slice object
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def release_slice(self, *, slice_obj: ISlice):
        """
        Releases any resources held by the slice.

        Args:
            slice_obj: slice
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def validate_incoming(self, *, reservation: IReservation, auth: AuthToken):
        """
        Validates an incoming reservation request

        Args:
            reservation: reservation
            auth: auth token of the caller
        Returns:
            True if the validation succeeds
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def set_actor(self, *, actor: IActor):
        """
        Sets the actor. Note: the actor has to be fully initialized.

        Args:
            actor: actor
        """

    @abstractmethod
    def get_actor(self):
        """
        Returns the actor associated with the plugin

        Returns:
            actor associated with the plugin
        """

    @abstractmethod
    def set_ticket_factory(self, *, ticket_factory):
        """
        Sets the ticket factory

        Args:
            ticket_factory: ticket factory
        """

    @abstractmethod
    def get_ticket_factory(self) -> IResourceTicketFactory:
        """
        Returns the ticket factory.

        Returns:
             ticket factory
        """

    @abstractmethod
    def get_logger(self):
        """
        Returns the logger.
        
        @returns logger instance
        """

    @abstractmethod
    def get_database(self) -> IDatabase:
        """
        Obtains the actor's database instance.
        
        @return database instance
        """
        
    @abstractmethod
    def set_database(self, *, db: IDatabase):
        """
        Sets the actor's database instance.
        
        @param db
                   database instance
        """