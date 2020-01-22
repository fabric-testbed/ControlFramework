#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from yapsy.IPlugin import IPlugin
from plugins.apis import IActor, IReservation, ISlice
from security import AuthToken


class IActorPlugin(IPlugin):
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
        super(IActorPlugin, self).__init__()

    def activate(self):
        """
        Activate the plugin
        """
        # Make sure to call `activate()` on the parent class to ensure that the
        # `is_activated` property gets set.
        super(IActorPlugin, self).activate()

    def deactivate(self):
        """
        Deactivate the plugin
        """
        # Make sure to call `deactivate()` on the parent class to ensure that
        # the `is_activated` property gets set.
        super(IActorPlugin, self).deactivate()

    def configure(self, properties):
        """
        Processes a list of configuration properties. This method is called by the configuration engine.

        Args:
            properties: properties

        Raises:
            Exception in case of error
        """
        print("Configure the IActorPlugin:plugin")

    def actor_added(self):
        """
        Performs initialization steps that require that the actor has been added

        Raises:
            Exception in case of error
        """
        print("actor has been added")

    def recovery_starting(self):
        """
        Informs the plugin that recovery is about to start.
        """
        print("recovery Starting")

    def initialize(self, actor: IActor):
        """
        Initializes the actor. Called early in the initialization process.

        Args:
            actor : the actor object

        Raises:
            Exception in case of error
        """
        return

    def revisit(self, reservation: IReservation):
        """
        Rebuilds plugin state associated with a restored reservation. Called once for each restored reservation.

        Args:
            reservation: restored reservation
        Raises:
            Exception if rebuilding state fails
        """
        print ("revisit Starting")

    def revisit(self, slice: ISlice):
        """
        Rebuilds plugin state associated with a restored slice. Called once for each restored slice.

        Args:
            slice: restored slice
        Raises:
            Exception if rebuilding state fails
        """
        print ("revisit Starting")

    def recovery_ended(self):
        """
        Informs the plugin that recovery has completed.
        """
        return

    def restart_configuration_actions(self, reservation:IReservation):
        """
        Restarts any pending configuration actions for the specified reservation

        Args:
            reservation: reservation
        Raises:
            Exception if restarting actions fails
        """
        return

    def create_slice(self, slice_id: str, name: str, request):
        """
        Creates a new slice.

        Args:
            slice_id: guid for the slice
            name: slice name
            request: properties for the slice
        Returns:
            a slice object
        Raises:
            Exception in case of error
        """
        return("IActorPlugin: creating slice")

    def release_slice(self, slice: ISlice):
        """
        Releases any resources held by the slice.

        Args:
            slice: slice
        Raises:
            Exception in case of error
        """
        print("creating slice")

    def validate_incoming(self, reservation: IReservation, auth: AuthToken):
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
        return False

    def set_actor(self, actor: IActor):
        """
        Sets the actor. Note: the actor has to be fully initialized.

        Args:
            actor: actor
        """
        return

    def get_actor(self):
        """
        Returns the actor associated with the plugin

        Returns:
            actor associated with the plugin
        """
        return None

    def set_ticket_factory(self, ticket_factory):
        """
        Sets the ticket factory

        Args:
            ticket_factory: ticket factory
        """
        return

    def get_ticket_factory(self):
        """
        Returns the ticket factory.

        Returns:
             ticket factory
        """
        return None
