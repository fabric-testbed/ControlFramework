#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from plugins.apis import IActorPlugin, IActorEvent, IActorProxy, IQueryResponseHandler
from plugins.apis.IActorIdentity import IActorIdentity
from plugins.apis.ISliceOperations import ISliceOperations
from plugins.apis.IReservationOperations import IReservationOperations
from security import AuthToken


class Actor(IActorIdentity, ISliceOperations, IReservationOperations):
    """
     IActor defines the common functionality of all actors. An actor
     offers a collection of management operations for slices and reservations and
     implements the public methods necessary to serve calls from other actors,
     e.g., requests for tickets and leases.

     Every actor has a globally unique identifier and a name. The current
     implementation assumes that names are globally unique. In addition, each
     actor can have an optional description (used for display purposes, e.g., the
     web portal).

     The actions of each actor, e.g, how to request new resources, how to
     arbitrate among multiple ticket requests, etc, are driven by policy modules.

     There are three types of actors:
         Orchestrator -
         Broker - arbiter among requests for resources. Brokers determine who gets what and for how long.
         Aggregate Manager - owner of resources.

         Each of the aforementioned roles is defined in a corresponding interface. An
         actor instance must implement at least one of these interfaces.
    """

    def actor_added(self):
        """
        Informs the actor that it has been integrated in the container. This
        method should finish the initialization of the actor: some initialization
        steps may not be able to execute until the actor is part of the running
        container.

        Raises:
            Exception: if a critical error occurs while processing the event
        """
        return

    def actor_removed(self):
        """
        Informs the actor that it has been removed. This method should finish the
        shutdown/cleanup of the actor.
        """
        return

    def get_actor_clock(self):
        """
        Returns the actor clock used by the actor.

        Returns:
            actor clock
        """
        return

    def get_current_cycle(self):
        """
        Returns the cycle this actor is processing.

        Returns:
            current clock cycle
        """
        return 0

    def get_description(self):
        """
        Returns the description for the actor.

        Returns:
            description for the actor.
        """
        return None

    def get_policy(self):
        """
        Returns the policy used by the actor.

        Returns:
            policy used by the actor.
        """
        return None

    def get_plugin(self):
        """
        Returns the plugin used by the actor.

        Returns:
            plugin used by the actor.
        """
        return None

    def get_type(self):
        """
        Returns the type of the actor.

        Returns:
            type of the actor.
        """
        return None

    def initialize(self):
        """
        Initializes the actor.

        Raises:
            Exception: if a critical error occurs while initialization
        """
        return

    def is_recovered(self):
        """
        Checks if the actor has completed recovery.

        Returns:
            true if this actor has completed recovery
        """
        return False

    def is_stopped(self):
        """
        Checks if the actor has completed stopped.

        Returns:
            true if this actor has been stopped
        """
        return False

    def recover(self):
        """
        Recovers the actor from saved state.

        Raises:
            Exception: if an error occurs during recovery
        """
        return

    def set_actor_clock(self, clock):
        """
        Sets the actor clock to be used by the actor.

        Args:
            clock: actor clock
        """
        return

    def set_description(self, description: str):
        """
        Sets the description for the actor.

        Args:
            description: actor description
        """
        return

    def set_identity(self, token: AuthToken):
        """
        Sets the identity of this actor. Must be called before initialize.

        Args:
            token: actor's identity token
        """
        return

    def set_policy(self, policy):
        """
        Sets the policy of this actor. Must be called before initialize.

        Args:
            policy: policy implementation to use
        """
        return

    def set_recovered(self, value: bool):
        """
        Sets the recovered flag.

        Args:
            value: flag value
        """
        return

    def set_plugin(self, plugin: IActorPlugin):
        """
        Sets the plugin of this actor. Must be called before initialize.

        Args:
            plugin: plugin to use.
        """
        return

    def start(self):
        """
        Performs all required actions when starting an actor.
        """
        return

    def stop(self):
        """
        Performs all required actions when stopping an actor.
        """
        return

    def queueEvent(self, incoming: IActorEvent):
        """
        Adds an event.

        Args:
            incoming: incoming event
        """
        return

    def query(self, actor_proxy: IActorProxy, query, handler: IQueryResponseHandler):
        """
        Issues a query request to the specified actor. The call is non-blocking.
        When the response from the remote actor is received, handler is invoked.

        Args:
            actor_proxy: actor proxy
            query: query
            handler: handler
        """
        return

    def query(self, query, caller:AuthToken):
        """
        Processes a query request from the specified caller.

        Args:
            query: query
            caller: caller

        Returns:
            query response
        """
        return None

    def executeOnActorThreadAndWait(self, r):
        return None

    def awaitNoPendingReservations(self):
        return

