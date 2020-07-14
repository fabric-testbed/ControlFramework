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


if TYPE_CHECKING:
    from fabric.actor.core.apis.IActorEvent import IActorEvent
    from fabric.actor.core.apis.IActorProxy import IActorProxy
    from fabric.actor.core.apis.IBasePlugin import IBasePlugin
    from fabric.actor.core.apis.IQueryResponseHandler import IQueryResponseHandler
    from fabric.actor.core.kernel.FailedRPC import FailedRPC
    from fabric.actor.core.time.ActorClock import ActorClock
    from fabric.actor.core.util.ID import ID
    from fabric.actor.security.AuthToken import AuthToken

from fabric.actor.core.apis.ITimerQueue import ITimerQueue
from fabric.actor.core.apis.IActorIdentity import IActorIdentity
from fabric.actor.core.apis.ISliceOperations import ISliceOperations
from fabric.actor.core.apis.IReservationOperations import IReservationOperations
from fabric.actor.core.apis.ITick import ITick


class IActor(IActorIdentity, ISliceOperations, IReservationOperations, ITick, ITimerQueue):
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
    PropertyGuid = "ActorGuid"
    PropertyName = "ActorName"
    PropertyType = "ActorType"

    def actor_added(self):
        """
        Informs the actor that it has been integrated in the container. This
        method should finish the initialization of the actor: some initialization
        steps may not be able to execute until the actor is part of the running
        container.

        Raises:
            Exception: if a critical error occurs while processing the event
        """
        raise NotImplementedError("Should have implemented this")

    def actor_removed(self):
        """
        Informs the actor that it has been removed. This method should finish the
        shutdown/cleanup of the actor.
        """
        raise NotImplementedError("Should have implemented this")

    def get_actor_clock(self) -> ActorClock:
        """
        Returns the actor clock used by the actor.

        Returns:
            actor clock
        """
        raise NotImplementedError("Should have implemented this")

    def get_current_cycle(self) -> int:
        """
        Returns the cycle this actor is processing.

        Returns:
            current clock cycle
        """
        raise NotImplementedError("Should have implemented this")

    def get_description(self) -> str:
        """
        Returns the description for the actor.

        Returns:
            description for the actor.
        """
        raise NotImplementedError("Should have implemented this")

    def get_policy(self):
        """
        Returns the policy used by the actor.

        Returns:
            policy used by the actor.
        """
        raise NotImplementedError("Should have implemented this")

    def get_plugin(self):
        """
        Returns the plugin used by the actor.

        Returns:
            plugin used by the actor.
        """
        raise NotImplementedError("Should have implemented this")

    def get_type(self):
        """
        Returns the type of the actor.

        Returns:
            type of the actor.
        """
        raise NotImplementedError("Should have implemented this")

    def initialize(self):
        """
        Initializes the actor.

        Raises:
            Exception: if a critical error occurs while initialization
        """
        raise NotImplementedError("Should have implemented this")

    def is_recovered(self):
        """
        Checks if the actor has completed recovery.

        Returns:
            true if this actor has completed recovery
        """
        raise NotImplementedError("Should have implemented this")

    def is_stopped(self):
        """
        Checks if the actor has completed stopped.

        Returns:
            true if this actor has been stopped
        """
        raise NotImplementedError("Should have implemented this")

    def recover(self):
        """
        Recovers the actor from saved state.

        Raises:
            Exception: if an error occurs during recovery
        """
        raise NotImplementedError("Should have implemented this")

    def set_actor_clock(self, clock):
        """
        Sets the actor clock to be used by the actor.

        Args:
            clock: actor clock
        """
        raise NotImplementedError("Should have implemented this")

    def set_description(self, description: str):
        """
        Sets the description for the actor.

        Args:
            description: actor description
        """
        raise NotImplementedError("Should have implemented this")

    def set_identity(self, token: AuthToken):
        """
        Sets the identity of this actor. Must be called before initialize.

        Args:
            token: actor's identity token
        """
        raise NotImplementedError("Should have implemented this")

    def set_policy(self, policy):
        """
        Sets the policy of this actor. Must be called before initialize.

        Args:
            policy: policy implementation to use
        """
        raise NotImplementedError("Should have implemented this")

    def set_recovered(self, value: bool):
        """
        Sets the recovered flag.

        Args:
            value: flag value
        """
        raise NotImplementedError("Should have implemented this")

    def set_plugin(self, plugin: IBasePlugin):
        """
        Sets the plugin of this actor. Must be called before initialize.

        Args:
            plugin: plugin to use.
        """
        raise NotImplementedError("Should have implemented this")

    def start(self):
        """
        Performs all required actions when starting an actor.
        """
        raise NotImplementedError("Should have implemented this")

    def stop(self):
        """
        Performs all required actions when stopping an actor.
        """
        raise NotImplementedError("Should have implemented this")

    def queue_event(self, incoming: IActorEvent):
        """
        Adds an event.

        Args:
            incoming: incoming event
        """
        raise NotImplementedError("Should have implemented this")

    def query(self, query: dict = None, caller: AuthToken= None,
              actor_proxy: IActorProxy= None, handler: IQueryResponseHandler= None):
        """
        Processes a query request from the specified caller.

        Args:
            query: query
            caller: caller
            actor_proxy: actor proxy
            handler: handler

        Returns:
            query response
        """
        raise NotImplementedError("Should have implemented this")

    def execute_on_actor_thread_and_wait(self, r):
        raise NotImplementedError("Should have implemented this")

    def await_no_pending_reservations(self):
        raise NotImplementedError("Should have implemented this")

    def get_logger(self):
        """
        Return the logger
        """
        raise NotImplementedError("Should have implemented this")

    def handle_failed_rpc(self, rid: ID, rpc: FailedRPC):
        raise NotImplementedError("Should have implemented this")

    @staticmethod
    def get_management_object_class() -> str:
        raise NotImplementedError("Should have implemented this")

    @staticmethod
    def get_management_object_module() -> str:
        raise NotImplementedError("Should have implemented this")

    @staticmethod
    def get_kafka_service_class() -> str:
        raise NotImplementedError("Should have implemented this")

    @staticmethod
    def get_kafka_service_module() -> str:
        raise NotImplementedError("Should have implemented this")

    @staticmethod
    def get_mgmt_kafka_service_class() -> str:
        raise NotImplementedError("Should have implemented this")

    @staticmethod
    def get_mgmt_kafka_service_module() -> str:
        raise NotImplementedError("Should have implemented this")