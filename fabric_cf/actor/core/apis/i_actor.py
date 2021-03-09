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
from enum import Enum
from typing import TYPE_CHECKING

from fabric_cf.actor.core.apis.i_actor_runnable import IActorRunnable
from fabric_cf.actor.core.apis.i_timer_queue import ITimerQueue
from fabric_cf.actor.core.apis.i_actor_identity import IActorIdentity
from fabric_cf.actor.core.apis.i_slice_operations import ISliceOperations
from fabric_cf.actor.core.apis.i_reservation_operations import IReservationOperations
from fabric_cf.actor.core.apis.i_tick import ITick

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_actor_event import IActorEvent
    from fabric_cf.actor.core.apis.i_actor_proxy import IActorProxy
    from fabric_cf.actor.core.apis.i_base_plugin import IBasePlugin
    from fabric_cf.actor.core.apis.i_query_response_handler import IQueryResponseHandler
    from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
    from fabric_cf.actor.core.time.actor_clock import ActorClock
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.apis.i_policy import IPolicy


class ActorType(Enum):
    """
    Enum for Actor Type
    """
    All = 0
    Orchestrator = 1
    Broker = 2
    Authority = 3

    @staticmethod
    def get_actor_type_from_string(*, actor_type: str) -> ActorType:
        """
        Convert String to Actor Type
        @param actor_type actor type
        """
        if actor_type.lower() == ActorType.Orchestrator.name.lower():
            return ActorType.Orchestrator
        if actor_type.lower() == ActorType.Broker.name.lower():
            return ActorType.Broker
        if actor_type.lower() == ActorType.Authority.name.lower():
            return ActorType.Authority
        return ActorType.All


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

    @abstractmethod
    def actor_added(self):
        """
        Informs the actor that it has been integrated in the container. This
        method should finish the initialization of the actor: some initialization
        steps may not be able to execute until the actor is part of the running
        container.

        Raises:
            Exception: if a critical error occurs while processing the event
        """

    @abstractmethod
    def actor_removed(self):
        """
        Informs the actor that it has been removed. This method should finish the
        shutdown/cleanup of the actor.
        """

    @abstractmethod
    def get_actor_clock(self) -> ActorClock:
        """
        Returns the actor clock used by the actor.

        Returns:
            actor clock
        """

    @abstractmethod
    def get_current_cycle(self) -> int:
        """
        Returns the cycle this actor is processing.

        Returns:
            current clock cycle
        """

    @abstractmethod
    def get_description(self) -> str:
        """
        Returns the description for the actor.

        Returns:
            description for the actor.
        """

    @abstractmethod
    def get_policy(self) -> IPolicy:
        """
        Returns the policy used by the actor.

        Returns:
            policy used by the actor.
        """

    @abstractmethod
    def get_plugin(self) -> IBasePlugin:
        """
        Returns the plugin used by the actor.

        Returns:
            plugin used by the actor.
        """

    @abstractmethod
    def get_type(self) -> ActorType:
        """
        Returns the type of the actor.

        Returns:
            type of the actor.
        """

    @abstractmethod
    def initialize(self):
        """
        Initializes the actor.

        Raises:
            Exception: if a critical error occurs while initialization
        """

    @abstractmethod
    def is_recovered(self) -> bool:
        """
        Checks if the actor has completed recovery.

        Returns:
            true if this actor has completed recovery
        """

    @abstractmethod
    def is_stopped(self) -> bool:
        """
        Checks if the actor has completed stopped.

        Returns:
            true if this actor has been stopped
        """

    @abstractmethod
    def recover(self):
        """
        Recovers the actor from saved state.

        Raises:
            Exception: if an error occurs during recovery
        """

    @abstractmethod
    def set_actor_clock(self, *, clock: ActorClock):
        """
        Sets the actor clock to be used by the actor.

        Args:
            clock: actor clock
        """

    @abstractmethod
    def set_description(self, *, description: str):
        """
        Sets the description for the actor.

        Args:
            description: actor description
        """

    @abstractmethod
    def set_identity(self, *, token: AuthToken):
        """
        Sets the identity of this actor. Must be called before initialize.

        Args:
            token: actor's identity token
        """

    @abstractmethod
    def set_policy(self, *, policy: IPolicy):
        """
        Sets the policy of this actor. Must be called before initialize.

        Args:
            policy: policy implementation to use
        """

    @abstractmethod
    def set_recovered(self, *, value: bool):
        """
        Sets the recovered flag.

        Args:
            value: flag value
        """

    @abstractmethod
    def set_plugin(self, *, plugin: IBasePlugin):
        """
        Sets the plugin of this actor. Must be called before initialize.

        Args:
            plugin: plugin to use.
        """

    @abstractmethod
    def start(self):
        """
        Performs all required actions when starting an actor.
        """

    @abstractmethod
    def stop(self):
        """
        Performs all required actions when stopping an actor.
        """

    @abstractmethod
    def queue_event(self, *, incoming: IActorEvent):
        """
        Adds an event.

        Args:
            incoming: incoming event
        """

    @abstractmethod
    def query(self, *, query: dict = None, caller: AuthToken = None,
              actor_proxy: IActorProxy = None, handler: IQueryResponseHandler = None,
              id_token: str = None):
        """
        Processes a query request from the specified caller.

        Args:
            query: query
            caller: caller
            actor_proxy: actor proxy
            handler: handler
            id_token: id_token

        Returns:
            query response
        """

    @abstractmethod
    def execute_on_actor_thread_and_wait(self, *, runnable: IActorRunnable):
        """
        Execute on Actor Thread and Wait until response is processed
        @params runnable: reservation to be processed
        """

    @abstractmethod
    def await_no_pending_reservations(self):
        """
        Await for pending reservations
        """

    @abstractmethod
    def get_logger(self):
        """
        Return the logger
        """

    @abstractmethod
    def handle_failed_rpc(self, *, rid: ID, rpc: FailedRPC):
        """
        Handle a failed rpc
        @params rid: reservation id
        @params rpc: failed rpc
        """

    @staticmethod
    def get_management_object_class() -> str:
        """
        Get Management Object class Name
        """

    @staticmethod
    def get_management_object_module() -> str:
        """
        Get Management Object class Module name
        """

    @staticmethod
    def get_kafka_service_class() -> str:
        """
        Get Kafka Service Class Name
        """

    @staticmethod
    def get_kafka_service_module() -> str:
        """
        Get Kafka Service Class Module Name
        """

    @staticmethod
    def get_mgmt_kafka_service_class() -> str:
        """
        Get Management Kafka Service Class Name
        """

    @staticmethod
    def get_mgmt_kafka_service_module() -> str:
        """
        Get Management Kafka Service Class Module Name
        """

    @abstractmethod
    def set_logger(self, logger):
        """
        Set logger
        @param logger logger
        """

    @abstractmethod
    def load_model(self, *, graph_id: str):
        """
        Load any Graph Model
        :param graph_id:
        :return:
        """
