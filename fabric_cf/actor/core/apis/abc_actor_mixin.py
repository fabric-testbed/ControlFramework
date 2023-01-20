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
from typing import TYPE_CHECKING, List, Dict

from fabric_cf.actor.boot.configuration import ActorConfig
from fabric_cf.actor.core.apis.abc_actor_runnable import ABCActorRunnable
from fabric_cf.actor.core.apis.abc_timer_queue import ABCTimerQueue
from fabric_cf.actor.core.apis.abc_actor_identity import ABCActorIdentity

from fabric_cf.actor.core.apis.abc_tick import ABCTick
from fabric_cf.actor.core.container.maintenance import Site

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_event import ABCActorEvent
    from fabric_cf.actor.core.apis.abc_actor_proxy import ABCActorProxy
    from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin
    from fabric_cf.actor.core.apis.abc_query_response_handler import ABCQueryResponseHandler
    from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
    from fabric_cf.actor.core.time.actor_clock import ActorClock
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
    from fabric_cf.actor.core.util.reservation_set import ReservationSet
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.core.time.term import Term


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


class ABCActorMixin(ABCActorIdentity, ABCTick, ABCTimerQueue):
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
    def actor_added(self, *, config: ActorConfig):
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
    def get_policy(self) -> ABCPolicy:
        """
        Returns the policy used by the actor.

        Returns:
            policy used by the actor.
        """

    @abstractmethod
    def get_plugin(self) -> ABCBasePlugin:
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
    def initialize(self, *, config: ActorConfig):
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
    def set_policy(self, *, policy: ABCPolicy):
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
    def set_plugin(self, *, plugin: ABCBasePlugin):
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
    def queue_event(self, *, incoming: ABCActorEvent):
        """
        Adds an event.

        Args:
            incoming: incoming event
        """

    @abstractmethod
    def query(self, *, query: dict = None, caller: AuthToken = None,
              actor_proxy: ABCActorProxy = None, handler: ABCQueryResponseHandler = None):
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
    @abstractmethod
    def execute_on_actor_thread(self, *, runnable: ABCActorRunnable):
        """
        Execute on Actor Thread and Wait until response is processed
        @params runnable: reservation to be processed
        """

    @abstractmethod
    def execute_on_actor_thread_and_wait(self, *, runnable: ABCActorRunnable):
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

    @abstractmethod
    def get_client_slices(self) -> List[ABCSlice]:
        """
        Returns all client slices registered with the actor.

        Returns:
            an array of client slices
        """

    @abstractmethod
    def get_slices(self) -> List[ABCSlice]:
        """
        Returns all slices registered with the actor.

        Returns:
            an array of slices
        """

    @abstractmethod
    def get_slice(self, *, slice_id: ID) -> ABCSlice:
        """
        Returns the slice with the given id.

        Args:
            slice_id: slice id
        Returns:
            the slice
        """

    @abstractmethod
    def register_slice(self, *, slice_object: ABCSlice):
        """
        Registers the slice with the actor. The slice must be a newly created one without a database record.
        If the slice is a recovered/previously unregistered one use re_register_slice(Slice) instead.

        Args:
            slice_object: slice
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def modify_slice(self, *, slice_object: ABCSlice):
        """
        Modify the slice registered with the actor. Moves the slice into Modifying State

        Args:
            slice_object: slice_object
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def remove_slice(self, *, slice_object: ABCSlice):
        """
        Removes the specified slice. Purges slice-related state from the database.

        Args:
            slice_object: slice
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def remove_slice_by_slice_id(self, *, slice_id: ID):
        """
        Removes the specified slice. Purges slice-related state from the database.

        Args:
            slice_id: slice id
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def modify_accept(self, *, slice_id: ID):
        """
        Accept the last modify

        Args:
            slice_id: slice id
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def re_register_slice(self, *, slice_object: ABCSlice):
        """
        Re-registers the slice with the actor. The slice must already have a database record.

        Args:
            slice_object: slice
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def unregister_slice(self, *, slice_object: ABCSlice):
        """
        Unregisters the slice. Does not purge slice-related state from the database.

        Args:
            slice_object: slice
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def unregister_slice_by_slice_id(self, *, slice_id: ID):
        """
        Unregisters the slice. Does not purge slice-related state from the database.

        Args:
            slice_id: slice_id
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def close_slice_reservations(self, *, slice_id: ID):
        """
        Close slice reservations

        Args:
            slice_id: slice_id
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def fail(self, *, rid: ID, message: str):
        """
        Fails the specified reservation.

        Args:
            rid: reservation id
            message: message
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def fail_delegation(self, *, did: str, message: str):
        """
        Fails the specified delegation.

        Args:
            did: delegation id
            message: message
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def close_by_rid(self, *, rid: ID):
        """
        Closes the reservation. Note: the reservation must have already been registered with the actor.
        This method may involve either a client or a server side action or both. When called on a broker,
        this method will only close the broker reservation.

        Args:
            rid: reservation id
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def close_delegation(self, *, did: str):
        """
        Closes the delegation. Note: the delegation must have already been registered with the actor.
        This method may involve either a client or a server side action or both. When called on a broker,
        this method will only close the broker delegation.

        Args:
            did: delegation id
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def close(self, *, reservation: ABCReservationMixin):
        """
        Closes the reservation. Note: the reservation must have already been registered with the actor.
        This method may involve either a client or a server side action or both. When called on a broker,
        this method will only close the broker reservation.

        Args:
            reservation: reservation
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def close_reservations(self, *, reservations: ReservationSet):
        """
        Issues a close for every reservation in the set. All exceptions are caught and logged but no exception
        is propagated. No information will be delivered to indicate that some failure has taken place, e.g.,
        failure to communicate with a broker. Inspect the state of individual reservations to
        determine whether/what failures have taken place.

        Args:
            reservations: reservation set
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def extend(self, *, rid: ID, resources: ResourceSet, term: Term, dependencies: List[ABCReservationMixin] = None):
        """
        Extends the reservation. Note: the reservation must have already been registered with the actor.
        This method may involve either a client or a server side action or both. When called on a broker,
        this method will extend the current ticket and send an update back to the client. When called on a
        site authority, this method will extend the lease and send the new lease back.

        Args:
            reservation: reservation to extend
            rid: reservation id
            resources: resource set describing the resources desired for the extension
            term: term for extension (must extend the current term)
            dependencies: dependencies
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def get_reservation(self, *, rid: ID) -> ABCReservationMixin:
        """
        Returns the specified reservation.

        Args:
            rid: reservation id
        Returns:
            reservation
        """

    @abstractmethod
    def get_reservations(self, *, slice_id: ID) -> List[ABCReservationMixin]:
        """
        Returns all reservations in the given slice.

        Args:
            slice_id: slice id
        Returns:
            reservations
        """

    @abstractmethod
    def register(self, *, reservation: ABCReservationMixin):
        """
        Registers the reservation with the actor. The reservation must not have been previously
        registered with the actor: there should be no database record for the reservation.

        Args:
            reservation: reservation to register
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def register_delegation(self, *, delegation: ABCDelegation):
        """
        Registers the delegation with the actor. The delegation must not have been previously
        registered with the actor: there should be no database record for the delegation.

        Args:
            delegation: delegation to register
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def remove_reservation(self, *, reservation: ABCReservationMixin = None, rid: ID = None):
        """
        Removes the specified reservation. Note: the reservation must have already been registered with the actor.
        This method will unregister the reservation and remove it from the underlying database.
        Only closed and failed reservations can be removed.

        Args:
            reservation: reservation
            rid: reservation id
        Raises:
            Exception if an error occurs or when trying to remove a reservation that is neither failed or closed.
        """

    @abstractmethod
    def re_register(self, *, reservation: ABCReservationMixin):
        """
        Registers a previously registered reservation with the actor. The reservation must have a database record.

        Args:
            reservation: reservation
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def re_register_delegation(self, *, delegation: ABCDelegation):
        """
        Registers a previously registered delegation with the actor. The delegation must have a database record.

        Args:
            delegation: delegation
        Raises:
            Exception in case of error
        """

    @abstractmethod
    def unregister(self, *, reservation: ABCReservationMixin, rid: ID):
        """
        Unregisters the reservation with the actor. The reservation's database record will not be removed.

        Args:
            reservation: reservation
            rid: reservation id
        Raises:
            Exception in case of error
        """

    def get_asm_thread(self):
        return None

    @abstractmethod
    def remove_delegation(self, *, did: str):
        """
        Removes the specified delegation. Note: the delegation must have already been registered with the actor.
        This method will unregister the reservation and remove it from the underlying database.
        Only closed and failed delegation can be removed.

        Args:
            did: delegation id
        Raises:
            Exception if an error occurs or when trying to remove a delegation that is neither failed or closed.
        """

    @abstractmethod
    def update_maintenance_mode(self, *, properties: Dict[str, str], sites: List[Site] = None):
        """
        Update Maintenance mode
        @param properties properties
        @param sites sites

        @raises Exception in case of failure
        """