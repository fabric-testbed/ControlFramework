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

from abc import abstractmethod, ABC
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
    from fabric_cf.actor.core.time.term import Term
    from fabric_cf.actor.core.util.reservation_set import ReservationSet
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation


class ABCPolicy(ABC):
    """
    IPolicy encapsulates all policy decisions an actor must make to
    perform its functions. Each actor has one policy instance that controls its
    policy decisions. The policy instance must implement the IPolicy
    interface together with one or more of the other policy interfaces (depending
    on the role the actor performs).
    Some things to know when implementing a policy:
    - The policy is always called by a single thread. No synchronization is
    required, but method implementations should not block for long periods of
    time, as that would prevent the actor from processing other events.
    - The policy operates on the passed ResourceSets. ResourceSets encapsulate
    the free resource pools and resource requests. The policy satisfies requests
    by using ResourceSet methods to shift resources from one ResourceSet to
    another. Resource attributes etc. flow with these operations, and they drive
    other resource-specific operations including SHARP ticket delegation.
    - The policy has access to all ResourceData attributes attached to the
    request and the free resource pools. The client-side methods may decorate the
    request with attributes (e.g., bids), which are passed through to the
    server-side policy that handles the request. The client-side also has the
    option of touching the AuthToken passed with the request...this may be a bit
    goofy.
    - If it is necessary to defer an allocation request, return false. At some
    later time, when the policy is ready to satisfy the request (e.g., after an
    auction on an agent), set bidPending to false and arrange a call to
    probePending on the requesting reservation. This causes the reservation to
    retry the pending operation.
    """
    @abstractmethod
    def prepare(self, *, cycle: int):
        """
        Informs the policy that processing for a new cycle is about to begin. The
        policy should initialize whatever internal state is necessary to process
        a new cycle.
        Note: The cycle number parameter is redundant and is passed for
        convenience. The policy can always obtain the cycle number by calling
        IActor.getCurrentCycle()
        """

    @abstractmethod
    def finish(self, *, cycle: int):
        """
        Informs the policy that all processing for the specified cycle is
        complete. The policy can safely discard any state associated with the
        cycle or any previous cycles.
        Note: The cycle number parameter is redundant and is passed for
        convenience. The policy can always obtain the cycle number by calling
        IActor.getCurrentCycle()
        """

    @abstractmethod
    def extend(self, *, reservation: ABCReservationMixin, resources: ResourceSet, term: Term):
        """
        Notifies the policy that a reservation is about to be extended. This
        method will be invoked only for reservations, whose extensions have not
        been triggered by the policy, e.g, from the management interface. The
        policy should update its state to reflect the extend request.

        @params reservation: reservation to be extended
        @params resources: resource set used for the extension
        @params term: term used for the extension
        """

    @abstractmethod
    def close(self, *, reservation: ABCReservationMixin):
        """
        Notifies the policy that a reservation is about to be closed. This method
        will be invoked for every reservation that is about to be closed, even if
        the close was triggered by the policy itself. The policy should update
        its internal state/cancel pending operations associated with the
        reservation.

        @params reservation: reservation about to be closed
        """

    @abstractmethod
    def closed(self, *, reservation: ABCReservationMixin):
        """
        Notifies the policy that a reservation has been closed. This method will
        be invoked for every reservation that closes successfully. The policy
        must uncommit any resources associated with the reservation, e.g,
        physical machines, currency, etc.
        Note: For an authority resources are released using the
        IAuthorityPolicy.release(ResourceSet) method. Authority policy
        implementations should not consider the resources of the passed
        reservation as released. The release will take place once all
        configuration actions complete.

        @params reservation: closed reservation
        """

    @abstractmethod
    def closed_delegation(self, *, delegation: ABCDelegation):
        """
        Notifies the policy that a delegation has been closed. This method will
        be invoked for every delegation that closes successfully. The policy
        must uncommit any resources associated with the delegation, e.g,
        physical machines, currency, etc.

        @params delegation: closed delegation
        """

    @abstractmethod
    def remove(self, *, reservation: ABCReservationMixin):
        """
        Notifies the policy that a reservation is about to be removed. This
        method will be invoked for each reservation that is to be removed from
        the system. The policy should remove any state that it maintains for the
        reservation.
        Note: Only failed and closed reservations can be removed. The
        system will not invoke this method if the reservation is not closed or
        failed.

        @params reservation: reservation to remove
        """

    @abstractmethod
    def query(self, *, p) -> dict:
        """
        Answers a query from another actor. This method is intended to be used to
        obtain policy-specific parameters and information. This method should be
        used when writing more complex policies requiring additional interaction
        among actors. Instead of extending the proxies to support
        passing/obtaining the required information, policy code can use the query
        interface to request/obtain such information. The implementation should
        not block for prolonged periods of time. If necessary, future versions
        will update this interface to allow query responses to be delivered using
        callbacks.

        @params p : a properties list of query parameters. Can be null or empty.

        @returns a properties list of outgoing values. If the incoming properties
                collection is null or empty, should return all possible
                properties that can be relevant to the caller.
        """

    @abstractmethod
    def configuration_complete(self, *, action: str, token: ConfigToken, out_properties: dict):
        """
        Notifies the policy that a configuration action for the object
        represented by the token parameter has completed.

        @params action : configuration action. See Config.Target*
        @params token : object or a token for the object whose configuration action has completed
        @params out_properties : output properties produced by the configuration action
        """

    @abstractmethod
    def reset(self):
        """
        Post recovery entry point. This method will be invoked once all revisit
        operations are complete and the actor is ready to operate normally.
        @raises Exception in case of error
        """

    @abstractmethod
    def recovery_starting(self):
        """
        Informs the policy that recovery is about to begin.
        """

    @abstractmethod
    def revisit(self, *, reservation: ABCReservationMixin):
        """
        Informs the policy about a reservation. Called during recovery/startup.
        The policy must re-establish any state required for the management of the
        reservation.

        @params reservation: reservation being recovered
        @raises Exception in case of error
        """

    @abstractmethod
    def revisit_delegation(self, *, delegation: ABCDelegation):
        """
        Informs the policy about a delegation. Called during recovery/startup.
        The policy must re-establish any state required for the management of the
        delegation.

        @params delegation: delegation being recovered
        @raises Exception in case of error
        """

    @abstractmethod
    def recovery_ended(self):
        """
        Informs the policy that recovery has completed.
        """

    @abstractmethod
    def set_actor(self, *, actor: ABCActorMixin):
        """
        Sets the actor the policy belongs to.

        @params actor : the actor the policy belongs to
        """

    @abstractmethod
    def get_closing(self, *, cycle: int) -> ReservationSet:
        """
        Returns a set of reservations that must be closed.

        @params cycle: the current cycle

        @returns reservations to be closed
        """

    @abstractmethod
    def get_guid(self) -> ID:
        """
        Returns the globally unique identifier of this policy object instance.

        @returns guid
        """

    @abstractmethod
    def set_logger(self, logger):
        """
        Set logger
        @param logger logger
        """

    @abstractmethod
    def set_properties(self, properties: dict):
        """
        Set Properties
        """