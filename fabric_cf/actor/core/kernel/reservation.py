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

from datetime import datetime, timezone
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin, ReservationCategory
from fabric_cf.actor.core.common.exceptions import ReservationException
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates, JoinState
from fabric_cf.actor.core.util.reservation_state import ReservationState

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.kernel.request_types import RequestTypes
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.core.time.term import Term
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.util.resource_type import ResourceType


class Reservation(ABCReservationMixin):
    """
    These are the only methods synchronized on the Reservation object itself. The
    purpose is to allow an external thread to await a state transition in a
    Reservation without holding the orchestrator lock. State changes are made only
    while holding the orchestrator lock, so a orchestrator may examine the state without
    acquiring the reservation lock.

    Reservation objects passed into a slices actor to initiate or request new
    reservations are taken over by the kernel. Validate the passed-in state, mark
    some context specific to the operation, and clean out the rest of it in
    preparation to link it into orchestrator structures. No locks are held, and these
    routines have no side effects other than to the (new) reservation.

    Reservation is the base for all reservation objects. It
    implements a part of the IReservation interface and defines the
    core functions expected by the kernel from all reservation classes. This is
    an abstract class and is intended as a building block of higher-level
    reservation classes.
    """

    def __init__(self, *, rid: ID = None, resources: ResourceSet = None, term: Term = None,
                 slice_object: ABCSlice = None):
        # The unique reservation identifier.
        self.rid = rid
        # Reservation category. Subclasses should supply the correct value.
        self.category = ReservationCategory.All
        # Reservation state.
        self.state = ReservationStates.Nascent
        # Reservation pending state.
        self.pending_state = ReservationPendingStates.None_
        # Has this reservation ever been extended?
        self.extended = False
        # The current resources associated with this reservation.
        self.resources = resources
        # Resources representing the last request issued/received for this reservation
        self.requested_resources = None
        # Resources approved by the policy for this reservation. This resource set
        # can be different from what was initially requested (requested_resources)
        # Eventually, resources will be merged with approved_resources.
        self.approved_resources = None
        # The current term of the reservation.
        self.term = term
        # The previous term of the reservation.
        self.previous_term = None
        # The term of the last request issued/received for this reservation.
        self.requested_term = None
        # The term the policy approved for this reservation. This term can be
        # different from what was initially requested (requested_term). Eventually,
        # term will be set to equal approved_term.
        self.approved_term = None
        # True if this is a renewable reservation. By default, reservations are not renewable
        self.renewable = None
        # Last error message.
        self.error_message = None
        # Cached pointer to the actor that operates on this reservation.
        self.actor = None
        # Logger
        self.logger = None
        # Slice this reservation belongs to.
        self.slice = slice_object
        if slice_object is not None:
            # Cached slice name. Necessary so that we can obtain the slice for
            # reservations that have not been fully recovered.
            self.slice_name = slice_object.get_name()
            # Cached slice id. Necessary so that we can obtain the slice for
            # reservations that have not been fully recovered.
            self.slice_id = slice_object.get_slice_id()
        else:
            self.slice_name = None
            self.slice_id = None
        # Indicates if the policy plugin has made a decision about this reservation
        self.approved = False
        # The resources assigned to the reservation before the last update.
        self.previous_resources = None
        # Is an allocation process in progress?
        self.bid_pending = False
        # Dirty flag. Indicates that the state of the reservation object has
        # changed since the last time it was persisted. Currently only transition
        # updates the dirty flag
        self.dirty = False
        # True if this reservation is expired. Used during recovery.
        self.expired = False
        # Recovery flag.
        self.pending_recover = False
        # True if the last state transition is not committed to external storage.
        # false otherwise.
        self.state_transition = False
        # Scratch element to trigger post-actions on a probe.
        self.service_pending = ReservationPendingStates.None_
        self.last_transition_time = None
        self.last_pending_state = ReservationPendingStates.None_

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['actor']
        del state['logger']
        del state['slice']
        del state['approved']
        del state['previous_resources']
        del state['bid_pending']
        del state['dirty']
        del state['expired']
        del state['pending_recover']
        del state['state_transition']
        del state['service_pending']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.actor = None
        self.logger = None
        self.slice = None
        self.approved = False
        self.previous_resources = None
        self.bid_pending = False
        self.dirty = False
        self.expired = False
        self.pending_recover = False
        self.state_transition = False
        self.service_pending = ReservationPendingStates.None_

    def restore(self, *, actor: ABCActorMixin, slice_obj: ABCSlice):
        """
        Must be invoked after creating reservation from unpickling
        """
        self.actor = actor
        self.slice = slice_obj
        if actor is not None:
            self.logger = self.actor.get_logger()
        self.approved = False
        self.previous_resources = None
        self.bid_pending = False
        self.dirty = False
        self.expired = False
        self.pending_recover = False
        self.state_transition = False
        self.service_pending = ReservationPendingStates.None_
        if actor is not None:
            if self.resources is not None:
                self.resources.restore(plugin=actor.get_plugin(), reservation=self)
            if self.requested_resources is not None:
                self.requested_resources.restore(plugin=actor.get_plugin(), reservation=self)
            if self.approved_resources is not None:
                self.approved_resources.restore(plugin=actor.get_plugin(), reservation=self)

    def can_redeem(self) ->bool:
        return True

    def can_renew(self) -> bool:
        return True

    def internal_error(self, *, err: str):
        """
        Internal error log and raise exception
        """
        self.logger.error(f"internal error for reservation: {self} : {err}")
        raise ReservationException(f"internal error: {err}")

    def error(self, *, err: str):
        """
        Error log and raise exception
        """
        if self.logger is not None:
            self.logger.error(f"error for reservation: {self} : {err}")
        else:
            print(f"error for reservation: {self} : {err}")
        raise ReservationException(f"error: {err}")

    def clear_dirty(self):
        """
        Clear dirty flag
        """
        self.dirty = False
        self.state_transition = False

    def clear_notice(self):
        """
        Clears all event notices associated with the reservation.
        """

    def close(self):
        """
        Close a reservation
        """

    def extend_lease(self):
        """
        Extend lease on reservation
        """

    def modify_lease(self):
        """
        Modify lease on reservation
        """

    def extend_ticket(self, *, actor: ABCActorMixin):
        """
        Extend a ticket
        """
        self.internal_error(err="abstract extend_ticket trap")

    def fail(self, *, message: str, exception: Exception = None):
        """
        Fail a reservation
        """
        self.error_message = message
        self.bid_pending = False
        self.transition(prefix=message, state=ReservationStates.Failed, pending=ReservationPendingStates.None_)
        self.logger.error(f"{message}  e: {exception}")

    def fail_warn(self, *, message: str):
        """
        Fail with a warning
        """
        self.error_message = message
        self.transition(prefix=message, state=ReservationStates.Failed, pending=ReservationPendingStates.None_)
        message = f"reservation has failed: {message} : [{self}]"
        self.logger.warning(message)

    def get_actor(self):
        return self.actor

    def get_approved_resources(self) -> ResourceSet:
        return self.approved_resources

    def get_approved_term(self) -> Term:
        return self.approved_term

    def get_approved_type(self) -> ResourceType:
        if self.approved_resources is not None:
            return self.approved_resources.get_type()
        return None

    def get_approved_units(self) -> int:
        if self.approved_resources is not None:
            return self.approved_resources.get_units()
        return 0

    def get_category(self) -> ReservationCategory:
        return self.category

    def get_kernel_slice(self) -> ABCSlice:
        return self.slice

    def get_leased_abstract_units(self) -> int:
        return 0

    def get_leased_units(self) -> int:
        return 0

    def get_notices(self) -> str:
        """
        Returns a descriptive string if this reservation requires attention, else None

        @return notices string
        """
        msg = f"Reservation {self.rid} (Slice {self.slice}) is in state ({self.get_state_name()}," \
              f"{self.get_pending_state_name()})"

        if self.error_message is not None and self.error_message != "":
            msg += f", err={self.error_message}"
        return msg

    def get_pending_state(self) -> ReservationPendingStates:
        return self.pending_state

    def get_pending_state_name(self) -> str:
        return ReservationPendingStates(self.pending_state).name

    def get_previous_resources(self) -> ResourceSet:
        return self.previous_resources

    def get_previous_term(self) -> Term:
        return self.previous_term

    def get_requested_resources(self) -> ResourceSet:
        return self.requested_resources

    def get_requested_term(self) -> Term:
        return self.requested_term

    def get_requested_type(self) -> ResourceType:
        if self.requested_resources is not None:
            return self.requested_resources.get_type()
        return None

    def get_requested_units(self) -> int:
        if self.requested_resources is not None:
            return self.requested_resources.get_units()
        return 0

    def get_reservation_id(self) -> ID:
        return self.rid

    def get_reservation_state(self) -> ReservationState:
        return ReservationState(state=self.state, pending=self.pending_state)

    def get_resources(self) -> ResourceSet:
        return self.resources

    def get_slice(self) -> ABCSlice:
        return self.slice

    def get_slice_id(self):
        if self.slice is None:
            return None
        return self.slice.get_slice_id()

    def get_slice_name(self):
        if self.slice is None:
            return None
        return self.slice.get_name()

    def get_state(self) -> ReservationStates:
        return self.state

    def get_state_name(self) -> str:
        return self.state.name

    def get_term(self) -> Term:
        return self.term

    def get_type(self) -> ResourceType:
        if self.resources is None:
            return None
        return self.resources.get_type()

    def get_units(self, *, when: datetime = None) -> int:
        if when is None:
            if self.resources is None:
                return 0
            return self.resources.get_units()
        if not self.is_terminal() and self.term is not None and self.term.contains(date=when):
            return self.resources.get_concrete_units(when=when)
        return 0

    def handle_duplicate_request(self, *, operation: RequestTypes):
        return

    def has_uncommitted_transition(self) -> bool:
        return self.state_transition

    def is_active(self) -> bool:
        return self.state == ReservationStates.Active or self.state == ReservationStates.ActiveTicketed

    def is_active_ticketed(self) -> bool:
        return self.state == ReservationStates.ActiveTicketed

    def is_approved(self) -> bool:
        return self.approved

    def is_bid_pending(self) -> bool:
        """
        Is bid pending
        @return bid pending
        """
        return self.bid_pending

    def is_closed(self) -> bool:
        return self.state == ReservationStates.Closed

    def is_closing(self) -> bool:
        return self.state == ReservationStates.CloseWait or self.pending_state == ReservationPendingStates.Closing

    def is_dirty(self) -> bool:
        return self.dirty

    def is_expired(self, *, t: datetime = None) -> bool:
        if t is None:
            return self.expired
        return self.term.expired(date=t)

    def is_extended(self) -> bool:
        return self.extended

    def is_extending_lease(self) -> bool:
        return self.pending_state == ReservationPendingStates.ExtendingLease

    def is_extending_ticket(self) -> bool:
        return self.pending_state == ReservationPendingStates.ExtendingTicket

    def is_failed(self) -> bool:
        return self.state == ReservationStates.Failed

    def is_nascent(self) -> bool:
        return self.state == ReservationStates.Nascent

    def is_no_pending(self) -> bool:
        return self.pending_state == ReservationPendingStates.None_

    def is_pending_recover(self):
        return self.pending_recover

    def is_priming(self) -> bool:
        return self.pending_state == ReservationPendingStates.Priming

    def is_redeeming(self) -> bool:
        return self.pending_state == ReservationPendingStates.Redeeming

    def is_renewable(self) -> bool:
        """
        Is reservation renewable
        @return true if renewable; false otherwise
        """
        return self.renewable

    def is_terminal(self) -> bool:
        return self.is_closed() or self.is_closing() or self.is_failed()

    def is_ticketed(self) -> bool:
        return self.state == ReservationStates.Ticketed

    def is_ticketing(self) -> bool:
        return self.pending_state == ReservationPendingStates.Ticketing

    def nothing_pending(self):
        """
        Ensures the reservation does not have a pending operation.

        @throws Exception if the reservation has a pending operation.
        """
        if self.pending_state != ReservationPendingStates.None_:
            self.error(err="reservation has a pending operation")

    def prepare_probe(self):
        return

    def probe_pending(self):
        return

    def ready(self):
        """
        An incoming client request named this validated Reservation object for an
        existing reservation. Check to be sure that it has not been destroyed in
        a race since the validate.

        @throws Exception thrown if the state is closed or failed
        """
        if self.state == ReservationStates.Closed or self.state == ReservationStates.Failed:
            self.error(err="invalid Reservation")

    def reserve(self, *, policy: ABCPolicy):
        return

    def setup(self):
        """
        Setup a reservation
        """
        if self.resources is not None:
            self.resources.setup(reservation=self)

        if self.approved_resources is not None:
            self.approved_resources.setup(reservation=self)

        if self.requested_resources is not None:
            self.requested_resources.setup(reservation=self)

    def service_claim(self):
        return

    def service_reclaim(self):
        return

    def service_close(self):
        return

    def service_extend_lease(self):
        return

    def service_modify_lease(self):
        return

    def service_extend_ticket(self):
        return

    def service_probe(self):
        return

    def service_reserve(self):
        return

    def service_update_lease(self):
        return

    def service_update_ticket(self):
        return

    def set_actor(self, *, actor: ABCActorMixin):
        self.actor = actor

    def set_approved(self, *, term: Term = None, approved_resources: ResourceSet = None):
        self.approved_term = term
        self.approved_resources = approved_resources
        self.approved = True

    def set_approved_resources(self, *, approved_resources: ResourceSet):
        self.approved_resources = approved_resources

    def set_approved_term(self, *, term: Term):
        self.approved_term = term

    def set_bid_pending(self, *, value: bool):
        """
        Set Bid Pending
        @param value value
        """
        self.bid_pending = value

    def set_dirty(self):
        """
        Set dirty
        """
        self.dirty = True

    def set_expired(self, *, value: bool):
        self.expired = value

    def set_logger(self, *, logger):
        self.logger = logger

    def set_pending_recover(self, *, pending_recover: bool):
        self.pending_recover = pending_recover

    def set_service_pending(self, *, code: ReservationPendingStates):
        self.service_pending = code

    def set_slice(self, *, slice_object: ABCSlice):
        self.slice = slice_object

    def __str__(self):
        msg = "res: "
        if self.rid is not None:
            msg += f"#{self.rid} "

        if self.slice is not None:
            msg += f"slice: [{self.slice}] "

        msg += f"state:[{self.get_state_name()},{self.get_pending_state_name()}] "

        if self.resources is not None:
            msg += f"resources: [{self.resources}] "

        if self.term is not None:
            msg += f"term: [{self.term}]"

        return msg

    def transition(self, *, prefix: str, state: ReservationStates, pending: ReservationPendingStates):
        if self.state == ReservationStates.Failed and self.logger is not None:
            self.logger.debug("failed")

        if self.logger is not None:
            self.logger.debug(f"Reservation #{self.rid} {prefix} transition: {self.get_state_name()} -> {state.name}, "
                              f"{self.get_pending_state_name()} -> {pending.name}")

        self.state = state
        self.last_pending_state = pending
        self.pending_state = pending

        self.set_dirty()
        self.state_transition = True
        self.last_transition_time = datetime.now(timezone.utc)

    def update_lease(self, *, incoming: ABCReservationMixin, update_data):
        self.internal_error(err="abstract update_lease trap")

    def update_ticket(self, *, incoming: ABCReservationMixin, update_data):
        self.internal_error(err="abstract update_ticket trap")

    def validate_outgoing(self):
        return

    def validate_incoming(self):
        return

    def validate(self):
        """
        Validates the reservation. For use by prepare() methods defined by
        subclasses.

        @throws Exception
        """
        assert self.state == ReservationStates.Nascent
        self.nothing_pending()

        if self.slice is None:
            self.error(err="no slice specified")

        if self.resources is None:
            self.error(err="no resource set specified")

        if self.term is None:
            self.error(err="no term specified")

        self.term.validate()

    def get_join_state(self) -> JoinState:
        """
        Get Join State
        @return join state
        """
        return JoinState.None_

    class CountHelper:
        """
        Helper class for counting units.
        """
        def __init__(self):
            self.pending = 0
            self.active = 0
            self.type = None

    def get_graph_node_id(self) -> str:
        if self.requested_resources is not None:
            request = self.requested_resources.get_sliver()
            if request is not None:
                node_map = request.get_node_map()
                if node_map is not None:
                    return node_map[1]
        return None

    def exceeds_timeout(self, timeout: int) -> bool:
        """
        Check if Reservation has been in the state for more than the timeout interval
        @param timeout: timeout
        @return True if reservation has been in the state for more than timeout; False otherwise
        """
        if self.last_transition_time is None:
            return False

        current_time = datetime.now(timezone.utc)
        if (current_time - self.last_transition_time).seconds > timeout:
            return True

        return False

    def get_error_message(self) -> str:
        return self.error_message
