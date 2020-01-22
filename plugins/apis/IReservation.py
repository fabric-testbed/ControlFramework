#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from plugins.apis.IReservationResources import IReservationResources
from plugins.apis.IReservationStatus import IReservationStatus


class IReservation(IReservationResources, IReservationStatus):
    """
    IReservation defines the the core API for a reservation. Most of the methods described in the interface allow the
    programmer to inspect the state of the reservation, access some of its core objects,
    and wait for the occurrence of a particular event.
    """
    def __init__(self):
        return None

    def clear_dirty(self):
        """
        Marks that the reservation has no uncommitted updates or state transitions.
        """
        return

    def get_actor(self):
        """
        Returns the actor in control of the reservation.

        Returns:
            the actor in control of the reservation.
        """
        return None

    def get_category(self):
        """
        Returns the reservation category.

        Returns:
            the reservation category.
        """
        return None

    def get_pending_state(self):
        """
        Returns the current pending reservation state.

        Returns:
            current pending reservation state.
        """
        return None

    def get_reservation_id(self):
        """
        Returns the reservation id.

        Returns:
            the reservation id.
        """
        return None

    def get_reservation_state(self):
        """
        Returns the current composite reservation state.

        Returns:
            current composite reservation state.
        """
        return None

    def get_slice(self):
        """
        Returns the slice the reservation belongs to.

        Returns:
            slice the reservation belongs to.
        """
        return None

    def get_slice_id(self):
        """
        Returns the slice GUID.

        Returns:
            slice guid
        """
        return None

    def get_slice_name(self):
        """
        Returns the slice name.

        Returns:
            slice name
        """
        return None

    def get_state(self):
        """
        Returns the current reservation state.

        Returns:
            current reservation state.
        """
        return None

    def get_state_name(self):
        """
        Returns the current reservation state name.

        Returns:
            current reservation state name.
        """
        return None

    def get_type(self):
        """
        Returns the resource type allocated to the reservation. If no resources have yet been allocated to the
        reservation, this method will return None.

        Returns:
            resource type allocated to the reservation. None if no resources have been allocated to the reservation.
        """
        return None

    def has_uncommitted_transition(self):
        """
        Checks if the reservation has uncommitted state transitions.

        Returns:
            true if the reservation has an uncommitted transition
        """
        return False

    def is_dirty(self):
        """
        Checks if the reservation has uncommitted updates.

        Returns:
            true if the reservation has an uncommitted updates
        """
        return False

    def is_pending_recover(self):
        """
        Checks if a recovery operation is in progress for the reservation

        Returns:
            true if a recovery operation for the reservation is in progress
        """
        return False

    def setDirty(self):
        """
        Marks the reservation as containing uncommitted updates.
        """
        return

    def set_pending_recover(self, pending_recover:bool):
        """
        Indicates if a recovery operation for the reservation is going to be in progress.

        Args:
            pending_recover: true, a recovery operation is in progress, false - no recovery operation is in progress.
        """
        return

    def set_slice(self, slice):
        """
        Sets the slice the reservation belongs to.

        Args:
            slice: slice the reservation belongs to
        """
        return

    def transition(self, state):
        """
        Transitions this reservation into a new state.

        Args:
            state: the new state
        """
        return None
