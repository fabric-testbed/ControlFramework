#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

import datetime


class IReservationStatus:
    """
    IReservationStatus defines a set of predicates that can be used to query the state of a reservation.
    """
    def __init__(self):
        return

    def fail(self, message: str):
        """
        Marks an operation failure. Transitions the reservation to the failed state and logs the message as an error.

        Args:
              message: error message
        """
        return

    def fail(self, message: str, exception: Exception):
        """
        Marks an operation failure. Transitions the reservation to the failed state and logs the message as an error.

        Args:
              message: error message
              exception: exception
        """
        return

    def fail_warn(self, message: str):
        """
        Marks an operation failure. Transitions the reservation to the failed state and logs the message as an error.

        Args:
              message: error message
        """
        return

    def is_action(self):
        """
        Checks if the reservation is active.

        Returns:
            true if the reservation is active
        """
        return None

    def is_active_ticketed(self):
        """
        Checks if the reservation is activeTicketed.

        Returns:
            true if the reservation is activeTicketed
        """
        return None

    def is_closed(self):
        """
        Checks if the reservation is closed.

        Returns:
            true if the reservation is closed
        """
        return None

    def is_closing(self):
        """
        Checks if the reservation is closing.

        Returns:
            true if the reservation is pending closing
        """
        return None

    def is_expired(self):
        """
        Checks if the reservation has expired.

        Returns:
            true if the reservation has expired
        """
        return None

    def is_expired(self, t: datetime):
        """
        Checks if the reservation has expired before time t.

        Args:
            t: target date
        Returns:
            true if the reservation has expired before t
        """
        return None

    def is_extended(self):
        """
        Checks if the reservation has extended at least once.

        Returns:
            true if the reservation has extended at least once
        """
        return None

    def is_extending_lease(self):
        """
        Checks if the reservation is extending a lease.

        Returns:
            true if the reservation is extending a lease
        """
        return None

    def is_extending_ticket(self):
        """
        Checks if the reservation is extending a ticket.

        Returns:
            true if the reservation is extending a ticket
        """
        return None

    def is_failed(self):
        """
        Checks if the reservation has failed.

        Returns:
            true if the reservation has failed
        """
        return None

    def is_nascent(self):
        """
        Checks if the reservation is nascent.

        Returns:
            true if the reservation pending is nascent
        """
        return None

    def is_no_pending(self):
        """
        Checks if there is no pending operation.

        Returns:
            true if there is no pending operation
        """
        return None

    def is_priming(self):
        """
        Checks if the reservation is priming.

        Returns:
            true if the reservation is priming
        """
        return None

    def is_redeeming(self):
        """
        Checks if the reservation is redeeming.

        Returns:
            true if the reservation is redeeming
        """
        return None

    def is_terminal(self):
        """
        Checks if the reservation is terminal, e.g., closing, closed, or failed.

        Returns:
            true if the reservation is terminal.
        """
        return None

    def is_ticketed(self):
        """
        Check if the reservation is ticketed.

        Returns:
            true if the reservation is ticketed.
        """
        return None

    def is_ticketing(self):
        """
        Checks if the reservation is obtaining a new ticket.

        Returns:
            true if the reservation is obtaining a ticket
        """
        return None

    def set_expired(self, value: bool):
        """
        Sets the expiration flag.

        Args:
            value: true if the reservation is expired
        """
        return
