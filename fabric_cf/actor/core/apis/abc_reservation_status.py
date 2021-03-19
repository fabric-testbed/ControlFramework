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
    import datetime


class ABCReservationStatus(ABC):
    """
    IReservationStatus defines a set of predicates that can be used to query the state of a reservation.
    """

    @abstractmethod
    def fail(self, *, message: str, exception: Exception = None):
        """
        Marks an operation failure. Transitions the reservation to the failed state and logs the message as an error.

        Args:
              message: error message
              exception: exception
        """

    @abstractmethod
    def fail_warn(self, *, message: str):
        """
        Marks an operation failure. Transitions the reservation to the failed state and logs the message as an error.

        Args:
              message: error message
        """

    @abstractmethod
    def is_active(self) -> bool:
        """
        Checks if the reservation is active.

        Returns:
            true if the reservation is active
        """

    @abstractmethod
    def is_active_ticketed(self) -> bool:
        """
        Checks if the reservation is activeTicketed.

        Returns:
            true if the reservation is activeTicketed
        """

    @abstractmethod
    def is_closed(self) -> bool:
        """
        Checks if the reservation is closed.

        Returns:
            true if the reservation is closed
        """

    @abstractmethod
    def is_closing(self) -> bool:
        """
        Checks if the reservation is closing.

        Returns:
            true if the reservation is pending closing
        """

    @abstractmethod
    def is_expired(self, *, t: datetime = None) -> bool:
        """
        Checks if the reservation has expired before time t.

        Args:
            t: target date
        Returns:
            true if the reservation has expired before t
        """

    @abstractmethod
    def is_extended(self) -> bool:
        """
        Checks if the reservation has extended at least once.

        Returns:
            true if the reservation has extended at least once
        """

    @abstractmethod
    def is_extending_lease(self) -> bool:
        """
        Checks if the reservation is extending a lease.

        Returns:
            true if the reservation is extending a lease
        """

    @abstractmethod
    def is_extending_ticket(self) -> bool:
        """
        Checks if the reservation is extending a ticket.

        Returns:
            true if the reservation is extending a ticket
        """

    @abstractmethod
    def is_failed(self) -> bool:
        """
        Checks if the reservation has failed.

        Returns:
            true if the reservation has failed
        """

    @abstractmethod
    def is_nascent(self) -> bool:
        """
        Checks if the reservation is nascent.

        Returns:
            true if the reservation pending is nascent
        """

    @abstractmethod
    def is_no_pending(self) -> bool:
        """
        Checks if there is no pending operation.

        Returns:
            true if there is no pending operation
        """

    @abstractmethod
    def is_priming(self) -> bool:
        """
        Checks if the reservation is priming.

        Returns:
            true if the reservation is priming
        """

    @abstractmethod
    def is_redeeming(self) -> bool:
        """
        Checks if the reservation is redeeming.

        Returns:
            true if the reservation is redeeming
        """

    @abstractmethod
    def is_terminal(self) -> bool:
        """
        Checks if the reservation is terminal, e.g., closing, closed, or failed.

        Returns:
            true if the reservation is terminal.
        """

    @abstractmethod
    def is_ticketed(self) -> bool:
        """
        Check if the reservation is ticketed.

        Returns:
            true if the reservation is ticketed.
        """

    @abstractmethod
    def is_ticketing(self) -> bool:
        """
        Checks if the reservation is obtaining a new ticket.

        Returns:
            true if the reservation is obtaining a ticket
        """

    @abstractmethod
    def set_expired(self, *, value: bool):
        """
        Sets the expiration flag.

        Args:
            value: true if the reservation is expired
        """
