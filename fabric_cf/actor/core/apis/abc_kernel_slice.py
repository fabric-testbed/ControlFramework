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
from typing import TYPE_CHECKING, List, Dict

from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_slice import ABCSlice

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_kernel_reservation import ABCKernelReservation
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.util.reservation_set import ReservationSet
    from fabric_cf.actor.security.auth_token import AuthToken


class ABCKernelSlice(ABCSlice):
    """
    Kernel-level interface for slice objects.
    """
    @abstractmethod
    def set_broker_client(self):
        """
        Marks the slice as a broker client slice (a client slice within
        an authority that represents a broker).
        """

    @abstractmethod
    def set_client(self):
        """
         Marks the slice as a client slice.
        """

    @abstractmethod
    def get_reservations(self) -> ReservationSet:
        """
        Returns the reservation set.

        @return reservation set
        """

    @abstractmethod
    def get_delegations(self) -> Dict[str, ABCDelegation]:
        """
        Returns the delegations dict.

        @return delegations dict
        """

    @abstractmethod
    def get_reservations_list(self) -> List[ABCKernelReservation]:
        """
        Returns the reservation set represented as a list. Must be
        called with the kernel lock on to prevent exceptions due to concurrent
        iteration.

        @return a list of reservation
        """

    @abstractmethod
    def is_empty(self) -> bool:
        """
        Checks if the slice is empty.

        @return true if there are no reservations in the slice
        """

    @abstractmethod
    def prepare(self):
        """
        Prepares to register a new slice.  Clears previous state, such
        as list of reservations in the slice.

        @raises Exception if validity checks fail
        """

    @abstractmethod
    def register(self, *, reservation: ABCKernelReservation):
        """
        Registers a new reservation.

        @param reservation reservation to register

        @throws Exception in case of error
        """

    @abstractmethod
    def soft_lookup(self, *, rid: ID) -> ABCKernelReservation:
        """
        Looks up a reservation by ID but does not throw error if the
        reservation is not present in the slice.

        @params rid the reservation ID

        @returns the reservation with that ID
        """

    @abstractmethod
    def unregister(self, *, reservation: ABCKernelReservation):
        """
        Unregisters the reservation from the slice.

        @param reservation reservation to unregister
        """

    @abstractmethod
    def set_owner(self, *, owner: AuthToken):
        """
        Sets the slice owner.

        @param auth the slice owner
        """

    @abstractmethod
    def register_delegation(self, *, delegation: ABCDelegation):
        """
        Registers a new delegation.

        @param delegation delegation to register

        @throws Exception in case of error
        """

    @abstractmethod
    def soft_lookup_delegation(self, *, did: str) -> ABCDelegation:
        """
        Looks up a delegation by ID but does not throw error if the
        delegation is not present in the slice.

        @params did the delegation ID

        @returns the delegation with that ID
        """

    @abstractmethod
    def unregister_delegation(self, *, delegation: ABCDelegation):
        """
        Unregisters the delegation from the slice.

        @param delegation delegation to unregister
        """
