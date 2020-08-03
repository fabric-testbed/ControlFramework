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

from fabric.actor.core.apis.i_slice import ISlice
if TYPE_CHECKING:
    from fabric.actor.core.apis.i_kernel_reservation import IKernelReservation
    from fabric.actor.core.util.id import ID
    from fabric.actor.core.util.reservation_set import ReservationSet
    from fabric.actor.security.auth_token import AuthToken
    from fabric.actor.security.guard import Guard


class IKernelSlice(ISlice):
    """
    Kernel-level interface for slice objects.
    """
    def get_guard(self) -> Guard:
        """
        Returns the slice guard.
       
        @return the guard
        """
        raise NotImplementedError("Should have implemented this")

    def set_broker_client(self):
        """
        Marks the slice as a broker client slice (a client slice within
        an authority that represents a broker).
        """
        raise NotImplementedError("Should have implemented this")

    def set_client(self):
        """
         Marks the slice as a client slice.
        """
        raise NotImplementedError("Should have implemented this")

    def get_reservations(self)->ReservationSet:
        """
        Returns the reservation set.
       
        @return reservation set
        """
        raise NotImplementedError("Should have implemented this")

    def get_reservations_list(self)->list:
        """
        Returns the reservation set represented as a list. Must be
        called with the kernel lock on to prevent exceptions due to concurrent
        iteration.
       
        @return a list of reservation
        """
        raise NotImplementedError("Should have implemented this")

    def is_empty(self) -> bool:
        """
        Checks if the slice is empty.
       
        @return true if there are no reservations in the slice
        """
        raise NotImplementedError("Should have implemented this")

    def prepare(self):
        """
        Prepares to register a new slice.  Clears previous state, such
        as list of reservations in the slice.
       
        @raises Exception if validity checks fail
        """
        raise NotImplementedError("Should have implemented this")

    def register(self, reservation: IKernelReservation):
        """
        Registers a new reservation.
       
        @param reservation reservation to register
       
        @throws Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def soft_lookup(self, rid: ID):
        """
        Looks up a reservation by ID but does not throw error if the
        reservation is not present in the slice.
       
        @params rid the reservation ID
       
        @returns the reservation with that ID
        """
        raise NotImplementedError("Should have implemented this")

    def unregister(self, reservation: IKernelReservation):
        """
        Unregisters the reservation from the slice.
       
        @param reservation reservation to unregister
        """
        raise NotImplementedError("Should have implemented this")

    def set_owner(self, auth: AuthToken):
        """
        Sets the slice owner.
       
        @param auth the slice owner
        """
        raise NotImplementedError("Should have implemented this")
