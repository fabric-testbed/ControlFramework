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

from fabric.actor.core.util.id import ID

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from fabric.actor.core.time.term import Term
    from fabric.actor.core.util.reservation_set import ReservationSet
    from fabric.actor.core.kernel.sesource_set import ResourceSet
    from fabric.actor.core.apis.i_reservation import IReservation


class IReservationOperations:
    """
    ReservationOperations</code> defines the core reservations management operations supported by each actor.
    """
    def fail(self, rid: ID, message: str):
        """
        Fails the specified reservation.

        Args:
            rid: reservation id
            message: message
        Raises:
            Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def close_by_rid(self, rid: ID):
        """
        Closes the reservation. Note: the reservation must have already been registered with the actor.
        This method may involve either a client or a server side action or both. When called on a broker,
        this method will only close the broker reservation.

        Args:
            rid: reservation id
        Raises:
            Exception in case of error
        """
        raise NotImplementedError("Should have implemented this" )

    def close(self, reservation: IReservation):
        """
        Closes the reservation. Note: the reservation must have already been registered with the actor.
        This method may involve either a client or a server side action or both. When called on a broker,
        this method will only close the broker reservation.

        Args:
            reservation: reservation
        Raises:
            Exception in case of error
        """
        raise NotImplementedError( "Should have implemented this" )

    def close_reservations(self, reservations: ReservationSet):
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
        raise NotImplementedError( "Should have implemented this" )

    def extend(self, rid: ID, resources: ResourceSet, term: Term):
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
        Raises:
            Exception in case of error
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_reservation(self, rid: ID) -> IReservation:
        """
        Returns the specified reservation.

        Args:
            rid: reservation id
        Returns:
            reservation
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_reservations(self, slice_id: ID) -> list:
        """
        Returns all reservations in the given slice.

        Args:
            slice_id: slice id
        Returns:
            reservations
        """
        raise NotImplementedError( "Should have implemented this" )

    def register(self, reservation: IReservation):
        """
        Registers the reservation with the actor. The reservation must not have been previously
        registered with the actor: there should be no database record for the reservation.

        Args:
            reservation: reservation to register
        Raises:
            Exception in case of error
        """
        raise NotImplementedError( "Should have implemented this" )

    def remove_reservation(self, reservation: IReservation = None, rid: ID = None):
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
        raise NotImplementedError( "Should have implemented this" )

    def re_register(self, reservation: IReservation):
        """
        Registers a previously registered reservation with the actor. The reservation must have a database record.

        Args:
            reservation: reservation
        Raises:
            Exception in case of error
        """
        raise NotImplementedError( "Should have implemented this" )

    def unregister(self, reservation: IReservation, rid: ID):
        """
        Unregisters the reservation with the actor. The reservation's database record will not be removed.

        Args:
            reservation: reservation
            rid: reservation id
        Raises:
            Exception in case of error
        """
        raise NotImplementedError( "Should have implemented this" )