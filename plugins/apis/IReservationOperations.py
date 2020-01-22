#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from plugins.apis import IReservation
from plugins.util import ReservationSet, ResourceSet


class IReservationOperations:
    """
    ReservationOperations</code> defines the core reservations management operations supported by each actor.
    """
    def fail(self, rid: str, message: str):
        """
        Fails the specified reservation.

        Args:
            rid: reservation id
            message: message
        Raises:
            Exception in case of error
        """
        return

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
        return

    def close(self, rid: str):
        """
        Closes the reservation. Note: the reservation must have already been registered with the actor.
        This method may involve either a client or a server side action or both. When called on a broker,
        this method will only close the broker reservation.

        Args:
            rid: reservation id
        Raises:
            Exception in case of error
        """
        return

    def close(self, reservations:ReservationSet):
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
        return

    def extend(self, reservation: IReservation, resources: ResourceSet, term):
        """
        Extends the reservation. Note: the reservation must have already been registered with the actor.
        This method may involve either a client or a server side action or both. When called on a broker,
        this method will extend the current ticket and send an update back to the client. When called on a
        site authority, this method will extend the lease and send the new lease back.

        Args:
            reservation: reservation to extend
            resources: resource set describing the resources desired for the extension
            term: term for extension (must extend the current term)
        Raises:
            Exception in case of error
        """
        return

    def extend(self, rid: str, resources: ResourceSet, term):
        """
        Extends the reservation. Note: the reservation must have already been registered with the actor.
        This method may involve either a client or a server side action or both. When called on a broker,
        this method will extend the current ticket and send an update back to the client. When called on a
        site authority, this method will extend the lease and send the new lease back.

        Args:
            rid: reservation id
            resources: resource set describing the resources desired for the extension
            term: term for extension (must extend the current term)
        Raises:
            Exception in case of error
        """
        return

    def get_reservation(self, rid: str):
        """
        Returns the specified reservation.

        Args:
            rid: reservation id
        Returns:
            reservation
        """
        return None

    def get_reservations(self, slice_id: str):
        """
        Returns all reservations in the given slice.

        Args:
            slice_id: slice id
        Returns:
            reservations
        """
        return None

    def register(self, reservation: IReservation):
        """
        Registers the reservation with the actor. The reservation must not have been previously
        registered with the actor: there should be no database record for the reservation.

        Args:
            reservation: reservation to register
        Raises:
            Exception in case of error
        """
        return

    def remove_reservation(self, reservation: IReservation):
        """
        Removes the specified reservation. Note: the reservation must have already been registered with the actor.
        This method will unregister the reservation and remove it from the underlying database.
        Only closed and failed reservations can be removed.

        Args:
            reservation: reservation
        Raises:
            Exception if an error occurs or when trying to remove a reservation that is neither failed or closed.
        """
        return

    def remove_reservation(self, rid: str):
        """
        Removes the specified reservation. Note: the reservation must have already been registered with the actor.
        This method will unregister the reservation and remove it from the underlying database.
        Only closed and failed reservations can be removed.

        Args:
            rid: reservation id
        Raises:
            Exception if an error occurs or when trying to remove a reservation that is neither failed or closed.
        """
        return

    def re_register(self, reservation: IReservation):
        """
        Registers a previously registered reservation with the actor. The reservation must have a database record.

        Args:
            reservation: reservation
        Raises:
            Exception in case of error
        """
        return

    def unregister(self, reservation: IReservation):
        """
        Unregisters the reservation with the actor. The reservation's database record will not be removed.

        Args:
            reservation: reservation
        Raises:
            Exception in case of error
        """
        return

    def unregister(self, rid: str):
        """
        Unregisters the reservation with the actor. The reservation's database record will not be removed.

        Args:
            rid: reservation id
        Raises:
            Exception in case of error
        """
        return