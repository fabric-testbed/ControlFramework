#!/usr/bin/env python
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################
import datetime

from plugins.apis.IReservation import IReservation
from plugins.util.ResourceCount import ResourceCount


class ReservationSet:
    """
    ReservationSet is a collection of reservations indexed by ReservationID
    """
    def __init__(self):
        self.reservations = {}

    def __int__(self, reservations: dict):
        self.reservations = reservations

    def add(self, reservation: IReservation):
        """
        Adds the reservation to the set

        Args:
            reservation: reservation to be added
        """
        self.reservations[reservation.get_reservation_id()] = reservation

    def clear(self):
        """
        Remove all the reservations from the set
        """
        self.reservations.clear()

    def contains(self, reservation: IReservation):
        """
        Checks if the reservation is part of the set

        Args:
            reservation: reservation to check
        Returns:
            true if the set contains the specified reservation; false otherwise
        """
        if reservation.get_reservation_id() in self.reservations :
            return True
        return False

    def contains(self, rid: str):
        """
        Checks if the reservation is part of the set

        Args:
            rid: reservation id
        Returns:
            true if the set contains the specified reservation; false otherwise
        """
        if rid in self.reservations:
            return True
        return False

    def count(self, rc: ResourceCount, when: datetime):
        """
        Tallies up resources in the ReservationSet. Note: "just a hint" unless kernel lock is held.

        Args:
            rc: holder for counts
            when: date relative to which to do the counting
        """
        for reservation in self.reservations.values():
            reservation.count(rc, when)

    def get(self, rid: str):
        """
        Retrieves a reservation from the set.

        Args:
            rid: reservation id
        Returns:
            Reservation identified by rid
        """
        return self.reservations.get(rid)

    def is_empty(self):
        """
        Checks if the set is empty.

        Returns:
            true if the set is empty
        """
        if len(self.reservations.keys()) == 0:
            return True
        return False

    def remove(self, reservation: IReservation):
        """
        Removes the specified reservation.

        Args:
            reservation: reservation to remove
        """
        self.reservations.pop(reservation.get_reservation_id())

    def size(self):
        """
        Returns the number of reservations in the set.

        Returns:
            the size of the reservation set
        """
        return len(self.reservations.keys())