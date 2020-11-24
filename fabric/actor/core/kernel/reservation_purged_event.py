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
from fabric.actor.core.apis.i_event import IEvent
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.util.id import ID


class ReservationPurgedEvent(IEvent):
    """
    Represents Reservation purged RPC event
    """
    def __init__(self, *, reservation: IReservation):
        self.actor_id = reservation.get_actor().get_guid()
        self.rid = reservation.get_reservation_id()
        self.slice_id = reservation.get_slice_id()

    def get_actor_id(self) -> ID:
        """
        Get Actor id
        @return actor id
        """
        return self.actor_id

    def get_reservation_id(self) -> ID:
        """
        Get reservation id
        @return reservation id
        """
        return self.rid

    def get_slice_id(self) -> ID:
        """
        Get Slice id
        @return slice id
        """
        return self.slice_id

    def get_properties(self) -> dict:
        return None
