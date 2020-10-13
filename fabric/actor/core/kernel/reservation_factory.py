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
import pickle

from fabric.actor.core.apis.i_actor import IActor
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.apis.i_slice import ISlice
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.util.id import ID


class ReservationFactory:
    @staticmethod
    def create_instance(*, properties: dict, actor: IActor, slice_obj: ISlice, logger) -> IReservation:
        """
        Creates and initializes a new reservation from a saved
        properties list.

        @param properties properties dict

        @return reservation instance

        @throws Exception in case of error
        """
        ## TODO
        if Constants.PropertyPickleProperties not in properties:
            raise Exception("Invalid arguments")

        serialized_reservation = properties[Constants.PropertyPickleProperties]
        deserialized_reservation = None
        try:
            deserialized_reservation = pickle.loads(serialized_reservation)
            deserialized_reservation.restore(actor=actor, slice_obj=slice_obj, logger=logger)
        except Exception as e:
            raise e
        return deserialized_reservation

    @staticmethod
    def get_reservation_id(*, properties: dict) -> ID:
        """
        Extracts the reservation identifier from the properties list.

        @param properties properties list

        @return reservation identifier

        @throws Exception if the properties list does not contain a reservation
                identifier
        """
        return ID(id=properties[Constants.PropertyReservationID])

    @staticmethod
    def get_slice_id(*, properties: dict) -> int:
        """
        Extracts the slice id from the properties list.

        @param properties properties list

        @return slice id

        @throws Exception if the properties list does not contain a slice name
        """
        slice_id = properties.get(Constants.PropertyReservationSliceId, None)
        if slice_id is not None:
            return int(slice_id)
        return None

    @staticmethod
    def get_delegation_slice_id(*, properties: dict) -> int:
        """
        Extracts the slice id from the properties list.

        @param properties properties list

        @return slice id

        @throws Exception if the properties list does not contain a slice name
        """
        slice_id = properties.get(Constants.PropertyDelegationSliceId, None)
        if slice_id is not None:
            return int(slice_id)
        return None