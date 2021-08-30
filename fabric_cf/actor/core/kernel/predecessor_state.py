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
from fabric_cf.actor.core.apis.abc_kernel_controller_reservation_mixin import ABCKernelControllerReservationMixin
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ReservationException


class PredecessorState:

    def __init__(self, *, reservation: ABCKernelControllerReservationMixin, filters: dict = None):
        if reservation is None:
            raise ReservationException(Constants.INVALID_ARGUMENT)
        self.reservation_id = None
        self.reservation = reservation
        if reservation is not None:
            self.reservation_id = reservation.get_reservation_id()

    def set_reservation(self, reservation: ABCReservationMixin):
        self.reservation = reservation
        if reservation is not None:
            self.reservation_id = reservation.get_reservation_id()

    def get_reservation(self) -> ABCKernelControllerReservationMixin:
        return self.reservation

    def set_properties(self, config: dict):
        return

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['reservation']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.reservation = None
