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
from fabric.actor.core.apis.IKernelControllerReservation import IKernelControllerReservation


class PredecessorState:
    PredecessorPrefix = "predecessor."

    def __init__(self, reservation: IKernelControllerReservation, filter: dict = None):
        if reservation is None:
            raise Exception("Invalid Arguments")
        self.reservation = reservation
        self.filter = filter

    def get_reservation(self) -> IKernelControllerReservation:
        return self.reservation

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['reservation']
        state['reservation_id'] = self.reservation.get_reservation_id()
        return state

    def __setstate__(self, state):
        reservation_id = state['reservation_id']
        # TODO setup reservation pointer
        del state['reservation_id']
        self.__dict__.update(state)