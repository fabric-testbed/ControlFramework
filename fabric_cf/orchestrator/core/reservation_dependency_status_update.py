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
from typing import List

from fabric_cf.actor.core.util.id import ID
from fabric_cf.orchestrator.core.i_status_update_callback import IStatusUpdateCallback
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng


class ReservationDependencyStatusUpdate(IStatusUpdateCallback):
    def __init__(self):
        self.reservation = None

    def success(self, *, ok: List[ID], act_on: List[ID]):
        """
        Success callback
        :param ok:
        :param act_on:
        :return:
        """
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        logger = GlobalsSingleton.get().get_logger()
        # TODO

        logger.debug("Success")

    def failure(self, *, failed: List[ID], ok: List[ID], act_on: List[ID]):
        """
        Failure callback
        :param failed:
        :param ok:
        :param act_on:
        :return:
        """
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        logger = GlobalsSingleton.get().get_logger()

        logger.debug("Failure")
        # TODO

    def get_reservation(self) -> ReservationMng:
        """
        Get Reservation
        :return:
        """
        return self.reservation

    def set_reservation(self, *, reservation: ReservationMng):
        """
        Set reservation
        :param reservation:
        :return:
        """
        self.reservation = reservation
