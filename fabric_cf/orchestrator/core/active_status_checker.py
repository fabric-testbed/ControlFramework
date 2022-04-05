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
from typing import Tuple

from fabric_mb.message_bus.messages.reservation_mng import ReservationMng

from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.util.id import ID
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.status_checker import StatusChecker, Status


class ActiveStatusChecker(StatusChecker):
    def check(self, *, controller: ABCMgmtControllerMixin, rid: ID) -> Tuple[Status, ReservationMng or None]:
        reservation = None
        try:
            reservations = controller.get_reservations(rid=rid)
            units = controller.get_reservation_units(rid=rid)

            if reservations is None or len(reservations) == 0:
                raise OrchestratorException("Unable to obtain reservation information for {}".format(rid))
            reservation = next(iter(reservations))

            res_state = ReservationStates(reservation.get_state())
            self.logger.debug(f"------State --- {res_state}     {units}")

            if res_state == ReservationStates.Active:
                if units is None or len(units) == 0:
                    return Status.NOT_READY, reservation
                else:
                    return Status.OK, reservation
            elif res_state == ReservationStates.Closed:
                return Status.NOT_OK, reservation
            elif res_state == ReservationStates.Failed:
                return Status.NOT_OK, reservation
        except Exception as e:
            self.logger.error("Exception occurred e: {}".format(e))
        return Status.NOT_READY, reservation
