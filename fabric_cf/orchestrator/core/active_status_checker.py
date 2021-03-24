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
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.util.id import ID
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.status_checker import StatusChecker, Status


class ActiveStatusChecker(StatusChecker):
    def __init__(self):
        super().__init__()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()

    def check_(self, *, controller: ABCMgmtControllerMixin, rid) -> Status:
        """
        Check status
        :param controller:
        :param rid:
        :return:
        """
        if not isinstance(rid, ID):
            return Status.NOTREADY

        try:
            reservations = controller.get_reservations(rid=rid)
            units = controller.get_reservation_units(rid=rid)

            if reservations is None or len(reservations) == 0:
                raise OrchestratorException("Unable to obtain reservation information for {}".format(rid))
            reservation = next(iter(reservations))

            if reservation.get_state() == ReservationStates.Active:
                if units is None or len(units) == 0:
                    return Status.NOTREADY
            elif reservation.get_state() == ReservationStates.Closed:
                return Status.OK
            elif reservation.get_state() == ReservationStates.Failed:
                return Status.NOTOK

        except Exception as e:
            self.logger.error("Exception occurred e: {}".format(e))
        return Status.NOTREADY
