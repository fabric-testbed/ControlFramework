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
from fabric_cf.actor.core.apis.i_mgmt_actor import IMgmtActor
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.reservation_id_with_modify_index import ReservationIDWithModifyIndex
from fabric_cf.orchestrator.core.status_checker import StatusChecker, Status


class ModifyStatusChecker(StatusChecker):
    def __init__(self):
        super().__init__()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()

    def check_(self, *, controller: IMgmtActor, rid) -> Status:
        if not isinstance(rid, ReservationIDWithModifyIndex):
            return Status.NOTREADY

        try:
            reservation = controller.get_reservation(rid=rid.get_reservation_id())
            if reservation is None:
                raise OrchestratorException("Unable to obtain reservation information for {}".format(rid.get_reservation_id()))

            if reservation.get_state() != ReservationStates.Active or \
                    reservation.get_pending_state() != ReservationPendingStates.None_:
                self.logger.debug("Returning NOTREADY for reservation {} in state ({}, {})".format(
                    rid, reservation.get_state(), reservation.get_pending_state()))
                return Status.NOTREADY

            if reservation.get_state() == ReservationStates.Failed or \
                    reservation.get_pending_state() == ReservationStates.Closed:
                return Status.NOTOK

            units_list = controller.get_reservation_units(rid=rid.get_reservation_id())

            if len(units_list) == 0:
                raise OrchestratorException("No units associated with reservation {}".format(rid.get_reservation_id()))

            unit = units_list.__iter__().__next__()

            properties = unit.get_properties()
            code_property_name = Constants.unit_modify_prop_prefix + \
                                 rid.get_modify_index() + \
                                 Constants.unit_modify_prop_code_suffix

            message_property_name = Constants.unit_modify_prop_prefix + \
                                    rid.get_modify_index() + \
                                    Constants.unit_modify_prop_message_suffix
            modify_failed = False
            modify_error_message = None
            modify_error_code = 0

            for key, value in properties.items():
                if not modify_failed and code_property_name == key:
                    modify_error_code = int(value)
                    if modify_error_code == 0:
                        self.logger.info("Reservation {} was modified successfully for modify index {}".format(
                            rid.get_reservation_id(), rid.get_modify_index()))
                        return Status.OK
                    else:
                        modify_failed = True
                if message_property_name == key:
                    modify_error_message = value
            if modify_failed:
                self.logger.info("Reservation {} failed modify for modify index {} with code {} and message {}".format(
                    rid.get_reservation_id(), rid.get_modify_index(), modify_error_code, modify_error_message))
                return Status.NOTOK

        except Exception as e:
            self.logger.error("Exception occurred e: {}".format(e))
        return Status.NOTREADY
