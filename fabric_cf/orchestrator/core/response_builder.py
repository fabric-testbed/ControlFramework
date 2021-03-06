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

from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.slice_avro import SliceAvro

from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates, JoinState
from fabric_cf.actor.core.kernel.slice_state_machine import SliceState


class ResponseBuilder:
    RESPONSE_STATUS = "status"
    SUCCESS_STATUS = "success"
    FAILURE_STATUS = "fail"

    RESPONSE_RESERVATIONS = "reservations"
    RESPONSE_SLICES = "slices"
    RESPONSE_MESSAGE = "message"
    RESPONSE_BQM = "bqm"
    RESPONSE_SLICE_MODEL = "slice_model"

    @staticmethod
    def get_reservation_summary(*, res_list: List[ReservationMng], include_notices: bool = False) -> dict:
        reservations = []
        status = ResponseBuilder.SUCCESS_STATUS
        message = ""

        if res_list is not None:
            for reservation in res_list:
                res_dict = {"slice_id": reservation.get_slice_id(), "reservation_id": reservation.get_reservation_id(),
                            "resource_type": reservation.get_resource_type(),
                            'state': ReservationStates(reservation.get_state()).name}

                if reservation.get_pending_state() is not None:
                    res_dict['pending_state'] = ReservationPendingStates(reservation.get_pending_state()).name

                if isinstance(reservation, LeaseReservationAvro) and reservation.get_join_state() is not None:
                    res_dict['join_state'] = JoinState(reservation.get_join_state()).name

                if include_notices:
                    res_dict['notices'] = reservation.get_notices()

                reservations.append(res_dict)
        else:
            status = ResponseBuilder.FAILURE_STATUS
            message = "No reservations were found/computed"

        response = {ResponseBuilder.RESPONSE_STATUS: status, ResponseBuilder.RESPONSE_MESSAGE: message,
                    ResponseBuilder.RESPONSE_RESERVATIONS: reservations}

        return response

    @staticmethod
    def get_slice_summary(*, slice_list: List[SliceAvro], slice_id: str = None) -> dict:
        slices = []
        status = ResponseBuilder.SUCCESS_STATUS
        message = ""

        if slice_list is not None:
            for s in slice_list:
                slice_state = SliceState(s.get_state())
                if slice_id is None and (slice_state == SliceState.Dead or slice_state == SliceState.Closing):
                    continue
                s_dict = {'slice_id': s.get_slice_id(), 'slice_name': s.get_slice_name(),
                          'graph_id': s.get_graph_id(), 'slice_state': slice_state.name}
                slices.append(s_dict)
        else:
            status = ResponseBuilder.FAILURE_STATUS
            message = "No slices were found"

        response = {ResponseBuilder.RESPONSE_STATUS: status, ResponseBuilder.RESPONSE_MESSAGE: message,
                    ResponseBuilder.RESPONSE_SLICES: slices}

        return response

    @staticmethod
    def get_broker_query_model_summary(*, bqm: dict):
        status = ResponseBuilder.SUCCESS_STATUS
        message = ""

        if bqm is None:
            message = "No broker query model found"
            status = ResponseBuilder.FAILURE_STATUS

        response = {ResponseBuilder.RESPONSE_STATUS: status, ResponseBuilder.RESPONSE_MESSAGE: message,
                    ResponseBuilder.RESPONSE_BQM: bqm}

        return response

    @staticmethod
    def get_slice_model_summary(*, slice_model: str):
        status = ResponseBuilder.SUCCESS_STATUS
        message = ""

        if slice_model is None:
            message = "No slice model found"
            status = ResponseBuilder.FAILURE_STATUS

        response = {ResponseBuilder.RESPONSE_STATUS: status, ResponseBuilder.RESPONSE_MESSAGE: message,
                    ResponseBuilder.RESPONSE_SLICE_MODEL: slice_model}

        return response
