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
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates, JoinState
from fabric_cf.actor.core.kernel.slice_state_machine import SliceState
from fabric_cf.actor.core.time.actor_clock import ActorClock


class ResponseBuilder:
    RESPONSE_STATUS = "status"
    STATUS_OK = "OK"
    STATUS_FAILURE = "FAILURE"

    RESPONSE_RESERVATIONS = "reservations"
    RESPONSE_SLICES = "slices"
    RESPONSE_MESSAGE = "message"
    RESPONSE_BQM = "bqm"
    RESPONSE_SLICE_MODEL = "slice_model"

    PROP_SLICE_ID = "slice_id"
    PROP_SLICE_NAME = "slice_name"
    PROP_SLICE_STATE = "slice_state"
    PROP_RESERVATION_ID = "reservation_id"
    PROP_RESOURCE_TYPE = "resource_type"
    PROP_RESERVATION_STATE = "reservation_state"
    PROP_LEASE_END_TIME = "lease_end"
    PROP_RESERVATION_PENDING_STATE = "pending_state"
    PROP_RESERVATION_JOIN_STATE = "join_state"
    PROP_NOTICES = "notices"
    PROP_GRAPH_NODE_ID = "graph_node_id"
    PROP_GRAPH_ID = "graph_id"
    PROP_NAME = "name"
    PROP_MANAGEMENT_IP = "management_ip"
    PROP_SITE = "site"
    PROP_CAPACITIES = "capacities"
    PROP_ALLOCATED_CAPACITIES = "allocated_capacities"
    PROP_LABELS = "labels"
    PROP_ALLOCATED_LABELS = "allocated_labels"
    PROP_CAPACITY_HINTS = "capacity_hints"

    @staticmethod
    def get_reservation_summary(*, res_list: List[ReservationMng], include_notices: bool = False,
                                include_sliver: bool = False) -> dict:
        """
        Get Reservation summary
        :param res_list:
        :param include_notices:
        :param include_sliver:
        :return:
        """
        reservations = []
        status = ResponseBuilder.STATUS_OK
        message = ""

        if res_list is not None:
            for reservation in res_list:
                res_dict = {ResponseBuilder.PROP_SLICE_ID: reservation.get_slice_id(),
                            ResponseBuilder.PROP_RESERVATION_ID: reservation.get_reservation_id(),
                            ResponseBuilder.PROP_RESERVATION_STATE: ReservationStates(reservation.get_state()).name}

                if reservation.get_pending_state() is not None:
                    res_dict[ResponseBuilder.PROP_RESERVATION_PENDING_STATE] = \
                        ReservationPendingStates(reservation.get_pending_state()).name

                if isinstance(reservation, LeaseReservationAvro) and reservation.get_join_state() is not None:
                    res_dict[ResponseBuilder.PROP_RESERVATION_JOIN_STATE] = JoinState(reservation.get_join_state()).name

                sliver = reservation.get_sliver()
                if sliver is not None:
                    res_dict[ResponseBuilder.PROP_GRAPH_NODE_ID] = sliver.node_id
                    if include_sliver:
                        res_dict = ResponseBuilder.get_sliver_json(sliver=sliver, result=res_dict)

                if include_notices:
                    res_dict[ResponseBuilder.PROP_NOTICES] = reservation.get_notices()

                if reservation.get_end() is not None:
                    end_time = ActorClock.from_milliseconds(milli_seconds=reservation.get_end())
                    res_dict[ResponseBuilder.PROP_LEASE_END_TIME] = end_time.strftime(Constants.RENEW_TIME_FORMAT)

                reservations.append(res_dict)
        else:
            status = ResponseBuilder.STATUS_FAILURE
            message = "No reservations were found/computed"

        response = {ResponseBuilder.RESPONSE_STATUS: status,
                    ResponseBuilder.RESPONSE_MESSAGE: message,
                    ResponseBuilder.RESPONSE_RESERVATIONS: reservations}

        return response

    @staticmethod
    def get_slice_summary(*, slice_list: List[SliceAvro], slice_id: str = None,
                          slice_states: List[SliceState] = None) -> dict:
        """
        Get slice summary
        :param slice_list:
        :param slice_id:
        :param slice_states:
        :return:
        """
        slices = []
        status = ResponseBuilder.STATUS_OK
        message = ""

        if slice_list is not None:
            for s in slice_list:
                slice_state = SliceState(s.get_state())
                if slice_id is None and slice_states is not None and slice_state not in slice_states:
                    continue
                s_dict = {ResponseBuilder.PROP_SLICE_ID: s.get_slice_id(),
                          ResponseBuilder.PROP_SLICE_NAME: s.get_slice_name(),
                          ResponseBuilder.PROP_GRAPH_ID: s.get_graph_id(),
                          ResponseBuilder.PROP_SLICE_STATE: slice_state.name,
                          }
                end_time = s.get_lease_end()
                if end_time is not None:
                    s_dict[ResponseBuilder.PROP_LEASE_END_TIME] = end_time.strftime(Constants.RENEW_TIME_FORMAT)
                slices.append(s_dict)
        else:
            status = ResponseBuilder.STATUS_FAILURE
            message = "No slices were found"

        response = {ResponseBuilder.RESPONSE_STATUS: status,
                    ResponseBuilder.RESPONSE_MESSAGE: message,
                    ResponseBuilder.RESPONSE_SLICES: slices}

        return response

    @staticmethod
    def get_broker_query_model_summary(*, bqm: str):
        """
        Get BQM
        :param bqm:
        :return:
        """
        status = ResponseBuilder.STATUS_OK
        message = ""

        if bqm is None:
            message = "No broker query model found"
            status = ResponseBuilder.STATUS_FAILURE

        response = {ResponseBuilder.RESPONSE_STATUS: status,
                    ResponseBuilder.RESPONSE_MESSAGE: message,
                    ResponseBuilder.RESPONSE_BQM: bqm}

        return response

    @staticmethod
    def get_slice_model_summary(*, slice_model: str):
        """
        Get Slice Model summary
        :param slice_model:
        :return:
        """
        status = ResponseBuilder.STATUS_OK
        message = ""

        if slice_model is None:
            message = "No slice model found"
            status = ResponseBuilder.STATUS_FAILURE

        response = {ResponseBuilder.RESPONSE_STATUS: status,
                    ResponseBuilder.RESPONSE_MESSAGE: message,
                    ResponseBuilder.RESPONSE_SLICE_MODEL: slice_model}

        return response

    @staticmethod
    def get_sliver_json(*, sliver: BaseSliver, result: dict) -> dict:
        result[ResponseBuilder.PROP_NAME] = sliver.get_name()
        result[ResponseBuilder.PROP_SITE] = sliver.get_site()
        result[ResponseBuilder.PROP_RESOURCE_TYPE] = str(sliver.get_type())
        if sliver.get_capacities() is not None:
            result[ResponseBuilder.PROP_CAPACITIES] = sliver.get_capacities().to_json()
        if sliver.get_capacity_allocations() is not None:
            result[ResponseBuilder.PROP_ALLOCATED_CAPACITIES] = sliver.get_capacity_allocations().to_json()
        if sliver.get_labels() is not None:
            result[ResponseBuilder.PROP_LABELS] = sliver.get_labels().to_json()
        if sliver.get_label_allocations() is not None:
            result[ResponseBuilder.PROP_ALLOCATED_LABELS] = sliver.get_label_allocations().to_json()

        # Network Node Specific Fields
        if isinstance(sliver, NodeSliver):
            if sliver.get_management_ip() is not None:
                result[ResponseBuilder.PROP_MANAGEMENT_IP] = sliver.get_management_ip()
            if sliver.get_capacity_hints() is not None:
                result[ResponseBuilder.PROP_CAPACITY_HINTS] = sliver.get_capacity_hints().to_json()

        # Network Service Specific Fields
        if isinstance(sliver, NetworkServiceSliver):
            print("TODO")

        return result

    @staticmethod
    def get_response_summary(*, rid_list: List[str]) -> dict:
        response = {}
        status = ResponseBuilder.STATUS_OK
        message = ""
        if rid_list is not None and len(rid_list) > 0:
            status = ResponseBuilder.STATUS_FAILURE
            response[ResponseBuilder.RESPONSE_RESERVATIONS] = rid_list

        response[ResponseBuilder.RESPONSE_STATUS] = status
        response[ResponseBuilder.RESPONSE_MESSAGE] = message

        return response
