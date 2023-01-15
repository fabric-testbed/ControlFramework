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
from fim.graph.abc_property_graph import ABCPropertyGraph

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates, JoinState
from fabric_cf.actor.core.kernel.slice_state_machine import SliceState
from fabric_cf.actor.core.time.actor_clock import ActorClock


class ResponseBuilder:
    PROP_SLICE_ID = "slice_id"
    PROP_NAME = "name"
    PROP_STATE = "state"
    PROP_PROJECT_ID = "project_id"
    PROP_PROJECT_NAME = "project_name"
    PROP_MODEL = "model"
    PROP_GRAPH_ID = "graph_id"
    PROP_LEASE_START_TIME = "lease_start_time"
    PROP_LEASE_END_TIME = "lease_end_time"

    PROP_SLIVER_ID = "sliver_id"
    PROP_PENDING_STATE = "pending_state"
    PROP_JOIN_STATE = "join_state"
    PROP_GRAPH_NODE_ID = "graph_node_id"
    PROP_SLIVER = "sliver"
    PROP_SLIVER_TYPE = "sliver_type"
    PROP_NOTICE = "notice"

    @staticmethod
    def get_reservation_summary(*, res_list: List[ReservationMng]) -> List[dict]:
        """
        Get Reservation summary
        :param res_list:
        :return:
        """
        reservations = []

        if res_list is not None:
            for reservation in res_list:
                res_dict = {ResponseBuilder.PROP_SLICE_ID: reservation.get_slice_id(),
                            ResponseBuilder.PROP_SLIVER_ID: reservation.get_reservation_id(),
                            ResponseBuilder.PROP_STATE: ReservationStates(reservation.get_state()).name}

                if reservation.get_pending_state() is not None:
                    res_dict[ResponseBuilder.PROP_PENDING_STATE] = \
                        ReservationPendingStates(reservation.get_pending_state()).name

                if isinstance(reservation, LeaseReservationAvro) and reservation.get_join_state() is not None:
                    res_dict[ResponseBuilder.PROP_JOIN_STATE] = JoinState(reservation.get_join_state()).name

                sliver = reservation.get_sliver()
                if sliver is not None:
                    res_dict[ResponseBuilder.PROP_GRAPH_NODE_ID] = sliver.node_id
                    res_dict[ResponseBuilder.PROP_SLIVER_TYPE] = type(sliver).__name__
                    res_dict[ResponseBuilder.PROP_SLIVER] = ABCPropertyGraph.sliver_to_dict(sliver)

                if reservation.get_start() is not None:
                    start_time = ActorClock.from_milliseconds(milli_seconds=reservation.get_start())
                    res_dict[ResponseBuilder.PROP_LEASE_START_TIME] = start_time.strftime(Constants.LEASE_TIME_FORMAT)

                if reservation.get_end() is not None:
                    end_time = ActorClock.from_milliseconds(milli_seconds=reservation.get_end())
                    res_dict[ResponseBuilder.PROP_LEASE_END_TIME] = end_time.strftime(Constants.LEASE_TIME_FORMAT)

                if reservation.get_notices() is not None:
                    res_dict[ResponseBuilder.PROP_NOTICE] = reservation.get_notices()

                reservations.append(res_dict)

        return reservations

    @staticmethod
    def get_slice_summary(*, slice_list: List[SliceAvro], slice_model: str = None) -> List[dict]:
        """
        Get slice summary
        :param slice_list:
        :param slice_model:
        :return:
        """
        slices = []

        if slice_list is None:
            return slices

        for s in slice_list:
            s_dict = {ResponseBuilder.PROP_SLICE_ID: s.get_slice_id(),
                      ResponseBuilder.PROP_NAME: s.get_slice_name(),
                      ResponseBuilder.PROP_GRAPH_ID: s.get_graph_id(),
                      ResponseBuilder.PROP_STATE: SliceState(s.get_state()).name,
                      }
            end_time = s.get_lease_end()
            if end_time is not None:
                s_dict[ResponseBuilder.PROP_LEASE_END_TIME] = end_time.strftime(Constants.LEASE_TIME_FORMAT)
            start_time = s.get_lease_start()
            if start_time is not None:
                s_dict[ResponseBuilder.PROP_LEASE_START_TIME] = start_time.strftime(Constants.LEASE_TIME_FORMAT)

            if s.get_project_id() is not None:
                s_dict[ResponseBuilder.PROP_PROJECT_ID] = s.get_project_id()

            if s.get_project_name() is not None:
                s_dict[ResponseBuilder.PROP_PROJECT_NAME] = s.get_project_name()

            if slice_model is not None:
                s_dict[ResponseBuilder.PROP_MODEL] = slice_model
            slices.append(s_dict)

        return slices

    @staticmethod
    def get_broker_query_model_summary(*, bqm: str):
        """
        Get BQM
        :param bqm:
        :return:
        """
        return {ResponseBuilder.PROP_MODEL: bqm}
