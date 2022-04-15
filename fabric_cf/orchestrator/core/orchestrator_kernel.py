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
import threading
import traceback

from fim.user import GraphFormat

from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.manage.management_utils import ManagementUtils
from fabric_cf.actor.core.util.id import ID
from fabric_cf.orchestrator.core.bqm_wrapper import BqmWrapper
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_slice_wrapper import OrchestratorSliceWrapper
from fabric_cf.orchestrator.core.reservation_status_update_thread import ReservationStatusUpdateThread


class OrchestratorKernel:
    """
    Class responsible for starting Orchestrator Threads; also holds Management Actor and Broker information
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.sut = None
        self.broker = None
        self.logger = None
        self.controller = None
        self.lock = threading.Lock()
        self.bqm_cache = {}
        
    def get_saved_bqm(self, *, graph_format: GraphFormat) -> BqmWrapper:
        """
        Get Saved BQM from cache
        """
        try:
            self.lock.acquire()
            saved_bqm = self.bqm_cache.get(graph_format, None)
            return saved_bqm
        finally:
            self.lock.release()

    def save_bqm(self, *, bqm: str, graph_format: GraphFormat):
        try:
            self.lock.acquire()
            saved_bqm = self.bqm_cache.get(graph_format, None)
            if saved_bqm is None:
                from fabric_cf.actor.core.container.globals import GlobalsSingleton
                refresh_interval = GlobalsSingleton.get().get_config().get_global_config().get_bqm_config().get(
                    Constants.REFRESH_INTERVAL, None)
                saved_bqm = BqmWrapper()
                saved_bqm.set_refresh_interval(refresh_interval=int(refresh_interval))
            saved_bqm.save(bqm=bqm, graph_format=graph_format)
            self.bqm_cache[graph_format] = saved_bqm
        finally:
            self.lock.release()

    def demand_slice(self, *, controller_slice: OrchestratorSliceWrapper):
        """
        Demand slice reservations.
        :param controller_slice:
        """
        computed_reservations = controller_slice.get_computed_reservations()

        try:
            self.lock.acquire()
            for reservation in computed_reservations:
                self.get_logger().debug(f"Issuing demand for reservation: {reservation.get_reservation_id()}")

                if reservation.get_state() != ReservationStates.Unknown.value:
                    self.get_logger().debug(f"Reservation not in {reservation.get_state()} state, ignoring it")
                    continue

                if not self.controller.demand_reservation(reservation=reservation):
                    raise OrchestratorException(f"Could not demand resources: {self.controller.get_last_error()}")
                self.get_logger().debug(f"Reservation #{reservation.get_reservation_id()} demanded successfully")
        except Exception as e:
            self.get_logger().error(traceback.format_exc())
            self.get_logger().error("Unable to get orchestrator or demand reservation: {}".format(e))
        finally:
            self.lock.release()

    def set_broker(self, *, broker: ID):
        """
        Set Broker
        :param broker:
        :return:
        """
        self.broker = broker

    def get_broker(self) -> ID:
        """
        Get Broker
        :return:
        """
        return self.broker

    def get_sut(self) -> ReservationStatusUpdateThread:
        """
        Get SUT thread
        :return:
        """
        return self.sut

    def get_logger(self):
        """
        Get logger
        :return:
        """
        if self.logger is None:
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            self.logger = GlobalsSingleton.get().get_logger()
        return self.logger

    def get_management_actor(self) -> ABCMgmtControllerMixin:
        """
        Get Management actor
        :return:
        """
        if self.controller is None:
            self.controller = ManagementUtils.get_local_actor()
        return self.controller

    def stop_threads(self):
        """
        Stop threads
        :return:
        """
        if self.sut is not None:
            self.sut.stop()

    def start_threads(self):
        """
        Start threads
        :return:
        """
        self.get_logger().debug("Starting ReservationStatusUpdateThread")
        self.sut = ReservationStatusUpdateThread()
        self.sut.start()


class OrchestratorKernelSingleton:
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise OrchestratorException("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = OrchestratorKernel()
        return self.__instance

    get = classmethod(get)