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

from datetime import datetime
from typing import List

from fabric_mb.message_bus.messages.reservation_mng import ReservationMng

from fabric_cf.actor.core.apis.i_mgmt_controller import IMgmtController
from fabric_cf.actor.core.manage.management_utils import ManagementUtils
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_slice_wrapper import OrchestratorSliceWrapper
from fabric_cf.orchestrator.core.reservation_status_update_thread import ReservationStatusUpdateThread
from fabric_cf.orchestrator.core.slice_defer_thread import SliceDeferThread


class OrchestratorState:
    DELETE_TIMEOUT = 24 * 3600 * 1000

    def __init__(self):
        self.lock = threading.Lock()
        self.used_mac = set()
        self.slices = {}
        self.sdt = None
        self.sut = None
        self.broker = None
        self.logger = None
        self.controller = None

    def set_broker(self, *, broker: ID):
        self.broker = broker

    def get_broker(self) -> ID:
        return self.broker

    def get_sut(self) -> ReservationStatusUpdateThread:
        return self.sut

    def get_sdt(self) -> SliceDeferThread:
        return self.sdt

    def get_logger(self):
        if self.logger is None:
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            self.logger = GlobalsSingleton.get().get_logger()
        return self.logger

    def get_management_actor(self) -> IMgmtController:
        if self.controller is None:
            self.controller = ManagementUtils.get_local_actor()
        return self.controller

    def stop_threads(self):
        if self.sdt is not None:
            self.sdt.stop()

        if self.sut is not None:
            self.sut.stop()

    def start_threads(self):
        self.get_logger().debug("Starting Slice Defer Thread")
        self.sdt = SliceDeferThread()
        self.sdt.start()

        self.get_logger().debug("Starting ReservationStatusUpdateThread")
        self.sut = ReservationStatusUpdateThread()
        self.sut.start()


class OrchestratorStateSingleton:
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise OrchestratorException("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = OrchestratorState()
        return self.__instance

    get = classmethod(get)