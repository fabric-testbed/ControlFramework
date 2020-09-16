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

from fabric.actor.core.apis.i_mgmt_actor import IMgmtActor
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.orchestrator.core.orchestrator_slice import OrchestratorSlice
from fabric.orchestrator.core.reservation_status_update_thread import ReservationStatusUpdateThread
from fabric.orchestrator.core.slice_defer_thread import SliceDeferThread
from fabric.message_bus.messages.reservation_mng import ReservationMng


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
        from fabric.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.controller = GlobalsSingleton.get().get_container().get_management_actor()

    def set_broker(self, broker: str):
        self.broker = broker

    def get_broker(self) -> str:
        return self.broker

    def get_sut(self) -> ReservationStatusUpdateThread:
        return self.sut

    def get_sdt(self) -> SliceDeferThread:
        return self.sdt

    def get_management_actor(self) -> IMgmtActor:
        return self.controller

    def stop_threads(self):
        if self.sdt is not None:
            self.sdt.stop()

        if self.sut is not None:
            self.sut.stop()

    def start_threads(self):
        self.logger.debug("Starting Slice Defer Thread")
        self.sdt = SliceDeferThread()
        self.sdt.start()

        self.logger.debug("Starting ReservationStatusUpdateThread")
        self.sut = ReservationStatusUpdateThread()
        self.sut.start()

    def add_slice(self, controller_slice: OrchestratorSlice):
        if controller_slice is not None:
            try:
                self.lock.acquire()
                self.slices[controller_slice.get_slice_id()] = controller_slice
            finally:
                self.lock.release()

    def remove_slice(self, controller_slice: OrchestratorSlice):
        if controller_slice is not None:
            try:
                self.lock.acquire()
                if controller_slice.get_slice_id() in self.slices:
                    self.slices.pop(controller_slice.get_slice_id())

            finally:
                self.lock.release()

    def get_slice(self, slice_id: ID) -> OrchestratorSlice:
        if slice_id is None:
            return None

        try:
            self.lock.acquire()
            return self.slices.get(slice_id, None)
        finally:
            self.lock.release()

    def close_dead_slices(self):
        remove_slices = []
        try:
            self.lock.acquire()
            for slice_id, slice_obj in self.slices.items():
                if slice_obj is None:
                    continue
                try:
                    last_attempt =  slice_obj.get_delete_attempt()
                    self.logger.debug("Slice {}/{} is_dead_or_closing: {} last_delete_attempt: {}".
                                      format(slice_obj.get_slice_urn(), slice_id,
                                             slice_obj.is_dead_or_closing(), last_attempt))
                    if slice_obj.is_dead_or_closing():
                        if slice_obj.all_failed():
                            if last_attempt is not None:
                                delta_ms = Term.delta(last_attempt, datetime.utcnow())
                                if delta_ms >= self.DELETE_TIMEOUT:
                                    self.logger.debug("Deleting all failed slice {}/{} after 24 hours".format(
                                        slice_obj.get_slice_urn(), slice_id))
                                    remove_slices.append(slice_obj)
                            else:
                                slice_obj.mark_delete_attempt()
                        else:
                            self.logger.debug("Deleting slice {}/{}".format(slice_obj.get_slice_urn(), slice_id))
                            remove_slices.append(slice_obj)
                except Exception as e:
                    self.logger.error("Slice {}/{} owned by {} has encountered a state "
                                      "transition problem at garbage collection".format(slice_obj.get_slice_urn(),
                                                                                        slice_id,
                                                                                        slice_obj.get_user_dn()))
            for slice_obj in remove_slices:
                try:
                    slice_obj.lock()
                    all_res = slice_obj.get_all_reservations(self.controller)
                    if all_res is not None:
                        for res in all_res:
                            self.release_address_assignment(res)
                            self.mark_failed_missed_tag(res)
                    self.remove_slice(slice_obj)
                except Exception as e:
                    self.logger.error("Exception occurred in slice cleanup e={}".format(e))
                finally:
                    slice_obj.unlock()
        except Exception as e:
            self.logger.error("Unable to close slices due to e={}".format(e))
        finally:
            self.lock.release()

    def release_address_assignment(self, reservation: ReservationMng):
        # TODO
        return

    def mark_failed_missed_tag(self, reservation: ReservationMng):
        # TODO
        return

    def get_slices(self, user_dn: str) -> list:
        if user_dn is None:
            return None

        result = []
        try:
            self.lock.acquire()
            for slice_id, slice_obj in self.slices.items():
                if slice_obj is not None and slice_obj.get_user_dn() == user_dn:
                    result.append(slice_id)
        finally:
            self.lock.release()

        return result

    def recover(self):
        # TODO
        return

    def recover_slice(self, controller: IMgmtActor, slice_id: str, slice_name: str, user_dn: str, ssh_credentials: list):
        # TODO
        return


class OrchestratorStateSingleton:
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise Exception("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = OrchestratorState()
        return self.__instance

    get = classmethod(get)