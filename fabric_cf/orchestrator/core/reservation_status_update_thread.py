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
import queue
import threading
import traceback
from typing import List

from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.iterable_queue import IterableQueue
from fabric_cf.orchestrator.core.active_status_checker import ActiveStatusChecker
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.i_status_update_callback import IStatusUpdateCallback
from fabric_cf.orchestrator.core.modify_status_checker import ModifyStatusChecker
from fabric_cf.orchestrator.core.status_checker import StatusChecker, Status
from fabric_cf.orchestrator.core.watch_entry import WatchEntry, TriggeredWatchEntry


class ReservationStatusUpdateThread:
    """
    This thread allows expressing interest in completion of certain reservations and can run callbacks on other specified
    reservations when the status changes accordingly

    Supports modify and more traditional ticketed-active

    Principle of operation: allows to watch the following state transitions and events: - to Active transitions on
    reservations - to Active transitions followed by OK unit status on modify

    The periodic thread polls reservations of interest to determine whether the they have reached the required state,
    upon which
    """
    MODIFY_CHECK_PERIOD = 5 # seconds

    def __init__(self):
        self.thread_lock = threading.Lock()
        self.reservation_lock = threading.Lock()
        self.active_watch = queue.Queue()
        self.modify_watch = queue.Queue()
        self.stopped_worker = threading.Event()

        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()

        self.thread = None
        self.stopped = False

    def start(self):
        """
        Start
        :return:
        """
        try:
            self.thread_lock.acquire()
            if self.thread is not None:
                raise OrchestratorException("This ReservationStatusUpdateThread has already been started")

            self.thread = threading.Thread(target=self.periodic)
            self.thread.setName(self.__class__.__name__)
            self.thread.setDaemon(True)
            self.thread.start()

        finally:
            self.thread_lock.release()

    def stop(self):
        """
        Stop
        :return:
        """
        self.stopped = True
        self.stopped_worker.set()
        try:
            self.thread_lock.acquire()
            temp = self.thread
            self.thread = None
            if temp is not None:
                self.logger.warning("It seems that the ReservationStatusUpdateThread is running. Interrupting it")
                try:
                    temp.join()
                except Exception as e:
                    self.logger.error("Could not join ReservationStatusUpdateThread thread {}".format(e))
                finally:
                    self.thread_lock.release()
        finally:
            if self.thread_lock is not None and self.thread_lock.locked():
                self.thread_lock.release()

    def periodic(self):
        """
        Periodic
        :return:
        """
        self.logger.debug("Reservation Status Update Thread started")
        while not self.stopped_worker.wait(timeout=self.get_period()):
            self.run()
        self.logger.debug("Reservation Status Update Thread exited")

    def add_active_status_watch(self, *, watch: List[ID], act: List[ID], callback: IStatusUpdateCallback):
        """
        Watch for transition to Active or Failed. Callback is called when ALL reservations in the watch list have reached
        Active or Failed or Closed state. If all reservations on the watch list went to Active or Closed, the success
        method of the callback is called If some or all reservations on the watch list went to Failed state, the failure
        method of the callback is called.
        @param watch - reservations to watch
        @param act - reservations to act on (can be null)
        @param callback - callback object
        """
        if watch is None or len(watch) == 0 or callback is None:
            self.logger.info("watch list is size 0 or callback is null, ignoring")
            return

        try:
            self.reservation_lock.acquire()
            self.active_watch.put_nowait(WatchEntry(watch=watch, rids=act, callback=callback))
        finally:
            self.reservation_lock.release()

    def add_modify_status_watch(self, *, watch, act, callback: IStatusUpdateCallback):
        """
        Watch for OK unit modify status (or not OK). Callback is called when ALL reservations in the watch list have
        reached some sort of modify status (OK or not OK) If all reservations on the watch list went to OK, the success
        method of the callback is called If some or all reservations on the watch list went to not OK, the failure method
        of the callback is called.
        @param watch - reservations to watch (with associated modify operation indices)
        @param act - reservations to act on (can be null)
        @param callback - callback
        """
        if watch is None or len(watch) == 0 or callback is None:
            self.logger.info("watch list is size 0 or callback is null, ignoring")
            return

        try:
            self.reservation_lock.acquire()
            self.modify_watch.put_nowait(WatchEntry(watch=watch, rids=act, callback=callback))
        finally:
            self.reservation_lock.release()

    def check_watch_entry(self, *, controller: ABCMgmtControllerMixin, watch_entry: WatchEntry,
                          status_checker: StatusChecker) -> TriggeredWatchEntry:
        """
        Check watch entry
        :param controller:
        :param watch_entry:
        :param status_checker:
        :return:
        """
        ok = []
        notok = []

        ready = True
        for rid in watch_entry.watch:
            status = status_checker.check(controller=controller, rid=rid, ok=ok, not_ok=notok)
            if status == Status.NOTREADY:
                ready = False

        if not ready:
            self.logger.debug("Reservation watch not ready for reservations {}".format(watch_entry.watch))
            return None

        return TriggeredWatchEntry(watch=watch_entry.watch, rids=watch_entry.act, callback=watch_entry.callback, ok=ok,
                                   no_ok=notok)

    def process_callback(self, *, watch_entry: TriggeredWatchEntry):
        """
        Process callback
        :param watch_entry:
        :return:
        """
        if len(watch_entry.not_ok) == 0:
            self.logger.debug("Invoking success callback for reservations: {}".format(watch_entry.watch))
            watch_entry.callback.success(watch_entry.ok, watch_entry.act)
        else:
            self.logger.debug("Invoking failure callback for reservations: {}".format(watch_entry.watch))
            watch_entry.callback.failure(watch_entry.not_ok, watch_entry.ok, watch_entry.act)

    def process_watch_list(self, *, controller: ABCMgmtControllerMixin, watch_list: List[WatchEntry], watch_type: str,
                           status_checker: StatusChecker):
        """
        Process watch list
        :param controller:
        :param watch_list:
        :param watch_type:
        :param status_checker:
        :return:
        """
        to_remove = []
        self.logger.debug("Scanning {} watch list".format(watch_type))

        to_process = []
        for watch_entry in watch_list:
            twe = self.check_watch_entry(controller=controller, watch_entry=watch_entry,
                                         status_checker=status_checker)
            if twe is not None:
                to_process.append(twe)
                to_remove.append(twe)
        self.logger.debug(f"Removing {watch_type} entries from watch {len(to_remove)}")

        for we in to_remove:
            watch_list.remove(we)

        self.logger.debug(f"Processing {len(to_process)} triggered {watch_type} callbacks")
        for we in to_process:
            try:
                self.process_callback(watch_entry=we)
            except Exception as e:
                self.logger.error(f"Triggered {watch_type} watch entry for reservations {we.watch} "
                                  f"returned with callback exception e: {e}")

    def run(self):
        # wake up periodically and check the status of reservations (state or unit modify properties)
        # and call appropriate callbacks if necessary.
        # scan both lists and check if any of the
        # reservation groups on them are ready for callbacks
        # remove those ready for callbacks off the lists and invoke callbacks
        # outside the critical section
        # NOTE: because callback can create another callback, which can add watches,
        # we must make this code re-entrant hence the need to change the watch list
        # and only then call the callbacks

        try:
            from fabric_cf.orchestrator.core.orchestrator_kernel import OrchestratorKernelSingleton
            controller = OrchestratorKernelSingleton.get().get_management_actor()
            active_watch_list = []
            modify_watch_list = []

            try:
                self.reservation_lock.acquire()
                if not self.active_watch.empty():
                    for a in IterableQueue(source_queue=self.active_watch):
                        active_watch_list.append(a)

                if not self.modify_watch.empty():
                    for m in IterableQueue(source_queue=self.modify_watch):
                        modify_watch_list.append(m)
            finally:
                self.reservation_lock.release()

            self.process_watch_list(controller=controller, watch_list=active_watch_list, watch_type="active",
                                    status_checker=ActiveStatusChecker())
            self.process_watch_list(controller=controller, watch_list=modify_watch_list, watch_type="modify",
                                    status_checker=ModifyStatusChecker())
        except Exception as e:
            self.logger.error(f"RuntimeException: {e} continuing")
            self.logger.error(traceback.format_exc())

    @staticmethod
    def get_period() -> int:
        return ReservationStatusUpdateThread.MODIFY_CHECK_PERIOD
