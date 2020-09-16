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

from fabric.actor.core.apis.i_mgmt_actor import IMgmtActor
from fabric.orchestrator.core.active_status_checker import ActiveStatusChecker
from fabric.orchestrator.core.i_status_update_callback import IStatusUpdateCallback
from fabric.orchestrator.core.modify_status_checker import ModifyStatusChecker
from fabric.orchestrator.core.status_checker import StatusChecker, Status
from fabric.orchestrator.core.watch_entry import WatchEntry, TriggeredWatchEntry


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
        self.lock = threading.Lock()
        self.active_watch = []
        self.modify_watch = []
        from fabric.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()

        self.thread = None
        self.stopped_worker = threading.Event()

    def start(self):
        self.thread = threading.Thread(target=self.periodic())
        self.thread.setName('ReservationStatusUpdateThread')
        self.thread.setDaemon(True)
        self.thread.start()

    def stop(self):
        self.stopped_worker.set()
        self.thread.join()

    def periodic(self):
        while not self.stopped_worker.wait(timeout=self.get_period()):
            self.run()

    def add_active_status_watch(self, watch: list, act: list, callback: IStatusUpdateCallback):
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
            self.lock.acquire()
            self.active_watch.append(WatchEntry(watch, act, callback))
        finally:
            self.lock.release()

    def add_modify_status_watch(self, watch, act, callback: IStatusUpdateCallback):
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
            self.lock.acquire()
            self.modify_watch.append(WatchEntry(watch, act, callback))
        finally:
            self.lock.release()

    def check_watch_entry(self, controller: IMgmtActor, watch_entry: WatchEntry,
                          status_checker: StatusChecker) -> TriggeredWatchEntry:

        ok = []
        notok = []

        ready = True
        for rid in watch_entry.watch:
            status = status_checker.check(controller, rid, ok, notok)
            if status == Status.NOTREADY:
                ready = False

        if not ready:
            self.logger.debug("Reservation watch not ready for reservations {}".format(watch_entry.watch))
            return None

        return TriggeredWatchEntry(watch_entry.watch, watch_entry.act, watch_entry.callback, ok, notok)

    def process_callback(self, watch_entry: TriggeredWatchEntry):
        if len(watch_entry.not_ok) == 0:
            self.logger.debug("Invoking success callback for reservations: {}".format(watch_entry.watch))
            watch_entry.callback.success(watch_entry.ok, watch_entry.act)
        else:
            self.logger.debug("Invoking failure callback for reservations: {}".format(watch_entry.watch))
            watch_entry.callback.failure(watch_entry.not_ok, watch_entry.ok, watch_entry.act)

    def process_watch_list(self, controller: IMgmtActor, watch_list: list, watch_type: str,
                           status_checker: StatusChecker):
        to_remove = []
        self.logger.debug("Scanning {} watch list".format(watch_type))
        to_process = []
        try:
            self.lock.acquire()
            for watch_entry in watch_list:
                twe = self.check_watch_entry(controller, watch_entry, status_checker)
                if twe is not None:
                    to_process.append(twe)
                    to_remove.append(twe)
            self.logger.debug("Removing {} entries from watch {}".format(watch_type, len(to_remove)))

            for we in to_remove:
                watch_list.remove(we)
        finally:
            self.lock.release()

        self.logger.debug("Processing {} triggered {} callbacks".format(len(to_process), watch_type))
        for we in to_process:
            try:
                self.process_callback(we)
            except Exception as e:
                self.logger.error("Triggered {} watch entry for reservations {} returned with callback exception e: {}".
                                  format(watch_type, we.watch, e))

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
            from fabric.orchestrator.core.orchestrator_state import ControllerStateSingleton
            controller = ControllerStateSingleton.get().get_management_actor()
            self.process_watch_list(controller, self.active_watch, "active", ActiveStatusChecker())
            self.process_watch_list(controller, self.modify_watch, "modify", ModifyStatusChecker())
        except Exception as e:
            self.logger.error("RuntimeException: {} continuing".format(e))

    @staticmethod
    def get_period() -> int:
        return ReservationStatusUpdateThread.MODIFY_CHECK_PERIOD
