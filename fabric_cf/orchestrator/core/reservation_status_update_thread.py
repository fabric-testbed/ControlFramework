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

from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.iterable_queue import IterableQueue
from fabric_cf.orchestrator.core.active_status_checker import ActiveStatusChecker
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.i_status_update_callback import IStatusUpdateCallback
from fabric_cf.orchestrator.core.status_checker import StatusChecker, Status
from fabric_cf.orchestrator.core.watch_entry import WatchEntry


class ReservationStatusUpdateThread:
    """
    This thread allows expressing interest in completion of certain reservations and can run callbacks on other specified
    reservations when the status changes accordingly

    The periodic thread polls reservations of interest to determine whether the they have reached the required state,
    upon which the callback is invoked
    """
    MODIFY_CHECK_PERIOD = 5 # seconds

    def __init__(self):
        self.controller = None
        self.thread_lock = threading.Lock()
        self.reservation_lock = threading.Lock()
        self.active_watch = queue.Queue()
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
        while not self.stopped_worker.wait(timeout=self.MODIFY_CHECK_PERIOD):
            self.run()
        self.logger.debug("Reservation Status Update Thread exited")

    def __add_active_status_watch(self, *, we: WatchEntry):
        try:
            self.reservation_lock.acquire()
            self.logger.debug(f"Added watch entry {we}")
            self.active_watch.put_nowait(we)
        finally:
            self.reservation_lock.release()

    def add_active_status_watch(self, *, watch: ID, callback: IStatusUpdateCallback):
        """
        Watch for transition to Active or Failed. Callback is called when reservation has either Failed or became Active
        @param watch - reservation to watch
        @param callback - callback object
        """
        if watch is None or callback is None:
            self.logger.info(f"watch {watch} callback {callback}, ignoring")
            return

        self.__add_active_status_watch(we=WatchEntry(watch=watch, callback=callback))

    def process_watch_list(self, *, watch_list: List[WatchEntry], watch_type: str, status_checker: StatusChecker):
        """
        Process watch list
        :param watch_list:
        :param watch_type:
        :param status_checker:
        :return:
        """
        to_remove = []
        self.logger.debug(f"Scanning {watch_type} watch list {len(watch_list)}")

        if self.controller is None:
            from fabric_cf.orchestrator.core.orchestrator_kernel import OrchestratorKernelSingleton
            self.controller = OrchestratorKernelSingleton.get().get_management_actor()

        for watch_entry in watch_list:
            status, reservation = status_checker.check(controller=self.controller, rid=watch_entry.reservation_to_watch)
            self.logger.info(f"Status------- {status} for {reservation.get_reservation_id()}")
            if status == Status.OK:
                to_remove.append(watch_entry)
                watch_entry.callback.success(controller=self.controller, reservation=reservation)
            elif status == Status.NOT_OK:
                to_remove.append(watch_entry)
                watch_entry.callback.failure(controller=self.controller, reservation=reservation)

        self.logger.debug(f"Removing {watch_type} entries from watch {len(to_remove)}")

        for we in to_remove:
            watch_list.remove(we)

        # Add the Not Ready entries back
        for we in watch_list:
            self.__add_active_status_watch(we=we)

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
            active_watch_list = []

            try:
                self.reservation_lock.acquire()
                if not self.active_watch.empty():
                    for a in IterableQueue(source_queue=self.active_watch):
                        active_watch_list.append(a)
            finally:
                self.reservation_lock.release()

            self.process_watch_list(watch_list=active_watch_list, watch_type="active",
                                    status_checker=ActiveStatusChecker())

        except Exception as e:
            self.logger.error(f"RuntimeException: {e} continuing")
            self.logger.error(traceback.format_exc())
