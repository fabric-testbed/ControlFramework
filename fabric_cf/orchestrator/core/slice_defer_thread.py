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
from datetime import datetime

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.time.term import Term
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_slice_wrapper import OrchestratorSliceWrapper


class SliceDeferThread:
    """
    This runs as a standalone thread started by OrchestratorState and deals with slices that have to wait for other
    slices to complete.
    """
    THREAD_SLEEP_TIME = 10000
    DEFAULT_MAX_CREATE_TIME_IN_MS = 600000

    def __init__(self):
        self.deferred_slices = queue.Queue()
        self.defer_slice_avail_condition = threading.Condition()
        self.thread_lock = threading.Lock()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()

        self.thread = None
        self.last_slice = None
        self.last_slice_time = None
        self.max_create_wait_time = 0
        self.stopped = False

        wait_time = GlobalsSingleton.get().get_config().get_runtime_config().get(
            Constants.PROPERTY_CONF_CONTROLLER_CREATE_WAIT_TIME_MS, None)

        if wait_time is None:
            self.max_create_wait_time = self.DEFAULT_MAX_CREATE_TIME_IN_MS
        else:
            self.max_create_wait_time = wait_time

    def queue_slice(self, *, controller_slice: OrchestratorSliceWrapper):
        """
        Queue a slice
        :param controller_slice:
        :return:
        """
        with self.defer_slice_avail_condition:
            self.deferred_slices.put_nowait(controller_slice)
            self.logger.debug(f"Added slice to deferred slices queue {controller_slice.__class__.__name__}")
            self.defer_slice_avail_condition.notify_all()

    def update_last(self, *, controller_slice: OrchestratorSliceWrapper):
        """
        Update the last slice
        :param controller_slice:
        :return:
        """
        if controller_slice is None:
            return

        self.logger.info(f"Updating last slice with: {controller_slice.get_slice_name()}/"
                         f"{controller_slice.get_slice_id()}")
        self.last_slice = controller_slice
        self.last_slice_time = datetime.utcnow()

    def process_slice(self, *, controller_slice: OrchestratorSliceWrapper):
        """
        Process a slice
        :param controller_slice:
        :return:
        """
        if controller_slice is None:
            return

        if controller_slice != self.last_slice and \
                self.check_computed_reservations(controller_slice=controller_slice) and \
                self.delay_not_done(controller_slice=self.last_slice):
            self.logger.info(f"Putting slice {controller_slice.get_slice_name()}/{controller_slice.get_slice_id()} "
                             f"on wait queue")

            self.queue_slice(controller_slice=controller_slice)
        else:
            self.logger.info(f"Processing slice {controller_slice.get_slice_name()}/{controller_slice.get_slice_id()} "
                             f"immediately")
            if self.check_computed_reservations(controller_slice=controller_slice):
                self.update_last(controller_slice=controller_slice)

            self.demand_slice(controller_slice=controller_slice)

    def start(self):
        """
        Start thread
        :return:
        """
        try:
            self.thread_lock.acquire()
            if self.thread is not None:
                raise OrchestratorException("This SliceDeferThread has already been started")

            self.thread = threading.Thread(target=self.run)
            self.thread.setName(self.__class__.__name__)
            self.thread.setDaemon(True)
            self.thread.start()

        finally:
            self.thread_lock.release()

    def stop(self):
        """
        Stop thread
        :return:
        """
        self.stopped = True
        try:
            self.thread_lock.acquire()
            temp = self.thread
            self.thread = None
            if temp is not None:
                self.logger.warning("It seems that the SliceDeferThread is running. Interrupting it")
                try:
                    # TODO find equivalent of interrupt
                    with self.defer_slice_avail_condition:
                        self.defer_slice_avail_condition.notify_all()
                    temp.join()
                except Exception as e:
                    self.logger.error(f"Could not join SliceDeferThread thread {e}")
                finally:
                    self.thread_lock.release()
        finally:
            if self.thread_lock is not None and self.thread_lock.locked():
                self.thread_lock.release()

    def run(self):
        """
        Thread main loop
        :return:
        """
        self.logger.debug("SliceDeferThread started")
        while True:
            if self.last_slice is not None:
                self.logger.info(f"Processing previously deferred slice {self.last_slice.get_slice_name()}/"
                                 f"{self.last_slice.get_slice_id()}")

                try:
                    self.last_slice.lock()
                    if self.delay_not_done(controller_slice=self.last_slice):
                        if Term.delta(self.last_slice_time, datetime.utcnow()) > self.max_create_wait_time:
                            self.logger.info(f"Maximum wait time exceeded for slice: {self.last_slice.get_slice_name()}/"
                                             f"{self.last_slice.get_slice_id()}, proceeding anyway")
                        else:
                            continue
                except Exception as e:
                    self.logger.error(f"Exception while checking slice {self.last_slice.get_slice_name()}/"
                                      f"{self.last_slice.get_slice_id()} e: {e}")
                    self.logger.error(traceback.format_exc())
                finally:
                    self.last_slice.unlock()

            controller_slice = None

            with self.defer_slice_avail_condition:

                while self.deferred_slices.empty() and not self.stopped:
                    try:
                        self.defer_slice_avail_condition.wait()
                    except InterruptedError as e:
                        self.logger.error(e)
                        self.logger.info("SliceDeferThread thread interrupted. Exiting")
                        return

                if self.stopped:
                    self.logger.info("SliceDeferThread exiting")
                    return

                if not self.deferred_slices.empty():
                    controller_slice = self.deferred_slices.get_nowait()

                self.defer_slice_avail_condition.notify_all()

            if controller_slice is None:
                continue

            self.logger.info(f"Performing demand on deferred slice {controller_slice.get_slice_name()}/"
                             f"{controller_slice.get_slice_id()}")

            self.update_last(controller_slice=controller_slice)
            try:
                controller_slice.lock()
                self.demand_slice(controller_slice=controller_slice)
            except Exception as e:
                self.logger.error(f"Exception while demanding slice: {self.last_slice.get_slice_name()}/"
                                  f"{self.last_slice.get_slice_id()} e: {e}")
            finally:
                controller_slice.unlock()

        self.logger.debug("SliceDeferThread exited")

    def demand_slice(self, *, controller_slice: OrchestratorSliceWrapper):
        """
        Demand slice
        :param controller_slice:
        :return:
        """
        if controller_slice is None:
            self.logger.error("demand slice was given a None slice")
            return

        computed_reservations = controller_slice.get_computed_reservations()

        if computed_reservations is None:
            return

        try:
            from fabric_cf.orchestrator.core.orchestrator_kernel import OrchestratorKernelSingleton
            controller = OrchestratorKernelSingleton.get().get_management_actor()
            for reservation in computed_reservations:
                self.logger.debug(f"Issuing demand for reservation: {reservation.get_reservation_id()}")

                if reservation.get_state() != ReservationStates.Unknown.value:
                    self.logger.debug(f"Reservation not in {reservation.get_state()} state, ignoring it")
                    continue

                if not controller.demand_reservation(reservation=reservation):
                    raise OrchestratorException(f"Could not demand resources: {controller.get_last_error()}")
                self.logger.debug(f"Reservation #{reservation.get_reservation_id()} demanded successfully")
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Unable to get orchestrator or demand reservation: {}".format(e))
            return

    def check_computed_reservations(self, *, controller_slice: OrchestratorSliceWrapper) -> bool:
        """
        Check computed reservations
        :param controller_slice:
        :return:
        """
        if controller_slice is None or controller_slice.get_computed_reservations() is None:
            self.logger.info("Empty slice or no computed reservations")
            return False

        return True

    def delay_not_done(self, *, controller_slice: OrchestratorSliceWrapper) -> bool:
        """
        Check if delay is done
        :param controller_slice:
        :return:
        """
        if controller_slice is None:
            return False

        self.logger.info("Checking slice {}/{}".format(controller_slice.get_slice_name(),
                                                       controller_slice.get_slice_id()))

        all_reservations = None

        try:
            all_reservations = controller_slice.get_all_reservations()
        except Exception as e:
            self.logger.error(f"Exception in delay_not_done for slice: {controller_slice.get_slice_name()}/"
                              f"{controller_slice.get_slice_id()} e: {e}")

        if all_reservations is None:
            self.logger.info(f"Slice: {controller_slice.get_slice_name()}/"
                             f"{controller_slice.get_slice_id()} has None reservations in delay_not_done")

            return self.check_computed_reservations(controller_slice=controller_slice)
        else:
            if len(all_reservations) <= 0:
                self.logger.info(f"Slice: {controller_slice.get_slice_name()}/{controller_slice.get_slice_id()} "
                                 f"has empty reservations in delay_not_done")
                return self.check_computed_reservations(controller_slice=controller_slice)

            for reservation in all_reservations:
                reservation_state = ReservationStates(reservation.get_state())
                if reservation_state != ReservationStates.Active and reservation_state != ReservationStates.Closed and \
                        reservation_state != ReservationStates.CloseWait and \
                        reservation_state != ReservationStates.Failed:
                    self.logger.info(f"Slice: {controller_slice.get_slice_name()}/{controller_slice.get_slice_id()}"
                                     f" has resource type: {reservation.get_resource_type()}"
                                     f" with reservation: {reservation.get_reservation_id()} "
                                     f"in state: {reservation_state.name} "
                                     f"that is not yet done")
                    return True
            self.logger.info(f"Slice: {controller_slice.get_slice_name()}/{controller_slice.get_slice_id()} "
                             f"has no non-final reservations ({len(all_reservations)})")

            return False
