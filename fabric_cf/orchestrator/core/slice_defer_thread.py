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
import time

from fim.user import ServiceType

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.time.term import Term
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_slice_wrapper import OrchestratorSliceWrapper


class SliceDeferThread:
    """
    This runs as a standalone thread started by Orchestrator and deals with slices that have
    NetworkService reservations which have to wait for NetworkNode reservations to be ticketed
    """
    THREAD_SLEEP_TIME_IN_SECONDS = 5
    DEFAULT_MAX_CREATE_TIME_IN_SECONDS = 600

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
            Constants.PROPERTY_CONF_CONTROLLER_CREATE_WAIT_TIME, None)

        if wait_time is None:
            self.max_create_wait_time = self.DEFAULT_MAX_CREATE_TIME_IN_SECONDS
        else:
            self.max_create_wait_time = wait_time

        self.delay_resource_types = []
        self.delay_resource_types.append(str(ServiceType.L2STS))
        self.delay_resource_types.append(str(ServiceType.L2Bridge))
        self.delay_resource_types.append(str(ServiceType.L2PTP))

    def queue_slice(self, *, controller_slice: OrchestratorSliceWrapper):
        """
        Queue a slice
        :param controller_slice:
        :return:
        """
        with self.defer_slice_avail_condition:
            self.deferred_slices.put_nowait(controller_slice)
            self.logger.debug(f"Added slice to deferred slices queue {controller_slice.__class__.__name__}")
            time.sleep(self.THREAD_SLEEP_TIME_IN_SECONDS)
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

        self.logger.info(f"Processing slice {controller_slice.get_slice_name()}/{controller_slice.get_slice_id()} "
                         f"immediately")
        if self.check_computed_reservations(controller_slice=controller_slice):
            self.update_last(controller_slice=controller_slice)

        if self.demand_slice(controller_slice=controller_slice):
            # Slice has pending reservations
            self.queue_slice(controller_slice=controller_slice)

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
                        current_time = datetime.utcnow()
                        if (current_time - self.last_slice_time).seconds > self.max_create_wait_time:
                            self.logger.info(f"Maximum wait time exceeded for slice: {self.last_slice.get_slice_name()}/"
                                             f"{self.last_slice.get_slice_id()}, proceeding anyway")
                            # This shall trigger failure
                            self.demand_slice(controller_slice=self.last_slice, force=True)
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
                if self.demand_slice(controller_slice=controller_slice):
                    self.logger.debug("Adding slice back to the queue!")
                    self.queue_slice(controller_slice=controller_slice)
            except Exception as e:
                self.logger.error(f"Exception while demanding slice: {self.last_slice.get_slice_name()}/"
                                  f"{self.last_slice.get_slice_id()} e: {e}")
            finally:
                controller_slice.unlock()

        self.logger.debug("SliceDeferThread exited")

    def demand_slice(self, *, controller_slice: OrchestratorSliceWrapper, force: bool = False) -> bool:
        """
        Demand slice reservations. If any of the reservations have predecessors which are not ticketed yet;
        those reservations are not demanded yet i.e. no Ticket request to broker is triggered for them.
        :param controller_slice:
        :param force:
        :return: True if slice should be added to deferred queue to demand the delayed reservations; False otherwise
        """
        ret_val = False
        if controller_slice is None:
            self.logger.error("demand slice was given a None slice")
            return ret_val

        computed_reservations = controller_slice.get_computed_reservations()

        if computed_reservations is None:
            return ret_val

        try:
            from fabric_cf.orchestrator.core.orchestrator_kernel import OrchestratorKernelSingleton
            controller = OrchestratorKernelSingleton.get().get_management_actor()
            for reservation in computed_reservations:
                if reservation.get_reservation_id() in controller_slice.demanded_reservations():
                    self.logger.debug(f"Reservation: {reservation.get_reservation_id()} already demanded")
                    continue

                self.logger.debug(f"Issuing demand for reservation: {reservation.get_reservation_id()}")

                if reservation.get_state() != ReservationStates.Unknown.value:
                    self.logger.debug(f"Reservation not in {reservation.get_state()} state, ignoring it")
                    continue

                if not controller_slice.check_predecessors_ticketed(reservation=reservation) and not force:
                    self.logger.info(f"Reservation waiting for predecessors to be ticketed, ignoring it")
                    ret_val = True
                    continue

                if not controller.demand_reservation(reservation=reservation):
                    raise OrchestratorException(f"Could not demand resources: {controller.get_last_error()}")
                controller_slice.mark_demanded(rid=reservation.get_reservation_id())
                self.logger.debug(f"Reservation #{reservation.get_reservation_id()} demanded successfully")
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Unable to get orchestrator or demand reservation: {}".format(e))

        return ret_val

    def check_computed_reservations(self, *, controller_slice: OrchestratorSliceWrapper) -> bool:
        """
        Check computed reservations to see if there are any network sliver reservations
        :param controller_slice:
        :return: True if slice has network sliver reservations
        """
        for r in controller_slice.get_computed_reservations():
            if r.get_resource_type() in self.delay_resource_types:
                self.logger.info(f"Slice: {controller_slice.get_slice_name()}/{controller_slice.get_slice_id()} "
                                 f" has delayed resource type: {r.get_resource_type()}")
            return True

        return False

    def delay_not_done(self, *, controller_slice: OrchestratorSliceWrapper) -> bool:
        """
        Check if predecessors for network sliver reservations have been ticketed
        :param controller_slice:
        :return: True if Predecessors for Network Sliver Reservations have not been ticketed; False otherwise
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
            # slice has not been submitted
            return self.check_computed_reservations(controller_slice=controller_slice)
        else:
            if len(all_reservations) <= 0:
                self.logger.info(f"Slice: {controller_slice.get_slice_name()}/{controller_slice.get_slice_id()} "
                                 f"has empty reservations in delay_not_done")
                # slice has not been submitted
                return self.check_computed_reservations(controller_slice=controller_slice)

            for reservation in all_reservations:
                if reservation.get_resource_type() in self.delay_resource_types:
                    # Reservation is Network Sliver Reservation;
                    # Reservation is not in a final state, we need to wait
                    reservation_state = ReservationStates(reservation.get_state())
                    if reservation_state != ReservationStates.Active and \
                            reservation_state != ReservationStates.Closed and \
                            reservation_state != ReservationStates.CloseWait and \
                            reservation_state != ReservationStates.Failed:
                        self.logger.info(f"Slice: {controller_slice.get_slice_name()}/{controller_slice.get_slice_id()}"
                                         f" has resource type: {reservation.get_resource_type()}"
                                         f" with reservation: {reservation.get_reservation_id()} "
                                         f"in state: {reservation_state.name} "
                                         f"that is not yet done")

                        # Use reservation from computed reservation as reservation has not been demanded yet
                        if controller_slice.check_predecessors_ticketed_by_rid(rid=reservation.get_reservation_id()):
                            self.logger.info("Predecessors ticketed, reservation can be demanded now!")
                            return False

                        return True
            self.logger.info(f"Slice: {controller_slice.get_slice_name()}/{controller_slice.get_slice_id()} "
                             f"has no non-final reservations ({len(all_reservations)})")

            return False
