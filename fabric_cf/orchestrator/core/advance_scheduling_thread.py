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
import time
import traceback

from fabric_cf.actor.core.time.actor_clock import ActorClock

from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.iterable_queue import IterableQueue
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_slice_wrapper import OrchestratorSliceWrapper


class AdvanceSchedulingThread:
    """
    This runs as a standalone thread started by Orchestrator and deals with determining the
    nearest start time for slices requested in future.
    The purpose of this thread is to help orchestrator respond back to the create
    without identifying the start time and waiting for the slivers to be demanded
    """

    def __init__(self, *, kernel):
        self.slice_queue = queue.Queue()
        self.slice_avail_condition = threading.Condition()
        self.thread_lock = threading.Lock()
        self.thread = None
        self.stopped = False
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.mgmt_actor = kernel.get_management_actor()
        self.kernel = kernel

    def queue_slice(self, *, controller_slice: OrchestratorSliceWrapper):
        """
        Queue a slice
        :param controller_slice:
        :return:
        """
        try:
            self.slice_queue.put_nowait(controller_slice)
            self.logger.debug(f"Added slice to slices queue {controller_slice.get_slice_id()}")
        except Exception as e:
            self.logger.error(f"Failed to queue slice: {controller_slice.get_slice_id()} e: {e}")

    def start(self):
        """
        Start thread
        :return:
        """
        try:
            self.thread_lock.acquire()
            if self.thread is not None:
                raise OrchestratorException(f"This {self.__class__.__name__} has already been started")

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
                self.logger.warning(f"It seems that the {self.__class__.__name__} is running. "
                                    f"Interrupting it")
                try:
                    # TODO find equivalent of interrupt
                    with self.slice_avail_condition:
                        self.slice_avail_condition.notify_all()
                    temp.join()
                except Exception as e:
                    self.logger.error(f"Could not join {self.__class__.__name__} thread {e}")
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
        self.logger.debug(f"{self.__class__.__name__} started")
        while True:
            slices = []
            if not self.stopped and self.slice_queue.empty():
                time.sleep(0.001)

            if self.stopped:
                self.logger.info(f"{self.__class__.__name__} exiting")
                return

            if not self.slice_queue.empty():
                try:
                    for s in IterableQueue(source_queue=self.slice_queue):
                        slices.append(s)
                except Exception as e:
                    self.logger.error(f"Error while adding slice to slice queue! e: {e}")
                    self.logger.error(traceback.format_exc())

            if len(slices) > 0:
                self.logger.debug(f"Processing {len(slices)} slices")
                for s in slices:
                    try:
                        # Process the Slice i.e. Determine the nearest start time
                        # If start time found, queue the slice on SliceDeferredThread
                        # Else move the slice to Close State with failure
                        self.process_slice(controller_slice=s)
                    except Exception as e:
                        self.logger.error(f"Error while processing slice {type(s)}, {e}")
                        self.logger.error(traceback.format_exc())

    def process_slice(self, *, controller_slice: OrchestratorSliceWrapper):
        """
        Determine nearest start time for a slice requested in future and
        add to deferred slice thread for further processing
        :param controller_slice:
        """
        computed_reservations = controller_slice.get_computed_reservations()

        try:
            controller_slice.lock()

            # Determine nearest start time in the time range requested
            # If not found, start time specified in the request is used as start resulting in slice failing
            # with insufficient resources error
            future_start, future_end = self.kernel.determine_future_lease_time(
                computed_reservations=computed_reservations,
                start=controller_slice.start, end=controller_slice.end,
                duration=controller_slice.lifetime)

            self.logger.debug(f"Slice: {controller_slice.slice_obj.slice_name}/{controller_slice.slice_obj.get_slice_id()}"
                              f" Start Time: {future_start} End: {future_end}")

            # Update slice start/end time
            controller_slice.slice_obj.set_lease_start(lease_start=future_start)
            controller_slice.slice_obj.set_lease_end(lease_end=future_end)
            self.logger.debug(f"Update Slice {controller_slice.slice_obj.slice_name}")
            self.mgmt_actor.update_slice(slice_obj=controller_slice.slice_obj)

            # Update the reservations start/end time
            for r in computed_reservations:
                r.set_start(value=ActorClock.to_milliseconds(when=future_start))
                r.set_end(value=ActorClock.to_milliseconds(when=future_end))

            self.add_and_demand_reservations(controller_slice=controller_slice)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Unable to schedule a future slice: {}".format(e))
            self.add_and_demand_reservations(controller_slice=controller_slice)
        finally:
            controller_slice.unlock()

    def add_and_demand_reservations(self, controller_slice: OrchestratorSliceWrapper):
        try:
            # Add Reservations to relational database;
            controller_slice.add_reservations()

            # Queue the slice to be demanded on Slice Defer Thread
            self.kernel.get_defer_thread().queue_slice(controller_slice=controller_slice)
        except Exception as e:
            self.logger.error(f"SHOULD_NOT_HAPPEN: Queueing slice on Deferred Queue Failed: {controller_slice.slice_obj}")
            self.logger.error(f"Exception: {e}")
            self.logger.error(traceback.format_exc())
