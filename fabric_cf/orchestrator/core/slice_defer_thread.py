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

from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.iterable_queue import IterableQueue
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_slice_wrapper import OrchestratorSliceWrapper
from fabric_cf.orchestrator.core.reservation_status_update import ReservationStatusUpdate


class SliceDeferThread:
    """
    This runs as a standalone thread started by Orchestrator and deals with issuing demand for the slivers for
    the newly created slices. The purpose of this thread is to help orchestrator respond back to the create
    without waiting for the slivers to be demanded
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
        self.sut = kernel.get_sut()

    def get_sut(self):
        if self.sut is None:
            self.sut = self.kernel.get_sut()
        return self.sut

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
                    with self.slice_avail_condition:
                        self.slice_avail_condition.notify_all()
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
        self.logger.debug(f"SliceDeferThread started")
        while True:
            slices = []
            if not self.stopped and self.slice_queue.empty():
                time.sleep(0.001)

            if self.stopped:
                self.logger.info("SliceDeferThread exiting")
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
                        # Process the Slice i.e. Demand the computed reservations i.e. Add them to the policy
                        # Once added to the policy; Actor Tick Handler will do following asynchronously:
                        # 1. Ticket message exchange with broker and
                        # 2. Redeem message exchange with AM once ticket is granted by Broker
                        self.process_slice(controller_slice=s)
                    except Exception as e:
                        self.logger.error(f"Error while processing slice {type(s)}, {e}")
                        self.logger.error(traceback.format_exc())

    def process_slice(self, *, controller_slice: OrchestratorSliceWrapper):
        """
        Demand slice reservations.
        :param controller_slice:
        """
        computed_reservations = controller_slice.get_computed_reservations()

        try:
            controller_slice.lock()
            for reservation in computed_reservations:
                self.logger.debug(f"Issuing demand for reservation: {reservation.get_reservation_id()}")

                if reservation.get_state() != ReservationStates.Unknown.value:
                    self.logger.debug(f"Reservation not in {reservation.get_state()} state, ignoring it")
                    continue

                if not self.mgmt_actor.demand_reservation(reservation=reservation):
                    raise OrchestratorException(f"Could not demand resources: {self.mgmt_actor.get_last_error()}")
                self.logger.debug(f"Reservation #{reservation.get_reservation_id()} demanded successfully")
            '''
            for r in controller_slice.computed_l3_reservations:
                res_status_update = ReservationStatusUpdate(logger=self.logger)
                self.get_sut().add_active_status_watch(watch=ID(uid=r.get_reservation_id()),
                                                       callback=res_status_update)
            '''

            for r in controller_slice.computed_remove_reservations:
                self.logger.debug(f"Issuing close for reservation: {r}")
                self.mgmt_actor.close_reservation(rid=ID(uid=r))

            for rid, modified_res in controller_slice.computed_modify_reservations.items():
                self.logger.debug(f"Issuing extend for modified reservation: {rid}")
                if not self.mgmt_actor.extend_reservation(reservation=ID(uid=rid), sliver=modified_res.sliver,
                                                          new_end_time=None, dependencies=modified_res.dependencies):
                    self.logger.error(f"Could not demand resources: {self.mgmt_actor.get_last_error()}")
                    continue
                self.logger.debug(f"Issued extend for reservation #{rid} successfully")

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Unable to get orchestrator or demand reservation: {}".format(e))
        finally:
            controller_slice.unlock()
