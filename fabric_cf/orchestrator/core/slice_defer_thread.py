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
import time
import traceback
from datetime import datetime

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.time.term import Term
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_slice import OrchestratorSlice


class SliceDeferThread:
    """
    This runs as a standalone thread started by OrchestratorState and deals with slices that have to wait for other
    slices to complete.
    """
    THREAD_SLEEP_TIME = 10000
    DEFAULT_MAX_CREATE_TIME = 600000
    DEFAULT_DELAY_RESOURCE_TYPES = "nlr.vlan ion.vlan ben.vlan"

    def __init__(self):
        self.deferred_slices = []
        self.queue_lock = threading.Lock()
        self.avail = threading.Condition()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.thread = None

        self.last_slice = None
        self.last_slice_time = None
        self.max_create_wait_time = 0
        self.delay_resource_types = None
        self.stopped = False

        wait_time = GlobalsSingleton.get().get_config().get_runtime_config().get(
            Constants.property_conf_controller_create_wait_time_ms, None)

        if wait_time is None:
            self.max_create_wait_time = self.DEFAULT_MAX_CREATE_TIME
        else:
            self.max_create_wait_time = wait_time

        delay_rtype = GlobalsSingleton.get().get_config().get_runtime_config().get(
            Constants.property_conf_controller_delay_resource_types, None)

        if delay_rtype is None:
            self.delay_resource_types = self.DEFAULT_DELAY_RESOURCE_TYPES.split(" ")
        else:
            self.delay_resource_types = delay_rtype.split(" ")

    def put_tail(self, *, controller_slice: OrchestratorSlice):
        try:
            self.queue_lock.acquire()
            if controller_slice is not None:
                self.deferred_slices.append(controller_slice)
                self.avail.notify_all()

        finally:
            self.queue_lock.release()

    def get_head(self):
        ret_val = None
        try:
            self.queue_lock.acquire()
            self.avail.wait(self.THREAD_SLEEP_TIME)
            if len(self.deferred_slices) > 0:
                ret_val = self.deferred_slices[0]
        except Exception:
            ret_val = None
        finally:
            self.queue_lock.release()

        return ret_val

    def remove_head(self):
        try:
            self.queue_lock.acquire()
            self.deferred_slices.pop(0)
        finally:
            self.queue_lock.release()

    def update_last(self, *, controller_slice: OrchestratorSlice):
        if controller_slice is None:
            return

        self.logger.info("Updating last slice with: {}/{}".format(controller_slice.get_slice_urn(),
                                                                  controller_slice.get_slice_id()))
        self.last_slice = controller_slice
        self.last_slice_time = datetime.utcnow()

    def process_slice(self, *, controller_slice: OrchestratorSlice):
        if controller_slice is None:
            return

        if controller_slice != self.last_slice and self.check_computed_reservations(controller_slice=controller_slice) and \
                self.delay_not_done(controller_slice=self.last_slice):
            self.logger.info("Putting slice {}/{} on wait queue".format(controller_slice.get_slice_urn(),
                                                                        controller_slice.get_slice_id()))
            self.put_tail(controller_slice=controller_slice)
        else:
            self.logger.info("Processing slice {}/{} immediately".format(controller_slice.get_slice_urn(),
                                                                         controller_slice.get_slice_id()))
            if self.check_computed_reservations(controller_slice=controller_slice):
                self.update_last(controller_slice=controller_slice)

            self.demand_slice(controller_slice=controller_slice)

    def start(self):
        self.thread = threading.Thread(target=self.run())
        self.thread.setDaemon(True)
        self.thread.setName('SliceDeferThread')
        self.thread.start()

    def stop(self):
        self.stopped = True
        if self.thread is not None:
            self.thread.join()

    def run(self):
        while True:
            controller_slice = self.get_head()
            #self.logger.debug("SliceDeferThread tick")
            time.sleep(1)

            if self.stopped:
                self.logger.debug("SliceDeferThread exiting")
                return

            if controller_slice is None:
                continue

            self.logger.info("Processing previously deferred slice {}/{}".format(self.last_slice.get_slice_urn(),
                                                                                 self.last_slice.get_slice_id()))
            try:
                self.last_slice.lock()
                if self.delay_not_done(controller_slice=self.last_slice):
                    if Term.delta(self.last_slice, datetime.utcnow()) > self.max_create_wait_time:
                        self.logger.info("Maximum wait time exceeded for slice: {}/{}, proceeding anyway".format(
                            self.last_slice.get_slice_urn(), self.last_slice.get_slice_id()))
                    else:
                        continue
            except Exception as e:
                self.logger.error("Exception while checking slice {}/{} e: {}".format(self.last_slice.get_slice_urn(),
                                                                                      self.last_slice.get_slice_id(),
                                                                                      e))
            finally:
                self.last_slice.unlock()

            self.logger.info("Performing demand on deferred slice {}/{}".format(self.last_slice.get_slice_urn(),
                                                                                self.last_slice.get_slice_id()))

            self.update_last(controller_slice=controller_slice)
            try:
                controller_slice.lock()
                self.demand_slice(controller_slice=controller_slice)
            except Exception as e:
                self.logger.error("Exception while demanding slice: {}/{} e: {}".format(self.last_slice.get_slice_urn(),
                                                                                        self.last_slice.get_slice_id(),
                                                                                        e))
            finally:
                self.last_slice.unlock()

            self.remove_head()

    def demand_slice(self, *, controller_slice: OrchestratorSlice):
        if controller_slice is None:
            self.logger.error("demand slice was given a None slice")
            return

        computed_reservations = controller_slice.get_computed_reservations()

        if computed_reservations is None:
            return

        try:
            from fabric_cf.orchestrator.core.orchestrator_state import OrchestratorStateSingleton
            controller = OrchestratorStateSingleton.get().get_management_actor()
            for reservation in computed_reservations:
                self.logger.debug("Issuing demand for reservation: {}".format(reservation.get_reservation_id()))

                if reservation.get_state() != ReservationStates.Unknown:
                    continue

                if not controller.demand(reservation):
                    raise OrchestratorException("Could not demand resources: {}".format(controller.get_last_error()))
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Unable to get orchestrator or demand reservation: {}".format(e))
            return

    def check_computed_reservations(self, *, controller_slice: OrchestratorSlice) -> bool:
        if controller_slice is None or controller_slice.get_computed_reservations() is None:
            self.logger.info("Empty slice or no computed reservations")
            return False

        for reservation in controller_slice.get_computed_reservations():
            for drt in self.delay_resource_types:
                if drt == reservation.get_resource_type():
                    self.logger.info("{}/{} has delayed domain".format(controller_slice.get_slice_urn(),
                                                                       controller_slice.get_slice_id()))
                    return True

        self.logger.info("{}/{} has no delayed domains".format(controller_slice.get_slice_urn(),
                                                               controller_slice.get_slice_id()))

        return False

    def delay_not_done(self, *, controller_slice: OrchestratorSlice) -> bool:
        if controller_slice is None:
            return False

        self.logger.info("Checking slice {}/{}".format(controller_slice.get_slice_urn(),
                                                       controller_slice.get_slice_id()))

        controller = None
        all_reservations = None

        try:
            from fabric_cf.orchestrator.core.orchestrator_state import OrchestratorStateSingleton
            controller = OrchestratorStateSingleton.get().get_management_actor()
            all_reservations = controller_slice.get_all_reservations(controller=controller)
        except Exception as e:
            self.logger.error("Exception in delay_not_done for slice: {}/{} e: {}".format(
                controller_slice.get_slice_urn(), controller_slice.get_slice_id(), e))

        if all_reservations is None:
            self.logger.info("Slice: {}/{} has None reservations in delay_not_done".format(
                controller_slice.get_slice_urn(), controller_slice.get_slice_id()))

            return self.check_computed_reservations(controller_slice=controller_slice)
        else:
            if len(all_reservations) <= 0:
                self.logger.info("Slice: {}/{} has empty reservations in delay_not_done".format(
                    controller_slice.get_slice_urn(), controller_slice.get_slice_id()))
                return self.check_computed_reservations(controller_slice=controller_slice)

            for reservation in all_reservations:
                rtype = reservation.get_resource_type()
                for drt in self.delay_resource_types:
                    if drt == rtype and reservation.get_state() != ReservationStates.Active and \
                        reservation.get_state() != ReservationStates.Closed and \
                        reservation.get_state() != ReservationStates.CloseWait and \
                            reservation.get_state() != ReservationStates.Failed:
                        self.logger.info("Slice: {}/{} has domain {} with reservation: {} that is not yet done".
                                         format(controller_slice.get_slice_urn(), controller_slice.get_slice_id(),
                                                drt, reservation.get_reservation_id()))
                        return True
            self.logger.info("Slice: {}/{} has no non-final reservations ({})".format(controller_slice.get_slice_urn(),
                                                                                      controller_slice.get_slice_id(),
                                                                                      len(all_reservations)))

            return False
