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
from abc import abstractmethod
from datetime import datetime

from fabric_cf.actor.core.apis.abc_ticker import ABCTicker
from fabric_cf.actor.core.common.exceptions import TickException
from fabric_cf.actor.core.time.actor_clock import ActorClock


class Tick(ABCTicker):
    """
    Abstract class for all container clock implementation
    """
    def __init__(self):
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        # The current cycle.
        self.current_cycle = -1
        # The logger.
        self.logger = GlobalsSingleton.get().get_logger()
        # Are we active?
        self.stopped = True
        # Cycle length in milliseconds. Default 1 cycle = 1millisecond.
        self.cycle_millis = 1
        # Cycle offset in milliseconds. Default 0 milliseconds.
        self.beginning_of_time = 0
        # The clock factory.
        self.clock = None
        # Manual flag. Default value: false.
        self.manual = False
        # Subscribers.
        self.subscribers = {}
        # Initialization status.
        self.initialized = False
        self.lock = threading.Lock()

    def calculate_cycle(self):
        """
        Calculates the cycle before the first cycle. The cycle is derived from current time.
        """
        self.current_cycle = self.clock.cycle(when=datetime.utcnow())

    def check_initialized(self):
        """
        Checks if the clock has been initialized. Throws an exception if
        the clock has not been initialized.
        """
        if not self.initialized:
            raise TickException("The clock must be initialized first.")

    def check_not_initialized(self):
        """
        Checks if the clock has not been initialized. Throws an
        exception if the clock has been initialized.
        """
        if self.initialized:
            raise TickException("The clock is already initialized")

    def check_running(self):
        """
        Checks if the clock has been stopped. Throws an exception if the
        clock is not running.
        """
        if self.stopped:
            raise TickException("The clock is stopped.")

    def check_stopped(self):
        """
        Checks if the clock has been stopped. Throws an exception if the
        clock is running.
        """
        if not self.stopped:
            raise TickException("The clock is already running.")

    def clear(self):
        try:
            self.lock.acquire()
            self.stop()
            self.subscribers.clear()
        finally:
            self.lock.release()

    def get_beginning_of_time(self) -> int:
        self.check_initialized()
        return self.beginning_of_time

    def get_current_cycle(self) -> int:
        self.check_initialized()
        return self.current_cycle

    def get_cycle_millis(self) -> int:
        self.check_initialized()
        return self.cycle_millis

    def initialize(self):
        try:
            self.lock.acquire()
            if not self.initialized:
                self.clock = ActorClock(beginning_of_time=self.beginning_of_time, cycle_millis=self.cycle_millis)
                if self.current_cycle == -1:
                    self.calculate_cycle()
                self.initialized = True
        finally:
            self.lock.release()

    def is_manual(self) -> bool:
        self.check_initialized()
        return self.manual

    @abstractmethod
    def next_tick(self):
        """
        Calculates and delivers the next tick.
        """

    def set_beginning_of_time(self, *, value: int):
        self.check_not_initialized()
        self.beginning_of_time = value

    def set_current_cycle(self, *, cycle: int):
        try:
            self.lock.acquire()
            self.check_not_initialized()
            self.current_cycle = cycle
        finally:
            self.lock.release()

    def set_cycle_millis(self, *, cycle_millis: int):
        self.check_not_initialized()
        self.cycle_millis = cycle_millis

    def set_manual(self, *, value: bool):
        self.check_not_initialized()
        self.manual = value

    def start(self):
        try:
            self.lock.acquire()
            self.check_initialized()
            self.check_stopped()
            self.stopped = False
            self.logger.info("Internal clock starting. Tick length {} ms".format(self.cycle_millis))
            if not self.manual:
                self.start_worker()
        finally:
            self.lock.release()

    @abstractmethod
    def start_worker(self):
        """
        Starts the worker thread(s).
        """

    def stop(self):
        try:
            self.lock.acquire()
            self.check_initialized()
            self.check_running()
            self.logger.info("Internal clock stopping")
            if not self.manual:
                self.stop_worker()
            self.stopped = True
        finally:
            self.lock.release()

    @abstractmethod
    def stop_worker(self):
        """
        Stops the worker thread(s).
        @throws Exception in case of error
        """

    def tick(self):
        self.check_initialized()
        self.check_running()
        if not self.manual:
            raise TickException("The clock is automatic. No manual calls are allowed.")
        self.next_tick()
