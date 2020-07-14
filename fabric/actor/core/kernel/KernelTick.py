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

from fabric.actor.core.apis.ITick import ITick
from fabric.actor.core.kernel.Tick import Tick


class KernelTick(Tick):
    def __init__(self):
        super().__init__()
        self.timer = None
        self.current_cycle = -1
        self.to_tick = set()
        self.stopped_worker = threading.Event()

    def add_tickable(self, tickable: ITick):
        with self.lock:
            self.to_tick.add(tickable)

    def remove_tickable(self, tickable: ITick):
        with self.lock:
            self.to_tick.remove(tickable)

    def start_worker(self):
        self.timer = threading.Thread(target=self.tick_notifier)
        self.timer.setDaemon(True)
        self.timer.setName("KernelTick")
        self.timer.start()

    def stop_worker(self):
        self.stopped_worker.set()
        self.timer.join()

    def next_tick(self):
        with self.lock:
            now = int(datetime.utcnow().timestamp() * 1000)
            self.current_cycle = self.clock.cycle(millis=now)
            self.logger.info("Clock interrupt: now= {} cycle={}".format(now, self.current_cycle))
            if not self.manual and self.timer is None:
                return

            for t in self.to_tick:
                try:
                    self.logger.info("Delivering external tick to {} cycle= {}".format(t.get_name(), self.current_cycle))
                    t.external_tick(self.current_cycle)
                except Exception as e:
                    self.logger.error("Unexpected error while delivering tick notification for {} {}".format(t.get_name(), e))

    def tick_notifier(self):
        while not self.stopped_worker.wait(timeout=self.cycle_millis / 1000):
            self.next_tick()
