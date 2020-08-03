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
from __future__ import annotations

import queue
import threading
from datetime import datetime
from typing import TYPE_CHECKING

from fabric.actor.core.container.aevent_subscription import AEventSubscription
from fabric.actor.core.util.iterable_queue import IterableQueue

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_event import IEvent
    from fabric.actor.security.auth_token import AuthToken


class EventSubscription(AEventSubscription):
    MAX_QUEUE_SIZE = 1000
    # Max time between drain calls. The subscription is considered
    # abandoned if the time between drain calls exceeds this limit.
    MAX_IDLE_TIME = 5 * 60 * 1000 * 1000

    def __init__(self, token: AuthToken, filters: list):
        super().__init__(token, filters)
        self.queue = queue.Queue(maxsize=self.MAX_QUEUE_SIZE)
        self.last_drain_time_stamp = datetime.utcnow().timestamp()
        self.lock = threading.Lock()

    def deliver_event(self, event: IEvent):
        if not self.matches(event):
            return
        try:
            self.lock.acquire()
            if self.queue.qsize() == self.MAX_QUEUE_SIZE:
                self.queue.get()

            self.queue.put_nowait(event)
            self.queue.task_done()
        finally:
            self.lock.release()

    def drain_events(self, timeout: int) -> list:
        self.last_drain_time_stamp = datetime.utcnow().timestamp()
        result = []
        try:
            self.lock.acquire()
            for item in IterableQueue(self.queue):
                result.append(item)
        finally:
            self.lock.release()

        return result

    def is_abandoned(self) -> bool:
        return datetime.utcnow().timestamp() - self.last_drain_time_stamp > self.MAX_IDLE_TIME


