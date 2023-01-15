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
import logging
import queue
import threading
import time
import traceback
import re

from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import ReservationInfo

from fabric_cf.actor.core.util.iterable_queue import IterableQueue
from fabric_cf.actor.fim.fim_helper import FimHelper


class AsmUpdateException(Exception):
    pass


class AsmEvent:
    def __init__(self, *, graph_id: str, sliver: BaseSliver):
        self.graph_id = graph_id
        self.sliver = sliver

    def process(self):
        FimHelper.update_node(graph_id=self.graph_id, sliver=self.sliver)


class AsmUpdateThread:
    def __init__(self, *, name, logger: logging.Logger = None):
        self.event_queue = queue.Queue()
        self.thread_lock = threading.Lock()
        self.condition = threading.Condition()
        self.thread = None
        self.shutdown = False
        self.name = name
        if logger is None:
            self.logger = logging.Logger(self.name)
        else:
            self.logger = logger

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['event_queue']
        del state['thread_lock']
        del state['condition']
        del state['thread']
        del state['shutdown']
        del state['logger']

    def __setstate__(self, state):
        self.event_queue = queue.Queue()
        self.thread_lock = threading.Lock()
        self.condition = threading.Condition()
        self.thread = None
        self.shutdown = False
        self.logger = None

    def set_logger(self, *, logger: logging.Logger):
        self.logger = logger

    def start(self):
        try:
            self.thread_lock.acquire()
            if self.thread is not None:
                raise AsmUpdateException(f"Thread {self.name} has already been started")

            self.thread = threading.Thread(target=self.__run)
            self.thread.setName(self.name)
            self.thread.setDaemon(True)
            self.thread.start()
            self.logger.debug(f"Thread {self.name} has been started")
        finally:
            self.thread_lock.release()

    def stop(self):
        self.shutdown = True
        try:
            self.thread_lock.acquire()
            temp = self.thread
            self.thread = None
            if temp is not None:
                self.logger.warning(f"It seems that the {self.name} thread is running. Interrupting it")
                try:
                    temp.join()
                except Exception as e:
                    self.logger.error(f"Could not join {self.name} thread {e}")
                finally:
                    self.thread_lock.release()
        finally:
            if self.thread_lock is not None and self.thread_lock.locked():
                self.thread_lock.release()

    def enqueue(self, *, graph_id: str, sliver: BaseSliver, rid: str, reservation_state: str,
                error_message: str):
        try:
            if sliver.reservation_info is None:
                sliver.reservation_info = ReservationInfo()
            sliver.reservation_info.reservation_id = rid
            sliver.reservation_info.reservation_state = reservation_state
            sliver.reservation_info.error_message = error_message

            event = AsmEvent(graph_id=graph_id, sliver=sliver)
            self.event_queue.put_nowait(event)
            with self.condition:
                self.condition.notify_all()
            self.logger.debug("Added event to event queue {}".format(event.__class__.__name__))
        except Exception as e:
            self.logger.error(f"Failed to queue event: e: {e}")

    def __dequeue(self, queue_obj: queue.Queue):
        events = []
        if not queue_obj.empty():
            try:
                for event in IterableQueue(source_queue=queue_obj):
                    events.append(event)
            except Exception as e:
                self.logger.error(f"Error while adding event/timer to queue! e: {e}")
                self.logger.error(traceback.format_exc())
        return events

    def __process_events(self, *, events: list):
        for event in events:
            try:
                begin = time.time()
                event.process()
                self.logger.info(f"Event {event.__class__.__name__} "
                                 f"TIME: {time.time() - begin:.0f}")
            except Exception as e:
                self.logger.error(f"Error while processing event {type(event)}, {e}")
                self.logger.error(traceback.format_exc())

    def __run(self):
        self.logger.info(f"Thread {self.name} started")
        while True:
            with self.condition:
                while not self.shutdown and self.event_queue.empty():
                    self.condition.wait()

            if self.shutdown:
                break

            if self.shutdown:
                self.logger.info(f"Thread {self.name} exiting")
                return

            events = self.__dequeue(self.event_queue)
            self.__process_events(events=events)
