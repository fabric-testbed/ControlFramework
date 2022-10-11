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
import enum
import logging
import queue
import threading
import time
import traceback

from fabric_cf.actor.core.apis.abc_actor_event import ABCActorEvent
from fabric_cf.actor.core.apis.abc_actor_runnable import ABCActorRunnable
from fabric_cf.actor.core.apis.abc_timer_task import ABCTimerTask
from fabric_cf.actor.core.util.iterable_queue import IterableQueue


class EventType(enum.Enum):
    TickEvent = enum.auto(),
    InterActorEvent = enum.auto(),
    SyncEvent = enum.auto()

    def __str__(self):
        return self.name


class EventProcessorException(Exception):
    pass


class ExecutionStatus:
    """
    Execution status
    """
    def __init__(self):
        self.done = False
        self.exception = None
        self.result = None
        self.lock = threading.Condition()

    def mark_done(self):
        """
        Mark as done
        """
        self.done = True


class TickEvent(ABCActorEvent):
    def __init__(self, *, actor, cycle: int):
        self.actor = actor
        self.cycle = cycle

    def __str__(self):
        return "{} {}".format(self.actor, self.cycle)

    def process(self):
        self.actor.actor_tick(cycle=self.cycle)


class ActorEvent(ABCActorEvent):
    def __init__(self, *, status: ExecutionStatus, runnable: ABCActorRunnable):
        self.status = status
        self.runnable = runnable

    def process(self):
        """
        Process an event
        """
        try:
            self.status.result = self.runnable.run()
        except Exception as e:
            traceback.print_exc()
            self.status.exception = e
        finally:
            with self.status.lock:
                self.status.done = True
                self.status.lock.notify_all()


class EventProcessor:
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
                raise EventProcessorException(f"Event Processor {self.name} has already been started")

            self.thread = threading.Thread(target=self.__run)
            self.thread.setName(self.name)
            self.thread.setDaemon(True)
            self.thread.start()
            self.logger.debug(f"Event Processor {self.name} has been started")
        finally:
            self.thread_lock.release()

    def stop(self):
        self.shutdown = True
        try:
            self.thread_lock.acquire()
            temp = self.thread
            self.thread = None
            if temp is not None:
                self.logger.warning("It seems that the Event Processor {self.name} thread is running. Interrupting it")
                try:
                    temp.join()
                except Exception as e:
                    self.logger.error(f"Could not join Event Processor {self.name} thread {e}")
                finally:
                    self.thread_lock.release()
        finally:
            if self.thread_lock is not None and self.thread_lock.locked():
                self.thread_lock.release()

    def enqueue(self, incoming):
        try:
            self.event_queue.put_nowait(incoming)
            with self.condition:
                self.condition.notify_all()
            self.logger.debug("Added event to event queue {}".format(incoming.__class__.__name__))
        except Exception as e:
            self.logger.error(f"Failed to queue event: {incoming.__class__.__name__} e: {e}")

    def execute_on_thread_async(self, *, runnable: ABCActorRunnable):
        """
        Execute an incoming action on actor thread
        @param runnable incoming action/operation
        """
        if self.__is_on_event_processor_thread():
            return runnable.run()
        else:
            status = ExecutionStatus()
            event = ActorEvent(status=status, runnable=runnable)

            self.enqueue(incoming=event)

    def execute_on_thread_sync(self, *, runnable: ABCActorRunnable):
        """
        Execute an incoming action on actor thread
        @param runnable incoming action/operation
        """
        if self.__is_on_event_processor_thread():
            return runnable.run()
        else:
            status = ExecutionStatus()
            event = ActorEvent(status=status, runnable=runnable)

            self.enqueue(incoming=event)

            with status.lock:
                while not status.done:
                    status.lock.wait()

            if status.exception is not None:
                raise status.exception

            return status.result

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
                if isinstance(event, ABCTimerTask):
                    event.execute()
                else:
                    event.process()
                self.logger.info(f"Event {event.__class__.__name__} TIME: {time.time() - begin:.0f}")
            except Exception as e:
                self.logger.error(f"Error while processing event {type(event)}, {e}")
                self.logger.error(traceback.format_exc())

    def __run(self):
        while True:
            with self.condition:
                while not self.shutdown and self.event_queue.empty():
                    self.condition.wait()

            if self.shutdown:
                break

            if self.shutdown:
                self.logger.info(f"Event Processor {self.name} exiting")
                return

            events = self.__dequeue(self.event_queue)
            self.__process_events(events=events)

    def __is_on_event_processor_thread(self) -> bool:
        """
        Check if running on event_processor
        @return true if running on event_processor, false otherwise
        """
        result = False
        try:
            self.thread_lock.acquire()
            result = self.thread == threading.current_thread()
        finally:
            self.thread_lock.release()
        return result
