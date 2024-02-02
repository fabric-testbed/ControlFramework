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

from typing import TYPE_CHECKING, List
import logging
import queue
import threading
import time
import traceback


from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_cf.actor.core.util.iterable_queue import IterableQueue

if TYPE_CHECKING:
    from fabric_cf.actor.core.proxies.kafka.services.actor_service import ActorService
    from fabric_cf.actor.core.manage.kafka.services.kafka_actor_service import KafkaActorService


class RPCConsumerException(Exception):
    pass


class RPCConsumer:
    MANAGEMENT_MESSAGES = [AbcMessageAvro.claim_resources,
                           AbcMessageAvro.reclaim_resources,
                           AbcMessageAvro.get_slices_request,
                           AbcMessageAvro.get_reservations_request,
                           AbcMessageAvro.get_reservations_state_request,
                           AbcMessageAvro.get_delegations,
                           AbcMessageAvro.get_reservation_units_request,
                           AbcMessageAvro.get_unit_request,
                           AbcMessageAvro.get_broker_query_model_request,
                           AbcMessageAvro.add_slice,
                           AbcMessageAvro.update_slice,
                           AbcMessageAvro.remove_slice,
                           AbcMessageAvro.close_reservations,
                           AbcMessageAvro.update_reservation,
                           AbcMessageAvro.remove_reservation,
                           AbcMessageAvro.extend_reservation,
                           AbcMessageAvro.close_delegations,
                           AbcMessageAvro.remove_delegation,
                           AbcMessageAvro.maintenance_request,
                           AbcMessageAvro.get_sites_request]

    def __init__(self, *, kafka_service: ActorService, kafka_mgmt_service: KafkaActorService,
                 logger: logging.Logger = None):
        self.message_queue = queue.Queue()
        self.thread_lock = threading.Lock()
        self.condition = threading.Condition()
        self.thread = None
        self.shutdown = False
        self.name = self.__class__.__name__
        if logger is None:
            self.logger = logging.Logger(self.name)
        else:
            self.logger = logger
        self.kafka_service = kafka_service
        self.kafka_mgmt_service = kafka_mgmt_service

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['message_queue']
        del state['thread_lock']
        del state['condition']
        del state['thread']
        del state['shutdown']
        del state['logger']

    def __setstate__(self, state):
        self.message_queue = queue.Queue()
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
                raise RPCConsumerException(f"{self.name} has already been started")

            self.thread = threading.Thread(target=self.__run, name=self.name, daemon=True)
            self.thread.start()
            self.logger.debug(f"{self.name} has been started")
        finally:
            self.thread_lock.release()

    def stop(self):
        self.shutdown = True
        try:
            self.thread_lock.acquire()
            temp = self.thread
            self.thread = None
            if temp is not None:
                self.logger.warning(f"It seems that the f{self.name} thread is running. Interrupting it")
                try:
                    temp.join()
                except Exception as e:
                    self.logger.error(f"Could not join {self.name} thread {e}")
                finally:
                    self.thread_lock.release()
        finally:
            if self.thread_lock is not None and self.thread_lock.locked():
                self.thread_lock.release()

    def enqueue(self, incoming):
        try:
            self.message_queue.put_nowait(incoming)
            with self.condition:
                self.condition.notify_all()
            self.logger.debug("Added message to queue {}".format(incoming.__class__.__name__))
        except Exception as e:
            self.logger.error(f"Failed to queue message: {incoming.__class__.__name__} e: {e}")

    def __dequeue(self, queue_obj: queue.Queue):
        events = []
        if not queue_obj.empty():
            try:
                for event in IterableQueue(source_queue=queue_obj):
                    events.append(event)
            except Exception as e:
                self.logger.error(f"Error while adding message to queue! e: {e}")
                self.logger.error(traceback.format_exc())
        return events

    def __process_messages(self, *, messages: list):
        for message in messages:
            try:
                begin = time.time()
                if message.get_message_name() in self.MANAGEMENT_MESSAGES:
                    self.kafka_mgmt_service.process(message=message)
                else:
                    self.kafka_service.process(message=message)
                diff = int(time.time() - begin)
                if diff > 0:
                    self.logger.info(f"Event {message.__class__.__name__} TIME: {diff}")
            except Exception as e:
                self.logger.error(f"Error while processing message {type(message)}, {e}")
                self.logger.error(traceback.format_exc())

    def __run(self):
        while True:
            with self.condition:
                while not self.shutdown and self.message_queue.empty():
                    self.condition.wait()

            if self.shutdown:
                break

            if self.shutdown:
                self.logger.info(f"{self.name} exiting")
                return

            messages = self.__dequeue(self.message_queue)
            self.__process_messages(messages=messages)
