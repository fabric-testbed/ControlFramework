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

import logging
import traceback
from typing import TYPE_CHECKING, List

import threading

from fabric_mb.message_bus.consumer import AvroConsumerApi
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro

from fabric_cf.actor.core.common.exceptions import KafkaServiceException

if TYPE_CHECKING:
    from fabric_cf.actor.core.proxies.kafka.services.actor_service import ActorService
    from fabric_cf.actor.core.manage.kafka.services.kafka_actor_service import KafkaActorService


class MessageService(AvroConsumerApi):
    def __init__(self, *, kafka_service: ActorService, kafka_mgmt_service: KafkaActorService, consumer_conf: dict,
                 key_schema_location, value_schema_location: str, topics: List[str], batch_size: int = 5,
                 logger: logging.Logger = None, sync: bool = False):
        super(MessageService, self).__init__(consumer_conf=consumer_conf, key_schema_location=key_schema_location,
                                             value_schema_location=value_schema_location, topics=topics,
                                             batch_size=batch_size, logger=logger, sync=sync)
        self.thread_lock = threading.Lock()
        self.thread = None
        self.kafka_service = kafka_service
        self.kafka_mgmt_service = kafka_mgmt_service

    def start(self):
        try:
            self.thread_lock.acquire()
            if self.thread is not None:
                raise KafkaServiceException("This Message Service has already been started")

            self.thread = threading.Thread(target=self.consume)
            self.thread.setName("MessageService")
            self.thread.setDaemon(True)
            self.thread.start()
            self.logger.debug("Message service has been started")
        finally:
            self.thread_lock.release()

    def stop(self):
        self.shutdown()
        try:
            self.thread_lock.acquire()
            temp = self.thread
            self.thread = None
            if temp is not None:
                self.logger.warning("It seems that the Message Service thread is running. Interrupting it")
                try:
                    temp.join()
                except Exception as e:
                    self.logger.error("Could not join Message Service thread {}".format(e))
                finally:
                    self.thread_lock.release()
        finally:
            if self.thread_lock is not None and self.thread_lock.locked():
                self.thread_lock.release()

    def handle_message(self, message: AbcMessageAvro):
        try:
            if message.get_message_name() == AbcMessageAvro.claim_resources or \
                    message.get_message_name() == AbcMessageAvro.reclaim_resources or \
                    message.get_message_name() == AbcMessageAvro.get_slices_request or \
                    message.get_message_name() == AbcMessageAvro.get_reservations_request or \
                    message.get_message_name() == AbcMessageAvro.get_reservations_state_request or \
                    message.get_message_name() == AbcMessageAvro.get_delegations or \
                    message.get_message_name() == AbcMessageAvro.get_reservation_units_request or \
                    message.get_message_name() == AbcMessageAvro.get_unit_request or \
                    message.get_message_name() == AbcMessageAvro.get_broker_query_model_request or \
                    message.get_message_name() == AbcMessageAvro.add_slice or \
                    message.get_message_name() == AbcMessageAvro.update_slice or \
                    message.get_message_name() == AbcMessageAvro.remove_slice or \
                    message.get_message_name() == AbcMessageAvro.close_reservations or \
                    message.get_message_name() == AbcMessageAvro.update_reservation or \
                    message.get_message_name() == AbcMessageAvro.remove_reservation or \
                    message.get_message_name() == AbcMessageAvro.extend_reservation:
                self.kafka_mgmt_service.process(message=message)
            else:
                self.kafka_service.process(message=message)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(e)
            self.logger.error("Discarding the incoming message {}".format(message))
