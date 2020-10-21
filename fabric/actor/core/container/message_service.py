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

import traceback
from typing import TYPE_CHECKING

import threading

from fabric.message_bus.consumer import AvroConsumerApi
from fabric.message_bus.messages.message import IMessageAvro

if TYPE_CHECKING:
    from fabric.actor.core.proxies.kafka.services.actor_service import ActorService
    from fabric.actor.core.manage.kafka.services.kafka_actor_service import KafkaActorService


class MessageService(AvroConsumerApi):
    def __init__(self, *, kafka_service: ActorService, kafka_mgmt_service: KafkaActorService, conf: dict, key_schema,
                 record_schema, topics, batchSize=5, logger=None):
        super().__init__(conf=conf, key_schema=key_schema, record_schema=record_schema, topics=topics,
                         batchSize=batchSize, logger=logger)
        self.thread_lock = threading.Lock()
        self.thread = None
        self.kafka_service = kafka_service
        self.kafka_mgmt_service = kafka_mgmt_service

    def start(self):
        try:
            self.thread_lock.acquire()
            if self.thread is not None:
                raise Exception("This Message Service has already been started")

            self.thread = threading.Thread(target=self.consume_auto)
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
                    self.logger.error("Could not join actor thread {}".format(e))
                finally:
                    self.thread_lock.release()
        finally:
            if self.thread_lock is not None and self.thread_lock.locked():
                self.thread_lock.release()

    def handle_message(self, *, message: IMessageAvro):
        try:
            if message.get_message_name() == IMessageAvro.ClaimResources or \
                    message.get_message_name() == IMessageAvro.ReclaimResources or \
                    message.get_message_name() == IMessageAvro.GetSlicesRequest or \
                    message.get_message_name() == IMessageAvro.GetReservationsRequest or \
                    message.get_message_name() == IMessageAvro.GetReservationsStateRequest or \
                    message.get_message_name() == IMessageAvro.GetDelegations or \
                    message.get_message_name() == IMessageAvro.GetReservationUnitsRequest or \
                    message.get_message_name() == IMessageAvro.GetUnitRequest or \
                    message.get_message_name() == IMessageAvro.GetPoolInfoRequest or \
                    message.get_message_name() == IMessageAvro.AddSlice or \
                    message.get_message_name() == IMessageAvro.UpdateSlice or \
                    message.get_message_name() == IMessageAvro.RemoveSlice or \
                    message.get_message_name() == IMessageAvro.CloseReservations or \
                    message.get_message_name() == IMessageAvro.UpdateReservation or \
                    message.get_message_name() == IMessageAvro.RemoveReservation or \
                    message.get_message_name() == IMessageAvro.ExtendReservation:
                self.kafka_mgmt_service.process(message=message)
            else:
                self.kafka_service.process(message=message)
        except Exception as e:
            traceback.print_exc()
            self.logger.error(e)
            self.logger.error("Discarding the incoming message {}".format(message))


