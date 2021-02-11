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
import traceback

from fabric_mb.message_bus.consumer import AvroConsumerApi
from fabric_mb.message_bus.messages.message import IMessageAvro

from fabric_cf.actor.core.common.exceptions import ManageException


class MessageWrapper:
    def __init__(self, *, message: IMessageAvro):
        self.message = message
        self.condition = threading.Condition()
        self.done = False
        self.response = None


class KafkaMgmtMessageProcessor(AvroConsumerApi):
    def __init__(self, *, conf: dict, key_schema, record_schema, topics, batch_size=5, logger=None):
        super().__init__(conf=conf, key_schema=key_schema, record_schema=record_schema, topics=topics,
                         batch_size=batch_size, logger=logger)
        self.thread_lock = threading.Lock()
        self.thread = None
        self.messages = {}
        self.lock = threading.Lock()
        self.logger = logger

    def start(self):
        try:
            self.thread_lock.acquire()
            if self.thread is not None:
                raise ManageException("KafkaMgmtMessageProcessor has already been started")

            self.thread = threading.Thread(target=self.consume_auto)
            self.thread.setName("KafkaMgmtMessageProcessor")
            self.thread.setDaemon(True)
            self.thread.start()
            self.logger.debug("KafkaMgmtMessageProcessor has been started")
        finally:
            self.thread_lock.release()

    def stop(self):
        self.shutdown()
        try:
            self.thread_lock.acquire()
            temp = self.thread
            self.thread = None
            if temp is not None:
                self.logger.warning("It seems that the KafkaMgmtMessageProcessor thread is running. Interrupting it")
                try:
                    temp.join()
                except Exception as e:
                    self.logger.error("Could not join KafkaMgmtMessageProcessor thread {}".format(e))
                finally:
                    self.thread_lock.release()
        finally:
            if self.thread_lock is not None and self.thread_lock.locked():
                self.thread_lock.release()

    def handle_message(self, message: IMessageAvro):
        try:
            message_id = message.get_message_id()

            request = self.remove_message(msg_id=message_id)
            if request is None:
                self.logger.error("No corresponding request found for message_id: {}".format(message_id))
                self.logger.error("Discarding the message: {}".format(message))
                return
            with request.condition:
                request.done = True
                request.response = message
                request.condition.notify_all()

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(e)
            self.logger.error("Discarding the incoming message {}".format(message))

    def add_message(self, *, message: IMessageAvro) -> MessageWrapper:
        result = None
        try:
            msg_id = message.get_message_id()
            if msg_id is not None:
                self.thread_lock.acquire()
                result = MessageWrapper(message=message)
                if self.messages.get(msg_id, None) is not None:
                    print("Discarding the message, message with id: {} already exists".format(msg_id))

                self.messages[msg_id] = result
        finally:
            self.thread_lock.release()
        return result

    def remove_message(self, *, msg_id: str):
        try:
            self.thread_lock.acquire()
            if self.messages.get(msg_id, None) is not None:
                message = self.messages.pop(msg_id)
                return message
        finally:
            self.thread_lock.release()
        return None
