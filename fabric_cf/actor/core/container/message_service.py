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
import traceback
from typing import List

import threading

from fabric_mb.message_bus.consumer import AvroConsumerApi
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro

from fabric_cf.actor.core.common.exceptions import KafkaServiceException


class MessageService(AvroConsumerApi):
    # uncomment for 1.7
    #def __init__(self, *, consumer_conf: dict, schema_registry_conf: dict, value_schema_location: str,
    #             topics: List[str], consumer_thread, batch_size: int = 5, logger: logging.Logger = None, sync: bool = False):
    #    super(MessageService, self).__init__(consumer_conf=consumer_conf, schema_registry_conf=schema_registry_conf,
    #                                         value_schema_location=value_schema_location, topics=topics,
    #                                         batch_size=batch_size, logger=logger)
    def __init__(self, *, consumer_conf: dict, key_schema_location, value_schema_location: str,
                 topics: List[str], consumer_thread, batch_size: int = 5, logger: logging.Logger = None,
                 poll_timeout: int = 250):
        super(MessageService, self).__init__(consumer_conf=consumer_conf, key_schema_location=key_schema_location,
                                             value_schema_location=value_schema_location, topics=topics,
                                             batch_size=batch_size, logger=logger, poll_timeout=poll_timeout)
        self.consumer_thread = consumer_thread
        self.thread_lock = threading.Lock()
        self.thread = None

    def start(self):
        try:
            self.thread_lock.acquire()
            if self.thread is not None:
                raise KafkaServiceException("This Message Service has already been started")

            self.thread = threading.Thread(target=self.consume, name=self.__class__.__name__,
                                           daemon=True)
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
            self.consumer_thread.enqueue(message)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(e)
            self.logger.error("Discarding the incoming message {}".format(message))
