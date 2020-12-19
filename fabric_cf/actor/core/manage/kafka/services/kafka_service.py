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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.result_avro import ResultAvro


class KafkaService:
    def __init__(self):
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.producer = GlobalsSingleton.get().get_kafka_producer()
        self.logger = None

    def set_logger(self, *, logger):
        self.logger = logger
        if self.producer is not None:
            self.producer.set_logger(logger=logger)

    def update_status(self, *, incoming: ResultAvro, outgoing: ResultAvro):
        outgoing.set_code(incoming.get_code())
        outgoing.set_details(incoming.get_details())
        outgoing.set_message(incoming.get_message())
        return outgoing

    def get_first(self, *, result_list: list):
        if result_list is not None and len(result_list) > 0:
            return result_list.__iter__().__next__()
        return None
