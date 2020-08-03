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


class ProxyMng:
    def __init__(self):
        self.protocol = None
        self.name = None
        self.guid = None
        self.type = None
        self.kafka_topic = None

    def set_name(self, name: str):
        self.name = name

    def set_guid(self, guid: str):
        self.guid = guid

    def get_name(self) -> str:
        return self.name

    def get_guid(self) -> str:
        return self.guid

    def set_protocol(self, protocol: str):
        self.protocol = protocol

    def set_type(self, type: str):
        self.type = type

    def get_protocol(self) -> str:
        return self.protocol

    def get_type(self) -> str:
        return self.type

    def set_kafka_topic(self, kafka_topic: str):
        self.kafka_topic = kafka_topic

    def get_kafka_topic(self) -> str:
        return self.kafka_topic