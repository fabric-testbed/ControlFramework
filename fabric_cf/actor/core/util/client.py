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
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.security.auth_token import AuthToken


class Client:
    def __init__(self):
        self.name = None
        self.guid = None
        self.type = None
        self.kafka_topic = None

    def __getstate__(self):
        state = self.__dict__.copy()
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def set_name(self, *, name: str):
        """
        Set Name
        :param name: name
        :return:
        """
        self.name = name

    def set_guid(self, *, guid: ID):
        """
        Set Guid
        :param guid: guid
        :return:
        """
        self.guid = guid

    def set_type(self, *, client_type: str):
        """
        Set type
        :param client_type:
        :return:
        """
        self.type = client_type

    def get_name(self) -> str:
        """
        Get name
        :return: name
        """
        return self.name

    def get_guid(self) -> ID:
        """
        Get guid
        :return: guid
        """
        return self.guid

    def get_type(self) -> str:
        """
        Get Type
        :return: type
        """
        return self.type

    def get_kafka_topic(self) -> str:
        """
        Get kafka Topic
        :return: kafka topic
        """
        return self.kafka_topic

    def set_kafka_topic(self, *, kafka_topic: str):
        """
        Set kafka topic
        :param kafka_topic:
        :return:
        """
        self.kafka_topic = kafka_topic

    def get_auth_token(self) -> AuthToken:
        """
        Get auth token
        :return:
        """
        return AuthToken(name=self.name, guid=self.guid)
