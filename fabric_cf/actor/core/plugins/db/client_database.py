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
from abc import abstractmethod
from typing import List

from fabric_cf.actor.core.util.client import Client
from fabric_cf.actor.core.util.id import ID


class ClientDatabase:
    """
    This interface describes the methods necessary to maintain information about
    the clients of an actor that acts in a server role.
    """
    @abstractmethod
    def add_client(self, *, client: Client):
        """
        Adds a new database record representing this client
        @param client client
        @throws Exception in case of error
        """

    @abstractmethod
    def update_client(self, *, client: Client):
        """
        Updates database record representing this client
        @param client client
        @throws Exception in case of error
        """

    @abstractmethod
    def remove_client(self, *, guid: ID):
        """
        Removes the specified client record
        @param guid client guid
        @throws Exception in case of error
        """

    @abstractmethod
    def get_client(self, *, guid: ID) -> Client:
        """
        Retrieves the specified client record
        @param guid client guid
        @return vector of properties
        @throws Exception in case of error
        """

    @abstractmethod
    def get_clients(self) -> List[Client]:
        """
        Retrieves all client records
        @return vector of properties
        @throws Exception in case of error
        """
