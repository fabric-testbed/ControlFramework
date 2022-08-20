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
from abc import abstractmethod, ABC

from fim.slivers.base_sliver import BaseSliver

from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType


class ConfigToken(ABC):
    @abstractmethod
    def get_reservation_id(self) -> ID:
        """
        Return reservation id
        @return reservation
        """

    @abstractmethod
    def get_resource_type(self) -> ResourceType:
        """
        return resource type
        @return resource type
        """

    @abstractmethod
    def complete_modify(self):
        """
        Complete the modify operation
        """

    @abstractmethod
    def get_sequence(self) -> int:
        """
        Get Sequence number
        @return sequence number
        """

    @abstractmethod
    def get_id(self) -> ID:
        """
        Get unit id
        @return unit id
        """

    @abstractmethod
    def close(self):
        """
        Mark the unit as closed
        """

    @abstractmethod
    def activate(self):
        """
        Mark the unit as active
        """

    @abstractmethod
    def get_sliver(self) -> BaseSliver:
        """
        Get Sliver
        """

    @abstractmethod
    def set_sliver(self, *, sliver: BaseSliver):
        """
        Set sliver
        """

    @abstractmethod
    def update_sliver(self, *, sliver:BaseSliver):
        """
        Update Sliver
        """

    @abstractmethod
    def get_modified(self) -> BaseSliver:
        """
        Return the modified sliver
        """

    @abstractmethod
    def get_properties(self) -> dict:
        """
        Return properties
        """