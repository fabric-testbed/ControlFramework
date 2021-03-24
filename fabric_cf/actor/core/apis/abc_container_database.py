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

from abc import abstractmethod, ABC
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin


class ABCContainerDatabase(ABC):
    """
    Interface for container-level databases. Defines the functions necessary for
    bootstrapping and recovering a container.
    """

    @abstractmethod
    def add_container_properties(self, *, properties: dict):
        """
        Adds the container properties  to the database.

        @param properties container properties dict.

        @throws Exception if an error occurs while accessing the database
        """

    @abstractmethod
    def get_container_properties(self) -> dict:
        """
        Retrieves the container properties.
        @return container properties
        @throws Exception if an error occurs while accessing the database
        """

    @abstractmethod
    def set_reset_state(self, *, value: bool):
        """
        Controls whether the database should reset its state.

        @param value TRUE if reset is required, FALSE otherwise
        """

    @abstractmethod
    def get_actors(self, *, name: str = None, actor_type: int = None) -> List[ABCActorMixin]:
        """
        Retrieves the actors defined in this container
        @return vector of properties
        @throws Exception in case of error
        """

    @abstractmethod
    def get_actor(self, *, actor_name: str) -> ABCActorMixin:
        """
        Retrieves the actors defined in this container
        @param actor_name actor name
        @return vector of properties
        @throws Exception in case of error
        """

    @abstractmethod
    def add_actor(self, *, actor: ABCActorMixin):
        """
        Adds a new actor record to the database
        @param actor actor to be added
        @throws Exception in case of error
        """

    @abstractmethod
    def remove_actor(self, *, actor_name: str):
        """
        Removes the specified actor record
        @param actor_name actor name
        @throws Exception in case of error
        """

    @abstractmethod
    def remove_actor_database(self, *, actor_name: str):
        """
        Destroy the database for this actor. Applies to actors storing their
        database on the same database server as the container database.
        @param actor_name actor name
        @throws Exception in case of error
        """

    @abstractmethod
    def add_time(self, *, properties: dict):
        """
        Adds the time record to the database
        @params properties: properties
        """

    @abstractmethod
    def get_time(self) -> dict:
        """
        Retrieves the time record from the database
        @return map of props
        """
