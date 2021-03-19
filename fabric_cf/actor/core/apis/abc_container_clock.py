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
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_tick import ABCTick
    from fabric_cf.actor.core.time.actor_clock import ActorClock


class ABCContainerClock(ABC):
    """
    IContainerClock describes the time-related container API.
    """

    @abstractmethod
    def get_actor_clock(self) -> ActorClock:
        """
        Returns an instance of the container's clock factory.
        @return container clock factory
        """

    @abstractmethod
    def tick(self):
        """
        Advances the container clock with one cycle (only if isManualClock() is true).
        """

    @abstractmethod
    def get_current_cycle(self):
        """
        Returns the current cycle.
        @return current clock cycle
        """

    @abstractmethod
    def stop(self):
        """
        Stops the clock.
        @throws Exception in case of error
        """

    @abstractmethod
    def register(self, *, tickable: ABCTick):
        """
        Registers an object with the ticker.
        @param tickable object to register
        """

    @abstractmethod
    def unregister(self, *, tickable: ABCTick):
        """
        Unregisters an object with the ticker.
        @param tickable object to register
        """

    @abstractmethod
    def is_manual_clock(self) -> bool:
        """
        Checks if the container clock advances manually.
        @return true if the container clock advances manually
        """
