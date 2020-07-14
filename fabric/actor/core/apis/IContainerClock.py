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
    from fabric.actor.core.apis.ITick import ITick
    from fabric.actor.core.time.ActorClock import ActorClock


class IContainerClock:
    """
    IContainerClock describes the time-related container API.
    """
    def get_actor_clock(self)-> ActorClock:
        """
        Returns an instance of the container's clock factory.
        @return container clock factory
        """
        raise NotImplementedError("Should have implemented this")

    def tick(self):
        """
        Advances the container clock with one cycle (only if isManualClock() is true).
        """
        raise NotImplementedError("Should have implemented this")

    def get_current_cycle(self):
        """
        Returns the current cycle.
        @return current clock cycle
        """
        raise NotImplementedError("Should have implemented this")

    def stop(self):
        """
        Stops the clock.
        @throws Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")

    def register(self, tickable: ITick):
        """
        Registers an object with the ticker.
        @param tickable object to register
        """
        raise NotImplementedError("Should have implemented this")

    def unregister(self, tickable: ITick):
        """
        Unregisters an object with the ticker.
        @param tickable object to register
        """
        raise NotImplementedError("Should have implemented this")

    def is_manual_clock(self) -> bool:
        """
        Checks if the container clock advances manually.
        @return true if the container clock advances manually
        """
        raise NotImplementedError("Should have implemented this")