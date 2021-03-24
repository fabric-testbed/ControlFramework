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


class ABCTick(ABC):
    """
    ITick defines the interface for objects that can be periodically "ticked".
    A tick is a timer interrupt. Ticks are identified by a
    cycle number. Cycle numbers increase monotonically with every tick. The
    interval between two consecutive ticks is the cycle length. Cycle length is
    fixed and preconfigured at system boot time.

    Implementors of this interface have to take special care to ensure that the
    code in externalTick(long) is efficient, and does not block
    unnecessarily or indefinitely. Blocking inside external tick may have
    significant consequences depending on how ticks are actually delivered.

    It is not guaranteed that cycles passed into two consecutive invocations of
    externalTick(long) will differ with by exactly 1 cycle. It is
    possible for ticks to be delivered not on consecutive cycles. Implementations
    of this interface have to take special care to ensure that they handle
    correctly skipped ticks: e.g., remember the argument of the last invocation
    and perform or operations scheduled to be executed between the last and the
    current cycle.
    """

    @abstractmethod
    def external_tick(self, *, cycle: int):
        """
        Processes a timer interrupt (a tick).

        @param cycle cycle number

        @throws Exception if a critical error occurs while processing the timer
                interrupt. In general, the implementor must catch exceptions
                internally and only pass exceptions up the call stack when
                critical conditions occur.
        """

    @abstractmethod
    def get_name(self) -> str:
        """
        Return the name
        @return name
        """
