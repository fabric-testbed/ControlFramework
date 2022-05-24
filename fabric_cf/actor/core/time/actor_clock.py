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
from datetime import datetime, timezone

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import TimeException


class ActorClock:
    """
    ActorClock contains the conversions between different notions of time
    (millis/dates/cycles). Each container can have its own notion how long a
    cycle is as well as when time starts and this should not affect the
    correctness of the program.
    Leases start on the first millisecond of the start time of the term, and
    continue until the last millisecond of the end time of the term. The lease
    interval of a term is closed on both sides.
    """
    def __init__(self, *, beginning_of_time: int, cycle_millis: int):
        """
        Constructor
        @params beginning_of_time : time offset (milliseconds)
        @params cycle_millis : cycle length (milliseconds)
        @raise TimeException for invalid arguments
        """
        if beginning_of_time < 0 or cycle_millis < 1:
            raise TimeException("Invalid arguments {} {}".format(beginning_of_time, cycle_millis))

        self.beginning_of_time = beginning_of_time
        self.cycle_millis = cycle_millis

    def convert_millis(self, *, millis: int) -> int:
        """
        Returns the number of cycles those milliseconds represent.
        @params millis : milliseconds
        @return cycles
        @raise TimeException for invalid arguments
        """
        if millis < 0:
            raise TimeException(Constants.INVALID_ARGUMENT)

        return int(millis / self.cycle_millis)

    def cycle(self, *, when: datetime) -> int:
        """
        Converts date/milliseconds to cycles.
        @params date : date
        @return cycles
        """
        if when is None:
            raise TimeException(Constants.INVALID_ARGUMENT)

        millis = self.to_milliseconds(when=when)

        if millis < self.beginning_of_time:
            return 0

        difference = millis - self.beginning_of_time
        return int(difference / self.cycle_millis)

    def cycle_end_date(self, *, cycle: int) -> datetime:
        """
        Calculates the last millisecond of the given cycle.
        @params cycle: cycle
        @return last millisecond of the cycle
        """
        if cycle < 0:
            raise TimeException(Constants.INVALID_ARGUMENT)

        millis = (self.beginning_of_time + ((cycle + 1) * self.cycle_millis)) - 1

        return self.from_milliseconds(milli_seconds=millis)

    def cycle_end_in_millis(self, *, cycle: int) -> int:
        """
        Calculates the last millisecond of the given cycle.
        @params cycle : cycle
        @returns the last millisecond of the cycle
        """
        return int((self.cycle_start_in_millis(cycle=cycle) + self.cycle_millis) - 1)

    def cycle_start_date(self, *, cycle: int) -> datetime:
        """
        Calculates the first millisecond of the given cycle.
        @params cycle : cycle
        @returns first millisecond of the cycle
        """
        if cycle < 0:
            raise TimeException(Constants.INVALID_ARGUMENT)

        return self.date(cycle=cycle)

    def cycle_start_in_millis(self, *, cycle: int) -> int:
        """
        Calculates the first millisecond of the given cycle.
        @params cycle: cycle
        @return the first millisecond of the cycle
        """
        return int(self.beginning_of_time + (cycle * self.cycle_millis))

    def date(self, *, cycle) -> datetime:
        """
        Converts a cycle to a date.
        @params cycle: cycle
        @returns date
        @raises Exception in case of invalid arguments
        """
        if cycle < 0:
            raise TimeException(Constants.INVALID_ARGUMENT)

        millis = self.beginning_of_time + (cycle * self.cycle_millis)
        return self.from_milliseconds(milli_seconds=millis)

    def get_beginning_of_time(self) -> int:
        """
        Returns the initial time offset.
        @returns time offset
        """
        return self.beginning_of_time

    def get_cycle_millis(self) -> int:
        """
        Returns the number of milliseconds in a cycle.
        @returns milliseconds in a cycle
        """
        return self.cycle_millis

    def get_millis(self, *, cycle: int) -> int:
        """
        Returns the number of milliseconds for a specified number of
        cycles. Does not look at beginning of time. This is useful for knowing
        the length of a cycle span in milliseconds.
        @params cycle : cycle
        @returns millis for cycle cycles
        @raises Exception in case of invalid arguments
        """
        if cycle < 0:
            raise TimeException(Constants.INVALID_ARGUMENT)
        return cycle * self.cycle_millis

    @staticmethod
    def get_current_milliseconds() -> int:
        """
        Returns the current time in milliseconds.  Note that
        while the unit of time of the return value is a millisecond,
        the granularity of the value depends on the underlying
        operating system and may be larger.  For example, many
        operating systems measure time in units of tens of
        milliseconds.

        @return  the difference, measured in milliseconds, between
                  the current time and midnight, January 1, 1970 UTC.
        """
        when = datetime.now(timezone.utc)
        epoch = datetime.fromtimestamp(0, timezone.utc)
        return int((when - epoch).total_seconds() * 1000)

    @staticmethod
    def from_milliseconds(*, milli_seconds) -> datetime:
        result = datetime.fromtimestamp(milli_seconds//1000, timezone.utc).replace(microsecond=milli_seconds%1000*1000)
        return result

    @staticmethod
    def to_milliseconds(*, when: datetime) -> int:
        if when.tzinfo is not None:
            epoch = datetime.fromtimestamp(0, timezone.utc)
        else:
            epoch = datetime.fromtimestamp(0)
        return int((when - epoch).total_seconds() * 1000)
