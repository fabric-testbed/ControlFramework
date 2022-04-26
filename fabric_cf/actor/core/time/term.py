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
from fabric_cf.actor.core.time.actor_clock import ActorClock


class Term:
    """
    Represents the term for a ticket or lease (i.e., a "reservation"). A term
    specifies the range of time units during which the term is valid.

    A term consists of start and end time. The interval represented by a term is
    closed on both ends. The term's newStart field represents the
    start time of the latest term extension. For extended terms
    start is constant, while newStart and end change to reflect the extensions.

    The length of a term is measured as the number of milliseconds in the
    closed interval [newStart, end].
    This number will be returned when calling getLength(). To obtain the
    number of milliseconds in the closed interval [start, end], use getFullLength().
    """

    # Flag that controls, whether cycle numbers should be calculated.
    set_cycles: bool = True
    clock: ActorClock = None
    HH_MM_TIME_FORMAT = "%Y-%m-%d %H:%M"

    @staticmethod
    def set_clock(clock: ActorClock):
        """
        Sets the clock to use when computing cycles.
        @params clock : clock instance
        """
        Term.clock = clock

    @staticmethod
    def delta(d1: datetime, d2: datetime) -> int:
        """
        Computes the difference in microseconds between two dates.
        @params d1 : date 1
        @params d2: date 2
        @returns the difference in microseconds between two dates.
        """
        d2_ms = ActorClock.to_milliseconds(when=d2)
        d1_ms = ActorClock.to_milliseconds(when=d1)

        return d2_ms - d1_ms

    @staticmethod
    def get_readable_date(date: datetime) -> str:
        """
        Returns a readable date.
        @params date: datetime object
        @returns Returns a readable date.
        """
        return date.strftime("%Y-%m-%d %H:%M:%S.%f %Z")

    @staticmethod
    def _get_clock():
        """
        Returns the clock factory in use in this container.
        @returns clock factory
        """
        if Term.clock is None:
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            actor_container = GlobalsSingleton.get().get_container()
            if actor_container is not None:
                Term.clock = actor_container.get_actor_clock()

            if Term.clock is None:
                raise RuntimeError("Failed to obtain the actor clock from the container")

        return Term.clock

    def __init__(self, *, start: datetime = None, end: datetime = None, new_start: datetime = None, length: int = None):
        """
        Creates a new term.
        @params start: start time
        @params end: end time
        @params new_start: new start time
        @params length: length in ms
        """
        # Start time: first valid millisecond.
        if start is not None:
            self.start_time = start
        else:
            self.start_time = datetime.now(timezone.utc)
        # End time: last valid millisecond.
        if end is not None:
            self.end_time = end
        else:
            if start is not None and length is not None and length > 1:
                start_ms = ActorClock.to_milliseconds(when=start) + length - 1
                self.end_time = ActorClock.from_milliseconds(milli_seconds=start_ms)
            else:
                raise TimeException("Invalid arguments, length and end both not specified")

        # Start time for this section of the lease
        if new_start is not None:
            self.new_start_time = new_start
        else:
            if start is not None:
                self.new_start_time = start
            else:
                self.new_start_time = datetime.now(timezone.utc)
        # Start cycle. Used only for debugging.
        self.cycle_start = 0
        # End cycle. Used only for debugging.
        self.cycle_end = 0
        # New start cycle. Used only for debugging.
        self.cycle_new_start = 0
        self._set_cycles()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['cycle_start']
        del state['cycle_end']
        del state['cycle_new_start']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.cycle_start = 0
        self.cycle_end = 0
        self.cycle_new_start = 0

    def _set_cycles(self):
        """
        Set cycles
        """
        if Term.set_cycles:
            clock = self._get_clock()
            if self.start_time is not None:
                self.cycle_start = clock.cycle(when=self.start_time)
            if self.end_time is not None:
                self.cycle_end = clock.cycle(when=self.end_time)
            if self.new_start_time is not None:
                self.cycle_new_start = clock.cycle(when=self.new_start_time)

    def change_length(self, *, length: int):
        """
        Creates a new term from the term. The new term has the same start time
        but different length.
        @param length : new term length (milliseconds)
        @returns term starting at the same time as this term but with the specified length
        """
        return Term(start=self.start_time, length=length)

    def contains(self, *, date: datetime = None, term=None) -> bool:
        """
        Checks if the term contains the given date or term.
        @params date: the date to check
        @params term: Term to check
        @returns true if the term contains the given date or term; false otherwise
        """
        if date is None and term is None:
            raise TimeException(Constants.INVALID_ARGUMENT)

        if self.start_time is None or self.end_time is None:
            raise TimeException(Constants.INVALID_STATE)

        if date is not None:
            return (not self.start_time > date) and (not self.end_time < date)

        if term is not None:
            if not isinstance(term, Term):
                raise TimeException(Constants.INVALID_ARGUMENT)
            return (not self.start_time > term.start_time) and (not self.end_time < term.end_time)

        return False

    def ends_after(self, *, date: datetime) -> bool:
        """
        Checks if the term ends after the given date.
        @params date : date to check against
        @returns true if the term ends after the given date
        """
        if date is None:
            raise TimeException(Constants.INVALID_ARGUMENT)

        if self.end_time is None:
            raise TimeException(Constants.INVALID_STATE)

        return date < self.end_time

    def ends_before(self, *, date: datetime) -> bool:
        """
        Checks if the term ends after the given date.
        @params date : date to check against
        @returns true if the term ends before the given date
        """
        if date is None:
            raise TimeException(Constants.INVALID_ARGUMENT)

        if self.end_time is None:
            raise TimeException(Constants.INVALID_STATE)

        return date > self.end_time

    def enforce_extends_term(self, *, old_term):
        """
        Checks if this term extends the old one. In case this term does not
        extend old term, logs the error and throws an exception.
        @params old_term : old term
        @raises Exception if this term does not extend the old term
        """
        if not isinstance(old_term, Term):
            raise TimeException("Invalid type: {}".format(type(old_term)))

        flag = self.extends_term(old_term=old_term)
        if flag is False:
            raise TimeException("New term does not extend previous term")

    def __eq__(self, other):
        """
        Compares two terms.
        @params other other term to compare to
        @returns true if both terms are equal
        """
        if other is None:
            return False
        if not isinstance(other, Term):
            return NotImplemented

        if self.start_time is not None and self.start_time != other.start_time:
            return False

        if self.end_time is not None and self.end_time != other.end_time:
            return False

        if self.new_start_time is not None and self.new_start_time != other.new_start_time:
            return False

        return True

    def expired(self, *, date: datetime) -> bool:
        """
        Checks if the term's expiration date is before the specified time.
        @params time : the time to check against
        @returns true if the term expires before the specified time
        """
        if date is None:
            raise TimeException(Constants.INVALID_ARGUMENT)

        if self.end_time is None:
            raise TimeException(Constants.INVALID_STATE)

        return date > self.end_time

    def extend(self, *, length: int = 0):
        """
        Creates a new term as an extension of the specified term. The term is
        extended with the current term length.
        @params length new term length in milliseconds
        @returns term extended with the current term length
        """
        if self.start_time is None or self.end_time is None:
            raise TimeException(Constants.INVALID_STATE)
        length_to_use = self.get_length()
        if length != 0:
            length_to_use = length

        new_start_ms = ActorClock.to_milliseconds(when=self.end_time) + 1
        new_start = ActorClock.from_milliseconds(milli_seconds=new_start_ms)
        end = ActorClock.from_milliseconds(milli_seconds=new_start_ms + length_to_use - 1)

        return Term(start=self.start_time, end=end, new_start=new_start)

    def extends_term(self, *, old_term) -> bool:
        """
        Checks if this term extends the old term.
        @params old_term: old term to check against
        @returns true if this term extends the old one, false if not
        """
        if old_term is None or old_term.start_time is None or old_term.end_time is None:
            raise TimeException(Constants.INVALID_STATE)

        if self.start_time is None or self.end_time is None or self.new_start_time is None:
            raise TimeException(Constants.INVALID_STATE)

        start_time_hh_mm = self.start_time.strftime(self.HH_MM_TIME_FORMAT)
        old_start_time_hh_mm = old_term.start_time.strftime(self.HH_MM_TIME_FORMAT)
        return start_time_hh_mm == old_start_time_hh_mm and self.end_time > self.new_start_time

    def get_end_time(self) -> datetime:
        """
        Returns the end time
        @returns end time
        """
        return self.end_time

    def get_full_length(self) -> int:
        """
        Returns the full length of a term in milliseconds. The full length of a
        term is the number of milliseconds in the closed interval [start, end].
        @returns term length
        """
        if self.start_time is None or self.end_time is None:
            raise TimeException(Constants.INVALID_STATE)

        start_ms = ActorClock.to_milliseconds(when=self.start_time)
        end_ms = ActorClock.to_milliseconds(when=self.end_time)

        return end_ms - start_ms + 1

    def get_length(self) -> int:
        """
        Returns the length of a term in milliseconds. The length of a term is the
        number of milliseconds in the closed interval [new_start, end]
        @returns term length
        """
        if self.new_start_time is None or self.end_time is None:
            raise TimeException(Constants.INVALID_STATE)

        new_start_ms = ActorClock.to_milliseconds(when=self.new_start_time)
        end_ms = ActorClock.to_milliseconds(when=self.end_time)

        return end_ms - new_start_ms + 1

    def get_new_start_time(self) -> datetime:
        """
        returns new start time
        @returns new start time
        """
        return self.new_start_time

    def get_start_time(self) -> datetime:
        """
        Returns start time
        @returns start time
        """
        return self.start_time

    def set_end_time(self, *, date: datetime):
        """
        Set end time
        """
        self.end_time = date
        self._set_cycles()

    def set_new_start_time(self, *, date: datetime):
        """
        Set new_start time
        """
        self.new_start_time = date
        self._set_cycles()

    def set_start_time(self, *, date: datetime):
        """
        Set start time
        """
        self.start_time = date
        self._set_cycles()

    def shift(self, *, date: datetime):
        """
        Creates a new term from the term. The new term is shifted in time to
        start at the specified start time.
        @params date : start time
        @returns term starting at the specified time, with the length of the current term
        """
        if date is None:
            raise TimeException("Invalid argument")

        return Term(start=date, length=self.get_length())

    def __str__(self):
        if Term.set_cycles:
            return "term=[{}:{}:{}]".format(self.cycle_start, self.cycle_new_start, self.cycle_end)
        else:
            return "Start: {} End: {}".format(Term.get_readable_date(self.start_time),
                                              Term.get_readable_date(self.end_time))

    def validate(self):
        """
        Validates the term
        @raises Exception thrown if invalid start time for term thrown if invalid
                end time for term thrown if negative duration for term
        """
        if self.start_time is None:
            raise TimeException("Invalid start time")

        if self.end_time is None:
            raise TimeException("Invalid end time")

        if self.end_time < self.start_time:
            raise TimeException("negative duration for term")

        if self.start_time == self.end_time:
            raise TimeException("zero duration for term")

        if self.new_start_time < self.start_time:
            raise TimeException("new start before start")

        if self.new_start_time > self.end_time:
            raise TimeException("new start after end")

    def clone(self):
        return Term(start=self.start_time, new_start=self.new_start_time, end=self.end_time)
