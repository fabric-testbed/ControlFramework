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
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import FrameworkException


class UpdateData:
    """
    UpdateData wraps state passed with a ticket or lease update. Server-side code registers status
    of operations as they execute; client-side code queries for status, and may also post status.
    """
    def __init__(self, *, message: str = None):
        self.message = message
        self.events = None
        self.failed = False
        if self.message is None:
            self.message = ""

    def absorb(self, *, other):
        """
        Merges passed UpdateData into this. Posted events are extracted
        and merged into this. The status message from this is overwritten with
        the status message from the absorbed UpdateData.
        """
        if not isinstance(other, UpdateData):
            raise FrameworkException(Constants.INVALID_ARGUMENT)

        self.post(event=other.events)
        if self.message is not None:
            self.post(event=self.message)
        self.message = other.message
        self.failed = other.failed

    def clear(self):
        """
        Clears all events.
        """
        self.events = None

    def error(self, *, message: str):
        """
        Indicates that an error has occurred.

        @param message error message
        """
        self.message = message
        self.failed = True

    def is_failed(self) -> bool:
        """
        Checks if the operation represented by the object has failed.

        @return true if the operation has failed
        """
        return self.failed

    def get_events(self) -> str:
        """
        Returns all events stored in the object.

        @return list of events. Event items are separated by "\n".
        """
        return self.events

    def get_message(self) -> str:
        """
        Returns the message attached to the object.

        @return message
        """
        return self.message

    def post(self, *, event: str):
        """
        Posts a human-readable string describing an event that the user
        may wish to know about. If the object already contains messages, the
        new message is prepended to the existing messages. Messages are
        separated using "\n".

        @param event message describing event
        """
        if self.events is None:
            self.events = event
        else:
            self.events = "{}\n{}".format(event, self.events)

    def post_error(self, *, event: str):
        """
        Posts a human-readable string describing an event that the user
        may wish to know about, and also marks the UpdateData in a failed
        state.

        @param event message describing event
        """
        self.post(event=event)
        self.error(message=event)

    def successful(self):
        """
        Checks if the operation represented by the object has succeeded.

        @return true if the operation has succeeded 
        """
        return not self.failed

    def __str__(self):
        return f"message: {self.message} events: {self.events} failed: {self.failed}"
