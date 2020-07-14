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

import threading
from datetime import datetime
from typing import TYPE_CHECKING

from fabric.actor.core.policy.BrokerSimplePolicy import BrokerSimplePolicy
from fabric.actor.core.policy.FIFOQueue import FIFOQueue
from fabric.actor.core.util.ReservationSet import ReservationSet

if TYPE_CHECKING:
    from fabric.actor.core.apis.IBroker import IBroker
    from fabric.actor.core.apis.IBrokerReservation import IBrokerReservation


class BrokerPriorityPolicy (BrokerSimplePolicy):
    """
    BrokerPriorityPolicy allocates requests based on requestType
    priorities set in the configuration at the broker. There may be multiple
    requestTypes with the same priority. Within each priority class, requests are
    allocated FIFO. Within each priority class the policy gives priority to
    extending reservations followed by new reservations.
    """
    PropertyRequestTypeCount = "requestType.count"
    PropertyRequestTypeName = "requestType.name"
    PropertyRequestTypePriority = "requestType.priority"
    PropertyQueueType = "queue.type"

    QueueTypeNone = "none"
    QueueTypeFifo = "fifo"

    QueueThreshold = "queueThreshold"
    # Pool the client requested its resources to be allocated from.
    PropertyPoolId = "pool.id"

    def __init__(self, actor: IBroker):
        super().__init__(actor)
        self.queue = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']

        del state['for_approval']
        del state['lock']

        del state['calendar']

        del state['last_allocation']
        del state['allocation_horizon']
        del state['ready']
        del state['queue']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.actor = None
        self.clock = None
        self.initialized = False

        self.lock = threading.Lock()
        self.calendar = None

        if self.required_approval:
            self.for_approval = ReservationSet()

        self.last_allocation = -1
        self.allocation_horizon = 0
        self.ready = False

        self.queue = None

        # TODO Fetch Actor object and setup logger, actor and clock member variables

    def configure(self, properties: dict):
        """
        Processes a list of configuration properties
        @param properties properties
        @throws Exception in case of error
        """
        super().configure(properties)

        if self.PropertyQueueType in properties:
            queue_type = properties[self.PropertyQueueType]
            if queue_type == self.QueueTypeFifo:
                self.queue = FIFOQueue()
            elif queue_type == self.QueueTypeNone:
                self.logger.debug("No queue")
            else:
                raise Exception("Unsupported queue type: {}".format(queue_type))

    def align_end(self, when: datetime) -> datetime:
        """
        Aligns the specified date with the end of the closest cycle.
       
        @param when when to align
       
        @return date aligned with the end of the closes cycle
        """
        cycle = self.clock.cycle(when=when)
        time = self.clock.cycle_end_in_millis(cycle)
        return datetime.fromtimestamp(time / 1000)

    def align_start(self, when: datetime) -> datetime:
        """
        Aligns the specified date with the start of the closest cycle.

        @param when when to align

        @return date aligned with the start of the closes cycle
        """
        cycle = self.clock.cycle(when=when)
        time = self.clock.cycle_start_in_millis(cycle)
        return datetime.fromtimestamp(time / 1000)

    def get_current_pool_id(self, reservation: IBrokerReservation) -> str:
        return str(reservation.get_source().get_type())

    def get_requested_pool_id(self, reservation: IBrokerReservation):
        if reservation.get_requested_resources() is not None:
            if reservation.get_requested_resources().get_request_properties() is not None:
                if self.PropertyPoolId in reservation.get_requested_resources().get_request_properties():
                    return reservation.get_requested_resources().get_request_properties()[self.PropertyPoolId]

        return None
