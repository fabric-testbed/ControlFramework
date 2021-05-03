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
import traceback

from fabric_cf.actor.core.apis.abc_timer_queue import ABCTimerQueue
from fabric_cf.actor.core.apis.abc_timer_task import ABCTimerTask


class KernelTimer:
    @staticmethod
    def schedule(*, queue: ABCTimerQueue, task: ABCTimerTask, delay: int):
        """
        Schedule a timer
        :param queue: timer queue (maps to Actor class)
        :param task: task
        :param delay: delay
        :return:
        """
        try:
            queue.logger.debug("Scheduling timer")
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            scheduler = GlobalsSingleton.get().timer_scheduler
            condition = GlobalsSingleton.get().timer_condition
            with condition:
                timer = scheduler.enter(delay=delay, priority=1, action=queue.queue_timer, argument=[task])
                condition.notify_all()
            queue.logger.debug("Timer scheduled")
            return timer
        except Exception as e:
            queue.logger.error(e)
            queue.logger.error(traceback.format_exc())
