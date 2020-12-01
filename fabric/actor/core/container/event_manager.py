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

from fabric.actor.core.common.constants import Constants
from fabric.actor.core.common.exceptions import EventException
from fabric.actor.core.container.event_subscription import EventSubscription
from fabric.actor.core.container.synchronous_event_subscription import SynchronousEventSubscription

if TYPE_CHECKING:
    from fabric.actor.core.container.aevent_subscription import AEventSubscription
    from fabric.actor.security.auth_token import AuthToken
    from fabric.actor.core.util.id import ID
    from fabric.actor.core.apis.i_event_handler import IEventHandler
    from fabric.actor.core.apis.i_event import IEvent


class EventManager:
    def __init__(self):
        self.subscriptions = {}

    def create_subscription(self, *, token: AuthToken, filters: list, handler: IEventHandler) -> ID:
        if token is None:
            raise EventException(Constants.invalid_argument)

        subscription = None
        if handler is None:
            subscription = EventSubscription(token=token, filters=filters)
        else:
            subscription = SynchronousEventSubscription(handler=handler, token=token, filters=filters)

        self.subscriptions[subscription.get_subscription_id()] = subscription
        return subscription.get_subscription_id()

    def delete_subscription(self, *, sid: ID, token: AuthToken):
        self.get_subscription(sid=sid, token=token)
        if sid in self.subscriptions:
            self.subscriptions.pop(sid)

    def get_subscription(self, *, sid: ID, token: AuthToken) -> AEventSubscription:
        if not sid in self.subscriptions:
            raise EventException("Invalid subscription")

        subscription = self.subscriptions[id]
        if not subscription.has_access(token=token):
            raise EventException("Access denied")
        return subscription

    def drain_events(self, *, sid: ID, token: AuthToken, timeout: int) -> list:
        subscription = self.get_subscription(sid=sid, token=token)
        return subscription.drain_events(timeout=timeout)

    def dispatch_event(self, *, event: IEvent):
        try:
            for s in self.subscriptions.values():
                if s.is_abandoned():
                    if s.get_subscription_id() in self.subscriptions:
                        self.subscriptions.pop(s.get_subscription_id())
                else:
                    s.deliver_event(event=event)
        except Exception as e:
            print("Could not dispatch event {}".format(e))
