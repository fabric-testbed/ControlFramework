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
from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.exceptions import ActorException
from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.core.kernel.resource_set import ResourceSet


class Policy(ABCPolicy):
    """
    Base class for all policy implementations.
    """
    def __init__(self, *, actor: ABCActorMixin = None):
        """
        Creates a new instance.
        @params actor : actor this policy belongs to
        """
        self.initialized = False
        if actor is not None:
            self.actor = actor
            self.logger = actor.get_logger()
            self.clock = actor.get_actor_clock()
        else:
            self.actor = None
            self.logger = None
            self.clock = None
            self.guid = ID()
        self.guid = ID()
        self.properties = None

    def close(self, *, reservation: ABCReservationMixin):
        """
        Close a reservation
        @params reservation: reservation about to be closed
        """

    def closed(self, *, reservation: ABCReservationMixin):
        """
        Notifies the policy that a reservation has been closed.
        @params reservation: reservation about to be closed
        """

    def configuration_complete(self, *, action: str, token: ConfigToken, out_properties: dict):
        """
        Notifies the policy that a configuration action for the object
        represented by the token parameter has completed.

        @params action : configuration action. See Config.Target*
        @params token : object or a token for the object whose configuration action has completed
        @params outProperties : output properties produced by the configuration action
        """

    def error(self, *, message: str):
        """
        Logs the specified error and throws an exception.
        @params message: error message
        @raises Exception in case of error
        """
        self.logger.error(message)
        raise ActorException(message)

    def extend(self, *, reservation: ABCReservationMixin, resources: ResourceSet, term: Term):
        return

    def finish(self, *, cycle: int):
        return

    def get_closing(self, *, cycle: int) -> ReservationSet:
        return None

    def get_guid(self) -> ID:
        return self.guid

    def initialize(self):
        """
        Initialize the policy object
        """
        if not self.initialized:
            if self.actor is None:
                raise ActorException("Missing actor")

            if self.logger is None:
                self.logger = self.actor.get_logger()

            if self.logger is None:
                raise ActorException("Missing logger")

            if self.clock is None:
                self.clock = self.actor.get_actor_clock()

            if self.clock is None:
                raise ActorException("Missing clock")

            self.initialized = True

    def internal_error(self, *, message: str):
        """
        Logs the specified error and throws an exception.
        @params message: error message
        @raises Exception in case of error
        """
        self.logger.error("Internal error: " + message)
        raise ActorException("Internal error: " + message)

    def log_error(self, *, message: str):
        """
        logs an error
        @params message: error message
        """
        self.logger.error("Internal mapper error: " + message)

    def log_warn(self, *, message: str):
        """
        logs a warning
        @params message: warning message
        """
        self.logger.warning("Internal mapper warning: " + message)

    def prepare(self, *, cycle: int):
        return

    def query(self, *, p):
        return None

    def remove(self, *, reservation: ABCReservationMixin):
        return

    def reset(self):
        return

    def recovery_starting(self):
        return

    def revisit(self, *, reservation: ABCReservationMixin):
        return

    def revisit_delegation(self, *, delegation: ABCDelegation):
        return

    def recovery_ended(self):
        return

    def set_actor(self, *, actor: ABCActorMixin):
        self.actor = actor
        if actor is not None:
            self.logger = actor.get_logger()

    def closed_delegation(self, *, delegation: ABCDelegation):
        return

    def set_logger(self, logger):
        self.logger = logger

    def set_properties(self, properties: dict):
        self.properties = properties