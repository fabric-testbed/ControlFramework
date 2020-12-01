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
from fabric.actor.core.apis.i_actor import IActor
from fabric.actor.core.apis.i_delegation import IDelegation
from fabric.actor.core.apis.i_policy import IPolicy
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.common.exceptions import ActorException
from fabric.actor.core.plugins.config.config_token import ConfigToken
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.reservation_set import ReservationSet
from fabric.actor.core.kernel.resource_set import ResourceSet


class Policy(IPolicy):
    """
    Base class for all policy implementations.
    """
    def __init__(self, *, actor: IActor = None):
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

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.actor = None
        self.clock = None
        self.initialized = False

    def close(self, *, reservation: IReservation):
        """
        Close a reservation
        @params reservation: reservation about to be closed
        """

    def closed(self, *, reservation: IReservation):
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

    def extend(self, *, reservation: IReservation, resources: ResourceSet, term: Term):
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

    def remove(self, *, reservation: IReservation):
        return

    def reset(self):
        return

    def recovery_starting(self):
        return

    def revisit(self, *, reservation: IReservation):
        return

    def revisit_delegation(self, *, delegation: IDelegation):
        return

    def recovery_ended(self):
        return

    def set_actor(self, *, actor: IActor):
        self.actor = actor

    def closed_delegation(self, *, delegation: IDelegation):
        return
