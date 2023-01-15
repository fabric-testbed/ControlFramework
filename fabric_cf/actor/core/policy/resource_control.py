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

from abc import abstractmethod
from typing import TYPE_CHECKING

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import PolicyException
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.apis.abc_resource_control import ABCResourceControl
from fabric_cf.actor.core.util.resource_type import ResourceType

if TYPE_CHECKING:
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
    from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.core.unit import Unit


class ResourceControl(ABCResourceControl):
    def __init__(self):
        self.guid = ID()
        self.types = set()
        self.authority = None
        self.logger = None
        self.initialized = False

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['authority']
        del state['logger']
        del state['initialized']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.authority = None
        self.logger = None
        self.initialized = False

    def initialize(self):
        if not self.initialized:
            if self.authority is None:
                raise PolicyException(Constants.NOT_SPECIFIED_PREFIX.format("authority"))
            self.logger = self.authority.get_logger()
            self.initialized = True

    def add_type(self, *, rtype: ResourceType):
        self.types.add(rtype)

    def remove_type(self, *, rtype: ResourceControl):
        self.types.remove(rtype)

    def available(self, *, resource_set: ResourceSet):
        return

    def unavailable(self, *, resource_set: ResourceSet) -> int:
        return -1

    def eject(self, *, resource_set: ResourceSet):
        return

    def failed(self, *, resource_set: ResourceSet):
        return

    def recovered(self, *, resource_set: ResourceSet):
        return

    def _free_resources(self, *, resource_set: ResourceSet):
        group = resource_set.get_resources()
        if group is None:
            raise PolicyException(Constants.NOT_SPECIFIED_PREFIX.format("concrete set"))
        self.free(uset=group.get_set())

    def freed(self, *, resource_set: ResourceSet):
        self._free_resources(resource_set=resource_set)

    def release(self, *, resource_set: ResourceSet):
        self._free_resources(resource_set=resource_set)

    def correct_deficit(self, *, reservation: ABCAuthorityReservation) -> ResourceSet:
        print("KOMAL correct_deficit")
        return self.assign(reservation=reservation)

    def configuration_complete(self, *, action: str, token: ConfigToken, out_properties: dict):
        self.logger.debug("configuration complete")

    def close(self, *, reservation: ABCReservationMixin):
        self.logger.debug("close")

    def recovery_starting(self):
        return

    def recovery_ended(self):
        return

    @abstractmethod
    def free(self, *, uset: dict):
        """
        Free the Unit Set
        @param uset: unit set
        """

    def fail(self, *, u: Unit, message: str, e: Exception = None):
        if e is not None:
            self.logger.error(e)
        else:
            self.logger.error(message)
        u.fail(message=message, exception=e)

    def get_types(self) -> set:
        return self.types.copy()

    def register_type(self, *, rtype: ResourceType):
        self.types.add(rtype)

    def set_actor(self, *, actor: ABCActorMixin):
        self.authority = actor
        if self.authority is not None:
            self.logger = self.authority.get_logger()

    def get_guid(self) -> ID:
        return self.guid
