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

from fabric.actor.core.apis.i_actor import IActor
from fabric.actor.core.util.id import ID
from fabric.actor.core.apis.i_resource_control import IResourceControl
from fabric.actor.core.util.resource_type import ResourceType

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_client_reservation import IClientReservation
    from fabric.actor.core.kernel.resource_set import ResourceSet
    from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
    from fabric.actor.core.plugins.config.config_token import ConfigToken
    from fabric.actor.core.apis.i_reservation import IReservation
    from fabric.actor.core.core.unit import Unit


class ResourceControl(IResourceControl):
    PropertySubstrateFile = "substrate.file"
    PropertyControlResourceTypes = "resource.types"

    @staticmethod
    def get_substrate_file(*, reservation: IClientReservation):
        rset = reservation.get_resources()
        substrate_file = None
        if ResourceControl.PropertySubstrateFile in rset.get_resource_properties():
            substrate_file = rset.get_resource_properties()[ResourceControl.PropertySubstrateFile]
        if substrate_file is None:
            raise Exception("Missing substrate file property")
        return substrate_file

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
                raise Exception("authority is not set")
            self.logger = self.authority.get_logger()
            self.initialized = True

    def add_type(self, *, rtype: ResourceType):
        self.types.add(rtype)

    def remove_type(self, *, rtype: ResourceControl):
        self.types.remove(rtype)

    def donate_reservation(self, *, reservation: IClientReservation):
        return

    def donate(self, *, resource_set: ResourceSet):
        return

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

    def freed(self, *, resource_set: ResourceSet):
        group = resource_set.get_resources()
        if group is None:
            raise Exception("Missing concrete set")
        self.free(uset=group.get_set())

    def release(self, *, resource_set: ResourceSet):
        group = resource_set.get_resources()
        if group is None:
            raise Exception("Missing concrete set")
        self.free(uset=group.get_set())

    def correct_deficit(self, *, reservation: IAuthorityReservation) -> ResourceSet:
        return self.assign(reservation=reservation)

    def configuration_complete(self, *, action:str, token: ConfigToken, out_properties: dict):
        self.logger.debug("configuration complete")
        return

    def close(self, *, reservation: IReservation):
        self.logger.debug("close")
        return

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

    def configure(self, *, properties: dict):
        if self.PropertyControlResourceTypes in properties:
            temp = properties[self.PropertyControlResourceTypes]
            for name in temp.split(","):
                self.add_type(rtype=ResourceType(resource_type=name))

    def get_types(self) -> set:
        return self.types.copy()

    def register_type(self, *, rtype: ResourceType):
        self.types.add(rtype)

    def set_actor(self, *, actor: IActor):
        self.authority = actor

    def get_guid(self) -> ID:
        return self.guid
