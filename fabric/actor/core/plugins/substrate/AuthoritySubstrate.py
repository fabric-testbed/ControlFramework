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

from fabric.actor.core.core.unit_set import UnitSet
from fabric.actor.core.core.pool_manager import PoolManager
from fabric.actor.core.kernel.sesource_set import ResourceSet
from fabric.actor.core.util.resource_data import ResourceData

if TYPE_CHECKING:
    from fabric.actor.core.core.actor import Actor
    from fabric.actor.core.plugins.config.config import Config
    from fabric.actor.core.apis.i_substrate_database import ISubstrateDatabase
    from fabric.actor.core.apis.i_reservation import IReservation
    from fabric.actor.core.apis.i_slice import ISlice

from fabric.actor.core.plugins.substrate.Substrate import Substrate


class AuthoritySubstrate(Substrate):
    def __init__(self, actor: Actor, db: ISubstrateDatabase, config: Config):
        super().__init__(actor, db, config)
        self.pool_manager = None
        self.initialized = False

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['ticket_factory']
        del state['actor']
        del state['initialized']
        del state['pool_manager']

        del state['db']

        return state

    def __setstate__(self, state):
        # TODO fetch actor via actor_id
        self.__dict__.update(state)
        self.logger = None
        self.ticket_factory = None
        self.actor = None
        self.initialized = None
        self.pool_manager = None

    def initialize(self):
        if not self.initialized:
            super().initialize()
            self.pool_manager = PoolManager(self.get_database(), self.actor, self.get_logger())
            self.initialized = True

    def get_pool_manager(self) -> PoolManager:
        return self.pool_manager

    def revisit(self, slice_obj: ISlice = None, reservation: IReservation = None):
        if slice_obj is not None:
            if slice_obj.is_inventory():
                self.recover_inventory_slice(slice_obj)

    def recover_inventory_slice(self, slice_obj: ISlice):
        try:
            rtype = slice_obj.get_resource_type()
            uset = self.get_units(slice_obj)
            rd = ResourceData()

            props = ResourceData.merge_properties(slice_obj.get_resource_properties(), rd.get_resource_properties())
            rd.resource_properties = props

            rset = ResourceSet(concrete=uset, rtype=rtype, rdata=rd)

            self.actor.donate(rset)
        except Exception as e:
            raise e

    def get_units(self, slice_obj: ISlice) -> UnitSet:
        # TODO recovery from database
        return None


