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

from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.apis.abc_slice import ABCSlice
from fabric_cf.actor.core.core.inventory_slice_manager import InventorySliceManager
from fabric_cf.actor.core.plugins.substrate.substrate_mixin import SubstrateMixin

if TYPE_CHECKING:
    from fabric_cf.actor.core.core.actor import ActorMixin
    from fabric_cf.actor.core.plugins.handlers.handler_processor import HandlerProcessor
    from fabric_cf.actor.core.apis.abc_substrate_database import ABCSubstrateDatabase


class AuthoritySubstrate(SubstrateMixin):
    def __init__(self, *, actor: ActorMixin, db: ABCSubstrateDatabase, handler_processor: HandlerProcessor):
        super().__init__(actor=actor, db=db, handler_processor=handler_processor)
        self.inventory_slice_manager = None
        self.initialized = False

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['initialized']
        del state['inventory_slice_manager']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.resource_delegation_factory = None
        self.actor = None

        self.initialized = False
        self.inventory_slice_manager = None

    def initialize(self):
        if not self.initialized:
            super().initialize()
            self.inventory_slice_manager = InventorySliceManager(db=self.get_database(), identity=self.actor,
                                                                 logger=self.get_logger())
            self.initialized = True

    def get_inventory_slice_manager(self) -> InventorySliceManager:
        return self.inventory_slice_manager

    def revisit(self, *, slice_obj: ABCSlice = None, reservation: ABCReservationMixin = None, delegation: ABCDelegation = None):
        if slice_obj is not None and slice_obj.is_inventory():
            self.logger.debug("Recovering inventory slice")
            self.recover_inventory_slice(slice_obj=slice_obj)

    def recover_inventory_slice(self, *, slice_obj: ABCSlice):
        if slice_obj.get_graph_id() is not None:
            self.actor.load_model(graph_id=slice_obj.get_graph_id())