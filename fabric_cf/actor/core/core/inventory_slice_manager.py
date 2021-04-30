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

import traceback
from enum import Enum
from typing import TYPE_CHECKING

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ResourcesException
from fabric_cf.actor.core.kernel.slice import SliceFactory

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_identity import ABCActorIdentity
    from fabric_cf.actor.core.apis.abc_database import ABCDatabase
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.util.resource_type import ResourceType
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice


class InventorySliceManagerError(Enum):
    """
    Enumeration for Inv Slice Manager errors
    """
    ErrorNone = 0
    ErrorPoolExists = -10
    ErrorTypeExists = -20
    ErrorInvalidArguments = -30
    ErrorDatabaseError = -40
    ErrorInternalError = -50


class CreateInventorySliceResult:
    """
    Result of Create Inv Slice
    """
    def __init__(self):
        self.code = InventorySliceManagerError.ErrorNone
        self.slice = None


class InventorySliceManager:
    """
    Implements the class responsible for creating inventory slices on startup
    """
    def __init__(self, *, db: ABCDatabase, identity: ABCActorIdentity, logger):
        if db is None or identity is None or logger is None:
            raise ResourcesException("Invalid arguments {} {} {}".format(db, identity, logger))
        self.db = db
        self.identity = identity
        self.logger = logger

    def create_inventory_slice(self, *, slice_id: ID, name: str, rtype: ResourceType) -> CreateInventorySliceResult:
        """
        Create Inventory Pool at boot
        @param slice_id slice id
        @param name name
        @param rtype resource type
        """
        result = CreateInventorySliceResult()
        if slice_id is None or name is None or rtype is None:
            result.code = InventorySliceManagerError.ErrorInvalidArguments
            return result
        try:
            temp = self.db.get_slice(slice_id=slice_id)

            if temp is not None:
                result.code = InventorySliceManagerError.ErrorPoolExists
                return result

            slice_list = self.db.get_inventory_slices()
            if slice_list is not None and len(slice_list) > 0:
                for slice_obj in slice_list:
                    rt = slice_obj.get_resource_type()
                    if rt == rtype:
                        result.slice = slice_obj
                        result.code = InventorySliceManagerError.ErrorTypeExists
                        return result

            slice_obj = SliceFactory.create(slice_id=slice_id, name=name)
            slice_obj.set_inventory(value=True)
            slice_obj.set_owner(owner=self.identity.get_identity())
            slice_obj.set_resource_type(resource_type=rtype)

            try:
                self.db.add_slice(slice_object=slice_obj)
                result.slice = slice_obj
            except Exception:
                self.logger.error(traceback.format_exc())
                result.code = InventorySliceManagerError.ErrorDatabaseError
        except Exception:
            self.logger.error(traceback.format_exc())
            result.code = InventorySliceManagerError.ErrorInternalError
        return result

    def update_inventory_slice(self, *, slice_obj: ABCSlice):
        """
        Update the resource pool
        @param slice_obj slice object
        """
        try:
            slice_obj.set_dirty()
            self.db.update_slice(slice_object=slice_obj)
        except Exception as e:
            raise ResourcesException("Could not update slice {}".format(e))

    def remove_inventory_slice(self, *, slice_id: ID, rtype: ResourceType):
        """
        Remove a pool
        @param slice_id pool id
        @param rtype resource type
        """
        slice_obj = self.db.get_slice(slice_id=slice_id)

        if slice_obj is not None:
            if not slice_obj.is_inventory() or rtype != slice_obj.get_resource_type():
                raise ResourcesException(Constants.INVALID_ARGUMENT)

            self.db.remove_slice(slice_id=slice_id)
