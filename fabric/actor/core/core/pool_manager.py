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

from fabric.actor.core.kernel.slice_factory import SliceFactory

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_actor_identity import IActorIdentity
    from fabric.actor.core.apis.i_database import IDatabase
    from fabric.actor.core.util.id import ID
    from fabric.actor.core.util.resource_data import ResourceData
    from fabric.actor.core.util.resource_type import ResourceType
    from fabric.actor.core.apis.i_slice import ISlice


class PoolManager:
    ErrorNone = 0
    ErrorPoolExists = -10
    ErrorTypeExists = -20
    ErrorInvalidArguments = -30
    ErrorDatabaseError = -40
    ErrorInternalError = -50

    class CreatePoolResult:
        def __init__(self):
            self.code = PoolManager.ErrorNone
            self.slice = None

    def __init__(self, db: IDatabase, identity: IActorIdentity, logger):
        if db is None or identity is None or logger is None:
            raise Exception("Invalid arguments {} {} {}".format(db, identity, logger))
        self.db = db
        self.identity = identity
        self.logger = logger

    def create_pool(self, slice_id: ID, name: str, rtype: ResourceType, resource_data: ResourceData) -> CreatePoolResult:
        result = self.CreatePoolResult()
        if slice_id is None or name is None or rtype is None:
            result.code = self.ErrorInvalidArguments
            return result
        try:
            temp = self.db.get_slice(slice_id)

            if temp is not None and len(temp) > 0:
                result.code = self.ErrorPoolExists
                return result

            temp = self.db.get_inventory_slices()
            if temp is not None and len(temp) > 0:
                for properties in temp:
                    slice_obj = SliceFactory.create_instance(properties)
                    rt = slice_obj.get_resource_type()
                    if rt == rtype:
                        result.slice = slice_obj
                        result.code = self.ErrorTypeExists
                        return result

            slice_obj = SliceFactory.create(slice_id, name, resource_data.clone())
            slice_obj.set_inventory(True)
            slice_obj.set_owner(self.identity.get_identity())
            slice_obj.set_resource_type(rtype)

            try:
                self.db.add_slice(slice_obj)
                result.slice = slice_obj
            except Exception as e:
                result.code = self.ErrorDatabaseError
        except Exception as e:
            result.code = self.ErrorInternalError
        return result

    def update_pool(self, slice_obj: ISlice):
        try:
            self.db.update_slice(slice_obj)
        except Exception as e:
            raise Exception("Could not update slice {}".format(e))

    def remove_pool(self, pool_id: ID, rtype: ResourceType):
        temp = self.db.get_slice(pool_id)

        if temp is not None and len(temp) > 0:
            slice_obj = SliceFactory.create_instance(temp)
            if not slice_obj.is_inventory() or rtype != slice_obj.get_resource_type():
                raise Exception("Invalid arguments")

            self.db.remove_slice(pool_id)
