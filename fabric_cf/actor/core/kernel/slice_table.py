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
from typing import TYPE_CHECKING, List

import threading

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import SliceException

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_kernel_slice import ABCKernelSlice
    from fabric_cf.actor.core.util.id import ID


class SliceTable:
    def __init__(self):
        self.lock = threading.Lock()
        self.slices = {}
        self.inventory_slices = {}
        self.client_slices = {}
        self.broker_client_slices = {}
        # A map of sets of slices indexed by name. Name is not guaranteed
        # to be unique per slice.
        self.slices_by_name = {}

    def add(self, *, slice_object: ABCKernelSlice):
        """
        Adds the given slice to the slice table.

        @param slice_object slice to add

        @throws Exception if the slice is invalid
        @throws Exception if the slice is already present in the table
        """
        if slice_object.get_slice_id() is None or slice_object.get_name() is None:
            raise SliceException(Constants.INVALID_ARGUMENT)

        try:
            self.lock.acquire()

            if slice_object.get_slice_id() in self.slices:
                raise SliceException("Already exists")

            if slice_object.is_inventory():
                self.inventory_slices[slice_object.get_slice_id()] = slice_object

            elif slice_object.is_broker_client():
                self.broker_client_slices[slice_object.get_slice_id()] = slice_object

            elif slice_object.is_client():
                self.client_slices[slice_object.get_slice_id()] = slice_object

            else:
                raise SliceException("Unsupported slice type")

            self.slices[slice_object.get_slice_id()] = slice_object

            temp_dict = None
            if slice_object.get_name() in self.slices_by_name:
                temp_dict = self.slices_by_name[slice_object.get_name()]
                temp_dict[slice_object.get_slice_id()] = slice_object
            else:
                temp_dict = {slice_object.get_slice_id(): slice_object}

            self.slices_by_name[slice_object.get_name()] = temp_dict
        finally:
            self.lock.release()

    def contains(self, *, slice_id: ID) -> bool:
        """
        Checks if the specified slice is contained in the table.

        @param slice_id identifier of slice to check

        @return true if the slice is contained in the table, false otherwise
        """
        ret_val = False
        try:
            self.lock.acquire()
            ret_val = slice_id in self.slices.keys()
        finally:
            self.lock.release()
        return ret_val

    def get(self, *, slice_id: ID, raise_exception: bool = False) -> ABCKernelSlice:
        """
        Returns the specified slice.

        @param slice_id identifier of slice to return
        @param raise_exception: raise_exception

        @return slice or null if the slice is not present in the table
        """
        if slice_id is None:
            raise SliceException("Invalid argument")

        result = None
        try:
            self.lock.acquire()

            if slice_id in self.slices:
                result = self.slices[slice_id]
        finally:
            self.lock.release()
        if raise_exception and result is None:
            raise SliceException("not registered")
        return result

    def get_slice_name(self, *, slice_name: str) -> list:
        """
        Returns all slices with the given name.

        @param slice_name slice name

        @return an list of slices with the given name
        """
        ret_val = None
        try:
            self.lock.acquire()
            if slice_name in self.slices_by_name:
                temp_dict = self.slices_by_name[slice_name]
                ret_val = []
                for v in temp_dict.values():
                    ret_val.append(v)
        finally:
            self.lock.release()
        return ret_val

    def get_broker_client_slices(self) -> list:
        """
        Returns all broker client slices in the table.

        @return a list of broker client slices
        """
        ret_val = None
        try:
            self.lock.acquire()
            ret_val = []
            for v in self.broker_client_slices.values():
                ret_val.append(v)
        finally:
            self.lock.release()

        return ret_val

    def get_client_slices(self) -> List[ABCKernelSlice]:
        """
        Returns all client slices in the table.

        @return an array of client slices
        """
        ret_val = None
        try:
            self.lock.acquire()
            ret_val = []
            for v in self.client_slices.values():
                ret_val.append(v)
            for v in self.broker_client_slices.values():
                ret_val.append(v)
        finally:
            self.lock.release()
        return ret_val

    def get_inventory_slices(self) -> List[ABCKernelSlice]:
        """
        Returns all inventory slices in the table.

        @return a list of inventory slices
        """
        ret_val = None
        try:
            self.lock.acquire()
            ret_val = []
            for v in self.inventory_slices.values():
                ret_val.append(v)
        finally:
            self.lock.release()
        return ret_val

    def get_slices(self) -> List[ABCKernelSlice]:
        """
        Returns all slices in the table.

        @return a list of slices
        """
        ret_val = None
        try:
            self.lock.acquire()
            ret_val = []
            for v in self.slices.values():
                ret_val.append(v)
        finally:
            self.lock.release()
        return ret_val

    def remove(self, *, slice_id: ID):
        """
        Removes the specified slice.

        @param slice_id identifier of slice to remove

        @throws Exception if the specified slice is not in the table
        """
        if slice_id is None:
            raise SliceException("Invalid argument")

        try:
            self.lock.acquire()
            slice_obj = self.slices.pop(slice_id)

            if slice_obj is None:
                raise SliceException("not registered")

            if slice_obj.is_inventory():
                self.inventory_slices.pop(slice_id)
            elif slice_obj.is_broker_client():
                self.broker_client_slices.pop(slice_id)
            elif slice_obj.is_client():
                self.client_slices.pop(slice_id)
            else:
                raise SliceException("Unsupported slice type")
            if slice_obj.get_name() in self.slices_by_name:
                entry = self.slices_by_name[slice_obj.get_name()]
                if entry is not None:
                    entry.pop(slice_obj.get_slice_id())
                    if len(entry) == 0:
                        self.slices_by_name.pop(slice_obj.get_name())
                    else:
                        self.slices_by_name[slice_obj.get_name()] = entry
        finally:
            self.lock.release()
