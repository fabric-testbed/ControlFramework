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

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_slice import ISlice
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.util.resource_data import ResourceData


class ISliceFactory:
    """
    Factory for slice objects.
    """
    @staticmethod
    def create(*, slice_id: ID, name: str = None, data: ResourceData = None) -> ISlice:
        """
        Creates a new slice with the specified id, name, and resource
        properties.

        @param slice_id slice id
        @param name slice name
        @param data slice properties

        @return slice object
        """
        raise NotImplementedError("Should have implemented this")

    @staticmethod
    def create_instance(*, properties: dict) -> ISlice:
        """
        Creates a new slice object by restoring it from the pickled instance read from the database
        @param properties dictionary containing the pickled instance read from the database
        """
        raise NotImplementedError("Should have implemented this")
