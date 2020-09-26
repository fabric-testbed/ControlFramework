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

import pickle
from typing import TYPE_CHECKING

from fabric.actor.core.common.constants import Constants

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_slice import ISlice
    from fabric.actor.core.util.resource_data import ResourceData

from fabric.actor.core.apis.i_slice_factory import ISliceFactory
from fabric.actor.core.kernel.slice import Slice
from fabric.actor.core.util.id import ID


class SliceFactory(ISliceFactory):
    @staticmethod
    def create(*, slice_id: ID, name: str = None, data: ResourceData = None) -> ISlice:
        return Slice(id=slice_id, name=name, data=data)

    @staticmethod
    def create_instance(*, properties: dict) -> ISlice:
        if Constants.PropertyPickleProperties not in properties:
            raise Exception("Invalid arguments")
        deserialized_slice = None
        try:
            serialized_slice = properties[Constants.PropertyPickleProperties]
            deserialized_slice = pickle.loads(serialized_slice)
        except Exception as e:
            raise e
        return deserialized_slice
