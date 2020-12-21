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

from fabric_cf.actor.boot.inventory.neo4j_resource_pool_factory import Neo4jResourcePoolFactory
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.apis.i_slice_factory import ISliceFactory
from fabric_cf.actor.core.common.exceptions import SliceException
from fabric_cf.actor.core.kernel.slice import Slice
from fabric_cf.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_slice import ISlice
    from fabric_cf.actor.core.util.resource_data import ResourceData


class SliceFactory(ISliceFactory):
    @staticmethod
    def create(*, slice_id: ID, name: str = None, data: ResourceData = None) -> ISlice:
        return Slice(slice_id=slice_id, name=name, data=data)

    @staticmethod
    def create_instance(*, properties: dict) -> ISlice:
        if Constants.property_pickle_properties not in properties:
            raise SliceException(Constants.invalid_argument)

        serialized_slice = properties[Constants.property_pickle_properties]
        deserialized_slice = pickle.loads(serialized_slice)
        if deserialized_slice.get_graph_id() is not None:
            graph_id = str(deserialized_slice.get_graph_id())
            arm_graph = Neo4jResourcePoolFactory.get_arm_graph(graph_id=graph_id)
            deserialized_slice.set_graph(graph=arm_graph)
        return deserialized_slice
