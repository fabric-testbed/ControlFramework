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
import pickle

from fabric_cf.actor.core.util.id import ID
from .delegation import Delegation
from ..apis.i_actor import IActor
from ..apis.i_delegation import IDelegation
from ..apis.i_slice import ISlice
from ..common.constants import Constants
from ..common.exceptions import DelegationException


class DelegationFactory:
    """
    Factory class to create delegation instances
    """
    @staticmethod
    def create(did: ID, slice_id: ID) -> IDelegation:
        """
        Create a delegation
        @param did delegation id
        @param slice_id slice id
        @return delegation
        """
        delegation = Delegation(dlg_graph_id=did, slice_id=slice_id)
        return delegation

    @staticmethod
    def create_instance(*, properties: dict, actor: IActor, slice_obj: ISlice, logger) -> IDelegation:
        """
        Creates and initializes a new delegation from a saved
        properties list.

        @param properties properties dict
        @param actor actor
        @param slice_obj slice_obj
        @param logger logger

        @return delegation instance

        @raises Exception in case of error
        """
        if Constants.property_pickle_properties not in properties:
            raise DelegationException(Constants.invalid_argument)

        serialized_delegation = properties[Constants.property_pickle_properties]
        deserialized_delegation = pickle.loads(serialized_delegation)
        deserialized_delegation.restore(actor=actor, slice_obj=slice_obj, logger=logger)
        return deserialized_delegation
