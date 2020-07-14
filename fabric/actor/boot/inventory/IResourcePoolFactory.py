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

from fabric.actor.core.apis.IClientReservation import IClientReservation
from fabric.actor.core.apis.ISlice import ISlice

if TYPE_CHECKING:
    from fabric.actor.core.apis.ISubstrate import ISubstrate
    from fabric.actor.core.common.ResourcePoolDescriptor import ResourcePoolDescriptor


class IResourcePoolFactory:
    def set_substrate(self, substrate: ISubstrate):
        """
        Sets the substrate.
        @param substrate substrate
        @throws Exception in case of error
        """
        raise Exception("Not implemented")

    def set_descriptor(self, descriptor: ResourcePoolDescriptor):
        """
        Sets the initial resource pools descriptor. The factory can modify the descriptor as needed.
        @param descriptor descriptor
        @throws Exception in case of error
        """
        raise Exception("Not implemented")

    def get_descriptor(self) -> ResourcePoolDescriptor:
        """
        Returns the final pool descriptor.
        @return ResourcePoolDescriptor
        @throws Exception in case of error
        """
        raise Exception("Not implemented")

    def create_source_reservation(self, slice_obj: ISlice) -> IClientReservation:
        """
        Returns the source reservation for this resource pool.
        @param slice_obj slice
        @return IClientReservation
        @throws Exception in case of error
        """
        raise Exception("Not implemented")