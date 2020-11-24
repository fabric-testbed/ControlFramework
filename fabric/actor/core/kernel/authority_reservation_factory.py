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

from fabric.actor.core.apis.i_authority_reservation_factory import IAuthorityReservationFactory
from fabric.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_slice import ISlice
    from fabric.actor.core.kernel.resource_set import ResourceSet
    from fabric.actor.core.time.term import Term


class AuthorityReservationFactory(IAuthorityReservationFactory):
    """
    Factory class for creating authority reservations
    """
    @staticmethod
    def create(*, resources: ResourceSet, term: Term, slice_obj: ISlice, rid: ID):
        """
        Create authority reservation
        @param resources resources
        @param term term
        @param slice_obj slice object
        @param rid reservation id
        @return Authority Reservation
        """
        from fabric.actor.core.kernel.authority_reservation import AuthorityReservation
        res = AuthorityReservation(rid=rid, resources=resources, term=term, slice_object=slice_obj)
        return res
