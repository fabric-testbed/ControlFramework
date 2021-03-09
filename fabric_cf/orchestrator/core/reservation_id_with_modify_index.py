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
from fabric_cf.actor.core.util.id import ID


class ReservationIDWithModifyIndex:
    def __init__(self, *, rid: ID, index: int):
        self.rid = rid
        self.index = index

    def get_reservation_id(self) -> ID:
        """
        Get Reservation Id
        :return:
        """
        return self.rid

    def get_modify_index(self) -> int:
        """
        Get Modify Index
        :return:
        """
        return self.index

    def override_modify_index(self, *, index: int):
        """
        Override modify index
        :param index:
        :return:
        """
        self.index = index

    def __str__(self):
        return "{}[{}]".format(self.rid, self.index)

    def __eq__(self, other):
        if not isinstance(other, ReservationIDWithModifyIndex):
            return False
        if self.rid == other.rid and self.index == other.index:
            return True
        return False
