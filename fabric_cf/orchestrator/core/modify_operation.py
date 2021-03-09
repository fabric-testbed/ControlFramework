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
from fabric_cf.orchestrator.core.reservation_id_with_modify_index import ReservationIDWithModifyIndex


class ModifyOperation:
    def __init__(self, *, rid: ID, index: int, modify_sub_command: str, properties: dict):
        self.res_id = ReservationIDWithModifyIndex(rid=rid, index=index)
        self.modify_sub_command = modify_sub_command
        self.properties = properties

    def get(self) -> ReservationIDWithModifyIndex:
        """
        Get Index
        :return:
        """
        return self.res_id

    def get_sub_command(self) -> str:
        """
        Get Sub Command
        :return:
        """
        return self.modify_sub_command

    def get_properties(self) -> dict:
        """
        Get properties
        :return:
        """
        return self.properties

    def override_index(self, *, index: int):
        """
        Override index
        :param index:
        :return:
        """
        self.res_id.override_modify_index(index=index)

    def __str__(self):
        return "{}/{}".format(self.modify_sub_command, self.res_id)
