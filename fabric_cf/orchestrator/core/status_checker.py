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
from enum import Enum
from typing import List

from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.util.id import ID


class Status(Enum):
    OK = 0
    NOTOK = 1
    NOTREADY = 3


class StatusChecker:

    def check(self, *, controller: ABCMgmtControllerMixin, rid, ok: List[ID], not_ok: List[ID]) -> Status:
        """
        Check status
        :param controller:
        :param rid:
        :param ok:
        :param not_ok:
        :return:
        """
        result = self.check_(controller=controller, rid=0)
        if result == Status.OK:
            ok.append(rid)
        elif result == Status.NOTOK:
            not_ok.append(rid)
        return result

    def check_(self, *, controller: ABCMgmtControllerMixin, rid) -> Status:
        raise NotImplementedError
