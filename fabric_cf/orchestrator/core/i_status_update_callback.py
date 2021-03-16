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
from typing import List

from fabric_cf.actor.core.util.id import ID


class IStatusUpdateCallback:
    """
    Callbacks on reservation status updates must comply with this interface
    """
    def success(self, *, ok: List[ID], act_on: List[ID]):
        """
        All reservations in indicated group have gone to Active or have OK modify status
        :param ok - reservations that transitioned to Active or OK modify status
        :param act_on - reservations that need to be acted on
        :raises Exception in case of error
        """
        raise NotImplementedError

    def failure(self, *, failed: List[ID], ok: List[ID], act_on: List[ID]):
        """
        Some reservations may have gone into Failed or not OK modify status, so provide an action for those
        :param failed - those reservations that failed or went to not OK
        :param ok - reservations in the same group that went Active or OK
        :param act_on - reservations to be acted on
        :raises Exception in case of error
        """
        raise NotImplementedError
