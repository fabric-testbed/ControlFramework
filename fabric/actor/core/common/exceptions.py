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


class DelegationNotFoundException(Exception):
    """
    Delegation Not Found Exception
    """
    def __init__(self, *, text: str = None, did=None):
        super(DelegationNotFoundException, self).__init__()
        if text is not None:
            self.text = str(text)
        else:
            self.text = "Delegation# {} not found".format(did)
        self.did = did

    def __str__(self):
        return self.text


class ReservationNotFoundException(Exception):
    """
    Reservation not found exception
    """
    def __init__(self, *, text: str = None, rid=None):
        super(ReservationNotFoundException, self).__init__()
        if text is not None:
            self.text = str(text)
        else:
            self.text = "Reservation# {} not found".format(rid)
        self.rid = rid

    def __str__(self):
        return self.text


class SliceNotFoundException(Exception):
    """
    Slice not found exception
    """
    def __init__(self, *, text: str = None, slice_id=None):
        super(SliceNotFoundException, self).__init__()
        if text is not None:
            self.text = str(text)
        else:
            self.text = "Slice# {} not found".format(slice_id)
        self.slice_id = slice_id

    def __str__(self):
        return self.text
