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
from fabric.actor.security.AuthToken import AuthToken
from fabric.actor.security.Guard import Guard


class AccessMonitor:
    """
    AccessMonitor encapsulates access control policy and any means of proving or gaining access to resources,
    e.g., payment. This should be an abstract pluggable class. Each operation on a slices actor includes an AuthToken
    to represent identity and any ancillary information. AccessMonitor determines whether any given access is permitted,
    given the AuthToken and a per-object auth.Guard, which encapsulates an access control list.
    """
    def __init__(self):
        return

    def check_reserve(self, guard: Guard, requester: AuthToken):
        guard.check_reserve(requester)

    def check_update(self, guard: Guard, requester: AuthToken):
        guard.check_update(requester)

    def check_proxy(self, proxy: AuthToken, requester: AuthToken):
        if requester is None:
            return proxy

        # TODO
        return requester
