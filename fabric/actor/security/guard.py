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
from fabric.actor.core.util.id import ID
from fabric.actor.security.auth_token import AuthToken


class Guard:
    PropertyOwner = "owner"
    PropertyObjectId = "object_id"

    def __init__(self, *, properties: dict = None):
        self.owner = None
        self.object_id = None

        if properties is not None:
            if self.PropertyOwner in properties:
                self.owner = AuthToken(properties=properties[self.PropertyOwner])

            if self.PropertyObjectId in properties:
                self.object_id = ID(id=properties[self.PropertyObjectId])

    def check_reserve(self, *, requester: AuthToken):
        self.check_privelege(requester=requester)

    def check_update(self, *, requester:AuthToken):
        self.check_privelege(requester=requester)

    def check_privelege(self, *, requester:AuthToken):
        # TDB add PDP validatation
        return

    def check_owner(self, *, auth: AuthToken):
        if self.owner is not None:
            if self.owner != auth:
                raise Exception("Authorization Exception")

    def set_owner(self, *, owner:AuthToken):
        self.owner = owner

    def set_object_id(self, *, object_id: ID):
        self.object_id = object_id

