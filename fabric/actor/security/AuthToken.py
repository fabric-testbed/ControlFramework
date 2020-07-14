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

from fabric.actor.core.util.ID import ID
from fabric.actor.security.Credentials import Credentials


class AuthToken:
    PropertyName = 'name'
    PropertyGuid = 'guid'
    PropertyCred = 'cred'
    """
    Represents the Authentication Token for a user
    """
    def __init__(self, name: str = None, guid: ID = None, properties: dict = None):
        self.name = name
        self.guid = guid
        self.id_token = None
        self.cred = None

        if properties is not None:
            if self.PropertyName in properties:
                self.name = properties[self.PropertyName]
            if self.PropertyGuid in properties:
                self.guid = ID(properties[self.PropertyGuid])
            if self.PropertyCred in properties:
                self.cred = Credentials(properties=properties[self.PropertyCred])

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['cred']
        del state['id_token']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.cred = None
        self.id_token = None

    def get_name(self) -> str:
        return self.name

    def get_guid(self) -> ID:
        return self.guid

    def set_credentials(self, id_token: str):
        self.cred = Credentials(id_token)

    def get_credentials(self) -> Credentials:
        return self.cred

