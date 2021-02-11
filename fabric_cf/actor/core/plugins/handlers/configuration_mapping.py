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


class ConfigurationMapping:
    def __init__(self):
        self.type = None
        self.class_name = None
        self.properties = None
        self.module_name = None

    def __getstate__(self):
        state = self.__dict__.copy()
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def get_module_name(self) -> str:
        return self.module_name

    def get_class_name(self) -> str:
        return self.class_name

    def get_key(self) -> str:
        return self.type

    def get_properties(self) -> dict:
        return self.properties

    def set_class_name(self, *, class_name: str):
        self.class_name = class_name

    def set_module_name(self, *, module_name: str):
        self.module_name = module_name

    def set_key(self, *, key: str):
        self.type = key

    def set_properties(self, *, properties: dict):
        self.properties = properties

    def __str__(self):
        return f"resource_type: {self.type} class_name: {self.class_name} module: {self.module_name} " \
               f"properties: {self.properties}"
