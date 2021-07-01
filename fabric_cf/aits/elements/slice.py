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
import json
from typing import List

from fim.slivers.capacities_labels import JSONField


class Slice(JSONField):
    """
    Class represents the slice received
    """
    def __init__(self):
        self.graph_id = None
        self.slice_id = None
        self.slice_name = None
        self.slice_state = None
        self.lease_end = None

    def set_fields(self, **kwargs):
        """
        Universal integer setter for all fields.
        Values should be non-negative integers. Throws a RuntimeError
        if you try to set a non-existent field.
        :param kwargs:
        :return: self to support call chaining
        """
        for k, v in kwargs.items():
            assert v != ''
            try:
                # will toss an exception if field is not defined
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                raise RuntimeError(f"Unable to set field {k} of reservation, no such field available")
        return self

    def to_json(self) -> str:
        """
        Dumps to JSON the __dict__ of the instance. Be careful as the fields in this
        class should only be those that can be present in JSON output.
        If there are no values in the object, returns empty string.
        :return:
        """
        d = self.__dict__.copy()
        for k in self.__dict__:
            if d[k] is None or d[k] == 0:
                d.pop(k)
        if len(d) == 0:
            return ''
        return json.dumps(d, skipkeys=True, sort_keys=True, indent=4)


class SliceFactory:
    """
    Factory class to instantiate Slice
    """
    @staticmethod
    def create_slices(*, slice_list: List[dict]) -> List[Slice]:
        """
        Create list of slice_list from JSON List
        :param slice_list slice list
        :return list of slices
        """
        result = []
        for s_dict in slice_list:
            slice_obj = SliceFactory.create(slice_dict=s_dict)
            result.append(slice_obj)

        return result

    @staticmethod
    def create(*, slice_dict: dict) -> Slice:
        """
        Create slices from JSON
        :param slice_dict slice json
        :return slice
        """
        slice_json = json.dumps(slice_dict)
        return Slice().from_json(json_string=slice_json)
