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

from fim.slivers.capacities_labels import JSONField, Capacities, Labels, CapacityHints

from .constants import Constants


class Reservation(JSONField):
    """
    Class represents the reservations received
    """
    def __init__(self):
        self.name = None
        self.site = None
        self.resource_type = None
        self.graph_node_id = None
        self.slice_id = None
        self.reservation_id = None
        self.management_ip = None
        self.capacities = None
        self.allocated_capacities = None
        self.capacity_hints = None
        self.labels = None
        self.allocated_labels = None
        self.join_state = None
        self.pending_state = None
        self.reservation_state = None
        self.notices = None
        self.lease_end = None

    def get_name(self) -> str:
        return self.name

    def get_site(self) -> str:
        return self.site

    def get_type(self) -> str:
        return self.resource_type

    def get_graph_node_id(self) -> str:
        return self.graph_node_id

    def get_slice_id(self) -> str:
        return self.slice_id

    def get_management_ip(self) -> str:
        return self.management_ip

    def get_capacities(self) -> Capacities:
        return self.capacities

    def get_labels(self) -> Labels:
        return self.labels

    def get_allocated_capacities(self) -> Capacities:
        return self.allocated_capacities

    def get_capacity_hints(self) -> CapacityHints:
        return self.capacity_hints

    def get_allocated_labels(self) -> Labels:
        return self.labels

    def get_join_state(self) -> str:
        return self.join_state

    def get_state(self) -> str:
        return self.reservation_state

    def get_pending_state(self) -> str:
        return self.pending_state

    def get_notices(self) -> str:
        return self.notices

    def set_fields(self, **kwargs):
        """
        Universal integer setter for all fields.
        Values should be non-negative integers. Throws a RuntimeError
        if you try to set a non-existent field.
        :param kwargs:
        :return: self to support call chaining
        """
        for k, v in kwargs.items():
            try:
                # will toss an exception if field is not defined
                self.__getattribute__(k)
                if k == Constants.PROP_CAPACITIES or k == Constants.PROP_ALLOCATED_CAPACITIES:
                    c = Capacities()
                    v = c.from_json(json_string=v)
                elif k == Constants.PROP_LABELS or k == Constants.PROP_ALLOCATED_LABELS:
                    l = Labels()
                    v = l.from_json(json_string=v)
                elif k == Constants.PROP_CAPACITY_HINTS:
                    ch = CapacityHints()
                    v = ch.from_json(json_string=v)
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
            if d[k] is None:
                d.pop(k)
            elif k == Constants.PROP_CAPACITIES or k == Constants.PROP_LABELS or \
                    k == Constants.PROP_ALLOCATED_CAPACITIES or k == Constants.PROP_ALLOCATED_LABELS or \
                    k == Constants.PROP_CAPACITY_HINTS:
                d[k] = d[k].to_json()
        if len(d) == 0:
            return ''
        return json.dumps(d, skipkeys=True, sort_keys=True, indent=4)


class ReservationFactory:
    """
    Factory class to instantiate Reservation
    """
    @staticmethod
    def create_reservations(*, reservation_list: List[dict]) -> List[Reservation]:
        """
        Create list of reservations from JSON List
        :param reservation_list reservation list
        :return list of reservations
        """
        result = []
        for r_dict in reservation_list:
            reservation = ReservationFactory.create(reservation_dict=r_dict)
            result.append(reservation)

        return result

    @staticmethod
    def create(*, reservation_dict: dict) -> Reservation:
        """
        Create reservations from JSON
        :param reservation_dict reservation jso
        :return reservation
        """
        reservation_json = json.dumps(reservation_dict)
        res_obj = Reservation().from_json(json_string=reservation_json)
        return res_obj
