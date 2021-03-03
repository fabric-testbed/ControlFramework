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
from typing import Dict, List

from fim.user import Labels, Capacities


class Neo4jGraphNode:
    def __init__(self, node_id:str):
        self.node_id = node_id
        self.capacities = None
        self.labels = None
        self.capacity_delegations = {}
        self.label_delegations = {}
        self.components = {}
        self.components_by_type = {}
        self.resource_type = None
        self.resource_model = None

    def get_resource_type(self):
        return self.resource_type

    def get_resource_model(self):
        return self.resource_model

    def get_node_id(self) -> str:
        return self.node_id

    def get_capacities(self) -> Capacities:
        return self.capacities

    def get_capacity_delegations(self) -> Dict[str, Capacities]:
        return self.capacity_delegations

    def get_label_delegations(self) -> Dict[str, Labels]:
        return self.label_delegations

    def get_labels(self) -> Labels:
        return self.labels

    def get_components(self) -> dict:
        return self.components

    def get_component(self, node_id: str):
        return self.components.get(node_id, None)

    def set_node_id(self, * node_id: str):
        self.node_id = node_id

    def set_resource_type(self, *, resource_type):
        self.resource_type = resource_type

    def set_resource_model(self, *, resource_model):
        self.resource_model = resource_model

    def set_labels(self, *, labels: Labels):
        self.labels = labels

    def set_capacities(self, *, capacities: Capacities):
        self.capacities = capacities

    def add_label_delegation(self, *, del_id: str, labels: Labels):
        self.label_delegations[del_id] = labels

    def add_capacity_delegation(self, *, del_id: str, capacities: Capacities):
        self.capacity_delegations[del_id] = capacities

    def add_component(self, *, component):
        if component is None:
            return
        self.components[component.get_node_id()] = component

        if component.get_resource_type() not in self.components_by_type:
            self.components_by_type[component.get_resource_type()] = []
        self.components_by_type[component.get_resource_type()].append(component.get_node_id())

    def __str__(self):
        return f"[node_id: {self.node_id} capacities: {self.capacities} " \
               f"capacity_delegations: {self.capacity_delegations} labels: {self.labels} " \
               f"label_delegations: {self.label_delegations} components: {self.components}]"

    def print_components(self):
        for key, value in self.components.items():
            print(f"Key: {key} Value: {value}")

        for key, value in self.components_by_type.items():
            print(f"ResourceType: {key} value: {value}")

    def get_components_by_type(self, *, resource_type: str) -> List[str]:
        return self.components_by_type.get(resource_type, None)
