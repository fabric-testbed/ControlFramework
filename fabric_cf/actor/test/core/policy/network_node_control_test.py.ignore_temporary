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
import logging
import time
import unittest

from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Capacities
from fim.slivers.network_node import NodeType, NodeSliver

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.delegation.delegation_factory import DelegationFactory
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.policy.network_node_control import NetworkNodeControl
from fabric_cf.actor.core.policy.resource_control import ResourceControl
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.test.core.policy.authoity_calendar_policy_test import AuthorityCalendarPolicyTest


class NetworkNodeControlTest(AuthorityCalendarPolicyTest, unittest.TestCase):
    logger = logging.getLogger('AuthorityPolicyTest')
    log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_format, filename="actor.log")
    logger.setLevel(logging.INFO)

    authority = None
    from fabric_cf.actor.core.container.globals import Globals
    Globals.config_file = "./config/config.test.yaml"
    Constants.SUPERBLOCK_LOCATION = './state_recovery.lock'

    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().start(force_fresh=True)
    while not GlobalsSingleton.get().start_completed:
        time.sleep(0.0001)

    def build_sliver(self) -> BaseSliver:
        node_sliver = NodeSliver()
        node_sliver.resource_type = NodeType.VM
        node_sliver.node_id = "test-slice-node-1"
        cap = Capacities()
        cap.set_fields(core=4, ram=64, disk=500)
        node_sliver.set_properties(name="node-1", type=NodeType.VM, site="RENC",
                                   capacities=cap, image_type='qcow2', image_ref='default_centos_8')
        node_map = tuple([self.arm.graph_id, 'HX6VQ53'])
        node_sliver.set_node_map(node_map=node_map)
        return node_sliver

    def get_control(self) -> ResourceControl:

        control = NetworkNodeControl()
        control.add_type(rtype=ResourceType(resource_type=NodeType.VM.name))
        return control

    def get_delegation(self, actor: ABCActorMixin) -> ABCDelegation:
        slice_object = SliceFactory.create(slice_id=ID(), name="inventory-slice")
        slice_object.set_inventory(value=True)
        actor.register_slice(slice_object=slice_object)

        delegation_name = 'primary'

        dlg_obj = DelegationFactory.create(did=self.adm.get_graph_id(), slice_id=slice_object.get_slice_id(),
                                           delegation_name=delegation_name)
        dlg_obj.set_slice_object(slice_object=slice_object)
        dlg_obj.set_graph(graph=self.adm)
        actor.register_delegation(delegation=dlg_obj)
        return dlg_obj
