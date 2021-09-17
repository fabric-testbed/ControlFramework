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
import time
import unittest

from fim.slivers.attached_components import ComponentSliver, AttachedComponentsInfo
from fim.slivers.network_node import NodeSliver
from fim.user import NodeType, Capacities, Labels, InstanceCatalog, CapacityHints, ComponentType, ReservationInfo

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.unit import Unit
from fabric_cf.actor.core.plugins.handlers.ansible_handler_processor import AnsibleHandlerProcessor
from fabric_cf.actor.core.plugins.handlers.configuration_mapping import ConfigurationMapping
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType


class AnsibleHandlerProcessorTest(unittest.TestCase):
    from fabric_cf.actor.core.container.globals import Globals
    Globals.config_file = "../../config/config.test.yaml"
    Constants.SUPERBLOCK_LOCATION = './state_recovery.lock'

    from fabric_cf.actor.core.container.globals import GlobalsSingleton
    GlobalsSingleton.get().start(force_fresh=True)
    while not GlobalsSingleton.get().start_completed:
        time.sleep(0.0001)

    def create_sliver(self, name: str, pci_address: str, gpu_name: str) -> Unit:
        u = Unit(rid=ID(uid=name))
        sliver = NodeSliver()
        cap = Capacities()
        cap.set_fields(core=2, ram=6, disk=10)
        sliver.set_properties(type=NodeType.VM, site="UKY", capacity_allocations=cap)
        sliver.label_allocations = Labels().set_fields(instance_parent="uky-w2.fabric-testbed.net")
        catalog = InstanceCatalog()
        instance_type = catalog.map_capacities_to_instance(cap=cap)
        cap_hints = CapacityHints().set_fields(instance_type=instance_type)
        sliver.set_properties(capacity_hints=cap_hints,
                              capacity_allocations=catalog.get_instance_capacities(instance_type=instance_type))

        sliver.set_properties(name=name)

        sliver.set_properties(image_type='qcow2', image_ref='default_ubuntu_20')

        component = ComponentSliver()
        labels = Labels()
        labels.set_fields(bdf=pci_address)
        component.set_properties(type=ComponentType.GPU, model='Tesla T4', name=gpu_name, label_allocations=labels)
        sliver.attached_components_info = AttachedComponentsInfo()
        sliver.attached_components_info.add_device(device_info=component)

        u.set_sliver(sliver=sliver)
        u.set_resource_type(rtype=ResourceType(resource_type=NodeType.VM.name))
        return u

    def test_create(self):
        ap = AnsibleHandlerProcessor()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        ap.set_plugin(plugin=GlobalsSingleton.get().get_container().get_actor().get_plugin())
        ap.set_logger(logger=GlobalsSingleton.get().get_logger())

        mapping = ConfigurationMapping()
        mapping.set_class_name(class_name="NoOpHandler")
        mapping.set_module_name(module_name="fabric_cf.actor.handlers.no_op_handler")
        mapping.set_key(key=NodeType.VM.name)
        ap.add_config_mapping(mapping=mapping)

        ap.start()

        u1 = self.create_sliver(name="n1", pci_address="0000:25:00.0", gpu_name="gpu1")
        ap.create(unit=u1)

        u2 = self.create_sliver(name="n2", pci_address="0000:81:00.0", gpu_name="gpu2")
        ap.create(unit=u2)

        time.sleep(30)
        self.assertIsNotNone(u1.sliver.label_allocations.instance)
        self.assertIsNotNone(u1.sliver.management_ip)

        self.assertIsNotNone(u2.sliver.label_allocations.instance)
        self.assertIsNotNone(u2.sliver.management_ip)

        ap.shutdown()

