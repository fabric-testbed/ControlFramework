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
import traceback
from typing import Tuple

from fim.slivers.attached_components import ComponentType
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver, ServiceType

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
from fabric_cf.actor.core.util.utils import sliver_to_str
from fabric_cf.actor.handlers.handler_base import HandlerBase


class NoOpHandler(HandlerBase):
    def __process_node_sliver_modify(self, *, existing: NodeSliver, new: NodeSliver):
        # Validate both existing and new sliver
        self.__process_node_sliver(sliver=existing)
        self.__process_node_sliver(sliver=new)
        #diff = sliver_diff(sliver1=existing, sliver2=new)
        diff = existing.diff(other_sliver=new)
        for x in diff.added.components:
            # invoke PCI attach
            self.get_logger().info(f"PCI attach for component {x}")

        for x in diff.removed.components:
            # invoke PCI detach
            self.get_logger().info(f"PCI detach for component {x}")

    def __process_node_sliver(self, *, sliver: NodeSliver):
        assert (sliver.label_allocations is not None)
        assert (sliver.label_allocations.instance_parent is not None)
        assert (sliver.get_capacity_hints() is not None)
        assert (sliver.get_capacity_hints().instance_type is not None)
        assert (sliver.get_image_ref() is not None)
        assert (sliver.get_name() is not None)
        assert (sliver.get_type() is not None)
        if sliver.attached_components_info is not None:
            for component in sliver.attached_components_info.devices.values():
                assert (component.label_allocations is not None)
                if component.get_type() != ComponentType.Storage:
                    assert (component.label_allocations.bdf is not None)
                else:
                    assert (component.label_allocations.local_name is not None)
                    component.label_allocations.device_name = '/dev/vdb'
        sliver.label_allocations.instance = 'instance_001'
        sliver.management_ip = '1.2.3.4'

        nic_types = [ComponentType.SharedNIC, ComponentType.SmartNIC]
        if sliver.attached_components_info is not None:
            for c in sliver.attached_components_info.devices.values():
                if c.get_type() not in nic_types:
                    continue
                self.get_logger().debug(f"c: {c}")
                if c.network_service_info is not None and c.network_service_info.network_services is not None:
                    for ns in c.network_service_info.network_services.values():
                        self.get_logger().debug(f"ns: {ns}")
                        if ns.interface_info is not None and ns.interface_info.interfaces is not None:
                            for i in ns.interface_info.interfaces.values():
                                self.get_logger().debug(f"ifs: {i}")

    def __process_ns_sliver(self, *, sliver: NetworkServiceSliver):
        assert (sliver.get_type() is not None)
        assert (sliver.interface_info is not None)
        for interface in sliver.interface_info.interfaces.values():
            assert (interface.labels is not None)
            assert (interface.labels.device_name is not None)
            assert (interface.labels.local_name is not None)
            assert (interface.capacities is not None)

            if sliver.get_type() == ServiceType.L2PTP:
                assert (interface.labels.vlan is not None)

            if sliver.get_type() == ServiceType.FABNetv4:
                assert (sliver.get_gateway() is not None)
                assert (sliver.get_gateway().lab.ipv4_subnet is not None)
                assert (sliver.get_gateway().lab.ipv4 is not None)
                assert (interface.labels.vlan is not None)

            if sliver.get_type() == ServiceType.FABNetv6:
                assert (sliver.get_gateway() is not None)
                assert (sliver.get_gateway().lab.ipv6_subnet is not None)
                assert (sliver.get_gateway().lab.ipv6 is not None)
                assert (interface.labels.vlan is not None)

    def create(self, unit: ConfigToken) -> Tuple[dict, ConfigToken]:
        result = None
        try:
            self.get_logger().info(f"Create invoked for unit: {unit}")
            sliver = unit.get_sliver()
            self.get_logger().info(f"Creating sliver: {sliver_to_str(sliver=sliver)}")

            time.sleep(10)

            if isinstance(sliver, NodeSliver):
                self.__process_node_sliver(sliver=sliver)

            elif isinstance(sliver, NetworkServiceSliver):
                self.__process_ns_sliver(sliver=sliver)

            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_CREATE,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_OK,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
        except Exception as e:
            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_CREATE,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_EXCEPTION,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
            self.get_logger().error(e)
            self.get_logger().error(traceback.format_exc())
        finally:

            self.get_logger().info(f"Create completed")
        return result, unit

    def delete(self, unit: ConfigToken) -> Tuple[dict, ConfigToken]:
        result = None
        try:
            self.get_logger().info(f"Delete invoked for unit: {unit}")
            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_DELETE,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_OK,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
        except Exception as e:
            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_DELETE,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_EXCEPTION,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
            self.get_logger().error(e)
            self.get_logger().error(traceback.format_exc())
        finally:

            self.get_logger().info(f"Delete completed")
        return result, unit

    def modify(self, unit: ConfigToken) -> Tuple[dict, ConfigToken]:
        result = None
        try:
            self.get_logger().info(f"Modify invoked for unit: {unit} type{unit.get_modified()}")
            sliver = unit.get_modified()
            self.get_logger().info(f"Modifying sliver: {sliver_to_str(sliver=sliver)}")

            if isinstance(sliver, NodeSliver):
                self.__process_node_sliver_modify(existing=unit.get_sliver(), new=sliver)

            elif isinstance(sliver, NetworkServiceSliver):
                self.__process_ns_sliver(sliver=sliver)

            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_MODIFY,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_OK,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
        except Exception as e:
            self.get_logger().error(e)
            self.get_logger().error(traceback.format_exc())
            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_MODIFY,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_EXCEPTION,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
        finally:
            self.get_logger().info(f"Modify completed")
        return result, unit

    def clean_restart(self):
        pass
