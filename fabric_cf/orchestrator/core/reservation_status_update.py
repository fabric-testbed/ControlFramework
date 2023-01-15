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

from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fim.user import Labels, ServiceType

from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.utils import sliver_to_str
from fabric_cf.orchestrator.core.i_status_update_callback import IStatusUpdateCallback


class ReservationStatusUpdate(IStatusUpdateCallback):
    def __init__(self, *, logger: logging.Logger = None):
        self.logger = logger
        self.l3_ns = [ServiceType.FABNetv6, ServiceType.FABNetv4, ServiceType.FABNetv6Ext, ServiceType.FABNetv4Ext]

    def success(self, *, controller: ABCMgmtControllerMixin, reservation: ReservationMng):
        """
        Success callback
        :return:
        """
        if not isinstance(reservation, LeaseReservationAvro):
            return
        if reservation.redeem_processors is None:
            return

        ns_sliver = reservation.get_sliver()

        # Only send updates for L3 Network Service Reservations
        if ns_sliver.get_type() not in self.l3_ns:
            return

        self.logger.debug(f"Triggering Updates for Predecessors of res# {reservation.get_reservation_id()}")

        for pred in reservation.get_redeem_predecessors():
            result = controller.get_reservations(rid=ID(uid=pred.get_reservation_id()))
            if result is None or len(result) == 0:
                continue
            pred_res = result[0]

            # Update the sliver
            node_sliver = pred_res.get_sliver()
            modified = False
            for d in node_sliver.attached_components_info.devices.values():
                for ns in d.network_service_info.network_services.values():
                    self.logger.debug(f"Checking keys {ns_sliver.interface_info.interfaces.keys()}")
                    for ifs_name, ifs in ns.interface_info.interfaces.items():
                        if ifs.flags is None or not ifs.flags.auto_config:
                            continue
                        lookup_name = f'{node_sliver.get_name()}-{ifs_name}'
                        if lookup_name in ns_sliver.interface_info.interfaces:
                            ns_ifs = ns_sliver.interface_info.interfaces[lookup_name]
                            if ns_ifs.labels.ipv4 is not None:
                                self.logger.info(f"Updating {ifs} IP4 to {ns_ifs.labels.ipv4}")
                                ifs.label_allocations = Labels.update(ifs.label_allocations,
                                                                           ipv4=ns_ifs.labels.ipv4)
                                modified = True
                            if ns_ifs.labels.ipv6 is not None:
                                self.logger.info(f"Updating {ifs} IP6 to {ns_ifs.labels.ipv6}")
                                ifs.label_allocations = Labels.update(ifs.label_allocations,
                                                                      ipv6=ns_ifs.labels.ipv6)
                                modified = True

            if modified:
                self.logger.debug(f"Updated Node Sliver - {sliver_to_str(sliver=node_sliver)}")
                controller.modify_reservation(rid=ID(uid=pred_res.get_reservation_id()), modified_sliver=node_sliver)

    def failure(self, *, controller: ABCMgmtControllerMixin, reservation: ReservationMng):
        """
        Failure callback
        :return:
        """
        self.logger.debug(f"Reservation {reservation.get_reservation_id()} failed: Nothing to do!")
