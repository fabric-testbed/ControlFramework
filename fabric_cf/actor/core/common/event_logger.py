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
import traceback

from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.network_node import NodeType, NodeSliver
from fim.slivers.network_service import NetworkServiceSliver

from fabric_cf.actor.security.pdp_auth import ActionId


class EventLogger:
    @staticmethod
    def log_slice_event(*, logger: logging.Logger, slice_object: SliceAvro, action: ActionId, sites: dict = None,
                        components: dict = None, facilities: dict = None):
        """
        Log Slice Event for metrics
        """
        try:
            log_message = f"Slice event slc:{slice_object.get_slice_name()}:{slice_object.get_slice_id()} {action} by " \
                          f"prj:{slice_object.get_project_id()} " \
                          f"usr:{slice_object.get_owner().get_oidc_sub_claim()}:{slice_object.get_owner().get_email()}"

            if action != ActionId.delete:
                log_message += " with"

            if facilities is not None and len(facilities) > 0:
                log_message += " facilities "
                for f, count in facilities.items():
                    log_message += f"{f} ({count}),"

            if sites is not None and len(sites) > 0:
                log_message += " sites "
                for s, count in sites.items():
                    log_message += f"{s} ({count}),"

            if components is not None and len(components) > 0:
                log_message += " components "
                for c, count in components.items():
                    log_message += f"{c} ({count}),"

            logger.info(log_message)
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            logger.error(traceback.format_exc())

    @staticmethod
    def log_sliver_event(*, logger:logging.Logger, slice_object: SliceAvro, sliver: BaseSliver):
        """
        Log Sliver events
        """
        try:
            log_message = f"Sliver event slvr:{sliver.get_reservation_info().reservation_id} of " \
                          f"type {sliver.get_type()} {sliver.get_reservation_info().reservation_state} " \
                          f"by prj:{slice_object.get_project_id()} usr:{slice_object.get_owner().get_oidc_sub_claim()}" \
                          f":{slice_object.get_owner().get_email()} with"

            if isinstance(sliver, NodeSliver):
                if sliver.get_type() == NodeType.Facility:
                    log_message += f" facilities {sliver.get_name()},"
                else:
                    log_message += f" sites {sliver.get_site()},"
                if sliver.attached_components_info is not None:
                    log_message += " components "
                    for c in sliver.attached_components_info.devices.values():
                        log_message += f"{c.get_type()},"
            elif isinstance(sliver, NetworkServiceSliver):
                if sliver.get_site() is not None:
                    log_message += f" on site {sliver.get_site()}"

            logger.info(log_message)
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            logger.error(traceback.format_exc())