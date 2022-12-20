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
from fim.logging.log_collector import LogCollector
from fim.slivers.base_sliver import BaseSliver
from fim.user.topology import ExperimentTopology

from fabric_cf.actor.security.pdp_auth import ActionId


class EventLogger:
    @staticmethod
    def log_slice_event(*, logger: logging.Logger, slice_object: SliceAvro, action: ActionId,
                        topology: ExperimentTopology = None):
        """
        Log Slice Event for metrics
        """
        try:

            log_message = f"Slice event slc:{slice_object.get_slice_name()}:{slice_object.get_slice_id()} {action} by " \
                          f"prj:{slice_object.get_project_id()} " \
                          f"usr:{slice_object.get_owner().get_oidc_sub_claim()}:{slice_object.get_owner().get_email()}"

            if topology is not None:
                lc = LogCollector()
                lc.collect_resource_attributes(source=topology)
                log_message += f" {str(lc)}"

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
            lc = LogCollector()
            lc.collect_resource_attributes(source=sliver)

            log_message = f"Sliver event slvr:{sliver.get_reservation_info().reservation_id} of " \
                          f"type {sliver.get_type()} {sliver.get_reservation_info().reservation_state} " \
                          f"by prj:{slice_object.get_project_id()} usr:{slice_object.get_owner().get_oidc_sub_claim()}" \
                          f":{slice_object.get_owner().get_email()} {str(lc)}"

            logger.info(log_message)
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            logger.error(traceback.format_exc())