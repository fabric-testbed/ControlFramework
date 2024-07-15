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
from fim.slivers.network_node import NodeSliver
from fim.user.topology import ExperimentTopology

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import InitializationException
from fabric_cf.actor.core.util.log_helper import LogHelper
from fabric_cf.actor.core.util.utils import generate_sha256
from fabric_cf.actor.security.pdp_auth import ActionId


class EventLogger:
    def __init__(self):
        self.logger = None

    def make_logger(self, log_config: dict):
        log_dir = log_config.get(Constants.PROPERTY_CONF_LOG_DIRECTORY, ".")
        log_file = log_config.get(Constants.PROPERTY_CONF_METRICS_LOG_FILE, "metrics.log")
        log_level = log_config.get(Constants.PROPERTY_CONF_LOG_LEVEL, logging.INFO)
        log_retain = int(log_config.get(Constants.PROPERTY_CONF_LOG_RETAIN, 50))
        log_size = int(log_config.get(Constants.PROPERTY_CONF_LOG_SIZE, 5000000))
        logger = f'{log_config.get(Constants.PROPERTY_CONF_LOGGER, "actor")}-metrics'
        log_format = '%(asctime)s - %(message)s'

        self.logger = LogHelper.make_logger(log_dir=log_dir, log_file=log_file, log_level=log_level,
                                            log_retain=log_retain, log_size=log_size, logger=logger,
                                            log_format=log_format)

    def log_slice_event(self, *, slice_object: SliceAvro, action: ActionId, topology: ExperimentTopology = None):
        """
        Log Slice Event for metrics
        """
        try:
            owner = slice_object.get_owner()
            log_message = f"CFEL Slice event slc:{slice_object.get_slice_id()} " \
                          f"{action} by prj:{slice_object.get_project_id()} " \
                          f"usr:{owner.get_oidc_sub_claim()}:{owner.get_email()}"
            if slice_object.get_config_properties() is not None:
                token_hash = slice_object.get_config_properties().get(Constants.TOKEN_HASH, "token_hash_not_available")
                log_message += f":{token_hash}"

            if topology is not None:
                lc = LogCollector()
                lc.collect_resource_attributes(source=topology)
                log_message += f" {str(lc)}"

            self.logger.info(log_message)
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error occurred: {e}")
            self.logger.error(traceback.format_exc())

    def log_sliver_event(self, *, slice_object: SliceAvro, sliver: BaseSliver, verb: str = None, keys: list = None):
        """
        Log Sliver events
        """
        try:
            lc = LogCollector()
            lc.collect_resource_attributes(source=sliver)
            if verb is None:
                verb = sliver.get_reservation_info().reservation_state

            ssh_foot_print = None
            if keys is not None:
                ssh_foot_print = ""
                for key_pair in keys:
                    fp = generate_sha256(token=key_pair.key)
                    ssh_foot_print += f":{fp}"

            owner = slice_object.get_owner()
            log_message = f"CFEL Sliver event slc:{slice_object.get_slice_id()} " \
                          f"slvr:{sliver.get_reservation_info().reservation_id}/{sliver.get_name()} of " \
                          f"type {sliver.get_type()} {verb} " \
                          f"by prj:{slice_object.get_project_id()} usr:{owner.get_oidc_sub_claim()}" \
                          f":{owner.get_email()}"

            if slice_object.get_config_properties() is not None:
                token_hash = slice_object.get_config_properties().get(Constants.TOKEN_HASH, "token_hash_not_available")
                log_message += f":{token_hash}"

            if ssh_foot_print is not None:
                log_message += f" keys{ssh_foot_print}"

            log_message += f" {str(lc)}"

            if isinstance(sliver, NodeSliver) and sliver.get_image_ref():
                log_message += f" image:{sliver.get_image_ref()}"

            self.logger.info(log_message)
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error occurred: {e}")
            self.logger.error(traceback.format_exc())


class EventLoggerSingleton:
    """
    EventLogger Singleton class
    """
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise InitializationException("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = EventLogger()
        return self.__instance

    get = classmethod(get)
