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
import argparse
import logging
import traceback
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler

import yaml
from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fim.slivers.network_service import ServiceType

from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.kernel.slice_state_machine import SliceState
from fabric_cf.actor.core.manage.kafka.kafka_actor import KafkaActor
from fabric_cf.actor.core.manage.kafka.kafka_mgmt_message_processor import KafkaMgmtMessageProcessor
from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.fim.fim_helper import FimHelper


class MainClass:
    """
    CLI interface to cleanup Dead/Closing slices
    """
    def __init__(self, config_file: str):
        with open(config_file) as f:
            config_dict = yaml.safe_load(f)
        self.log_config = config_dict[Constants.CONFIG_LOGGING_SECTION]

        self.logger = logging.getLogger(self.log_config[Constants.PROPERTY_CONF_LOGGER])
        file_handler = RotatingFileHandler(f"{self.log_config[Constants.PROPERTY_CONF_LOG_DIRECTORY]}/cleanup.log",
                                           backupCount=self.log_config[Constants.PROPERTY_CONF_LOG_RETAIN],
                                           maxBytes=self.log_config[Constants.PROPERTY_CONF_LOG_SIZE])
        logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
                            handlers=[logging.StreamHandler(), file_handler])

        self.neo4j_config = config_dict[Constants.CONFIG_SECTION_NEO4J]
        self.database_config = config_dict[Constants.CONFIG_SECTION_DATABASE]
        self.actor_config = config_dict[Constants.CONFIG_SECTION_ACTOR]
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        GlobalsSingleton.get().config_file = config_file
        GlobalsSingleton.get().load_config()
        GlobalsSingleton.get().initialized = True

        self.mgmt_actor = None

        actor_type = GlobalsSingleton.get().get_config().get_actor_config().get_type()
        actor_topic = GlobalsSingleton.get().get_config().get_actor_config().get_kafka_topic()
        actor_guid = GlobalsSingleton.get().get_config().get_actor_config().get_guid()

        if actor_type.lower() == ActorType.Orchestrator.name.lower():
            # Setup Kafka
            producer_config = GlobalsSingleton.get().get_kafka_config_producer()
            key_schema_loc = GlobalsSingleton.get().get_config().get_kafka_key_schema_location()
            val_schema_loc = GlobalsSingleton.get().get_config().get_kafka_value_schema_location()

            from fabric_mb.message_bus.producer import AvroProducerApi
            producer = AvroProducerApi(producer_conf=producer_config, key_schema_location=key_schema_loc,
                                       value_schema_location=val_schema_loc, logger=self.logger)

            auth = AuthAvro()
            auth.name = "db-cleanup"
            auth.guid = "db-cleanup-guid"

            self.mgmt_actor = KafkaActor(guid=ID(uid=actor_guid), kafka_topic=actor_topic,
                                         auth=auth,
                                         logger=self.logger, message_processor=None,
                                         producer=producer)

    def delete_dangling_network_slivers(self):
        actor_db = ActorDatabase(user=self.database_config[Constants.PROPERTY_CONF_DB_USER],
                                 password=self.database_config[Constants.PROPERTY_CONF_DB_PASSWORD],
                                 database=self.database_config[Constants.PROPERTY_CONF_DB_NAME],
                                 db_host=self.database_config[Constants.PROPERTY_CONF_DB_HOST],
                                 logger=self.logger)
        states = [ReservationStates.Active.value, ReservationStates.ActiveTicketed.value,
                  ReservationStates.Failed.value]
        resource_type = []
        for s in ServiceType:
            resource_type.append(str(s))
        actor_type = self.actor_config[Constants.TYPE]
        if actor_type.lower() != ActorType.Orchestrator.name.lower():
            return

        slivers = actor_db.get_reservations(states=states, rsv_type=resource_type)
        for s in slivers:
            # Check dependencies
            closed_preds = 0
            for pred_state in s.get_redeem_predecessors():
                if pred_state.get_reservation().is_closed() or pred_state.get_reservation().is_closing():
                    closed_preds += 1

            # if closed_preds > len(self.get_redeem_predecessors()):
            if closed_preds:
                self.logger.debug(f"Found dependencies# {closed_preds} in closed/closing state")
                # Trigger close
                self.mgmt_actor.close_reservation(rid=s.get_reservation_id())

    def delete_dead_closing_slice(self, *, days: int):
        actor_db = ActorDatabase(user=self.database_config[Constants.PROPERTY_CONF_DB_USER],
                                 password=self.database_config[Constants.PROPERTY_CONF_DB_PASSWORD],
                                 database=self.database_config[Constants.PROPERTY_CONF_DB_NAME],
                                 db_host=self.database_config[Constants.PROPERTY_CONF_DB_HOST],
                                 logger=self.logger)
        states = [SliceState.Dead.value, SliceState.Closing.value]
        lease_end = datetime.now(timezone.utc) - timedelta(days=days)
        slices = actor_db.get_slices(states=states, lease_end=lease_end)
        actor_type = self.actor_config[Constants.TYPE]
        for s in slices:
            try:
                if actor_type.lower() == ActorType.Orchestrator.name.lower():
                    try:
                        FimHelper.delete_graph(graph_id=s.get_graph_id(), neo4j_config=self.neo4j_config)
                    except Exception as e:
                        self.logger.error(f"Failed to delete graph {s.get_graph_id()} for Slice# {s.get_slice_id()}: e: {e}")
                        self.logger.error(traceback.format_exc())
                reservations = actor_db.get_reservations(slice_id=s.get_slice_id())
                for r in reservations:
                    try:
                        actor_db.remove_reservation(rid=r.get_reservation_id())
                    except Exception as e:
                        self.logger.error(
                            f"Failed to delete reservation {r.get_reservation_id()} for Slice# {s.get_slice_id()}: e: {e}")
                        self.logger.error(traceback.format_exc())
                actor_db.remove_slice(slice_id=s.get_slice_id())
            except Exception as e:
                self.logger.error(f"Failed to delete slice: {s.get_slice_id()}: e: {e}")
                self.logger.error(traceback.format_exc())

    def handle_command(self, args):
        if args.command == "slices":
            if args.operation is not None and args.operation == "remove":
                self.delete_dead_closing_slice(days=args.days)
            else:
                print(f"Unsupported operation: {args.operation}")
        else:
            print(f"Unsupported command: {args.command}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", dest='config', required=True, type=str)
    parser.add_argument("-d", dest='days', required=True, type=int, default=30)
    parser.add_argument("-c", dest='command', required=True, type=str)
    parser.add_argument("-o", dest='operation', required=True, type=str)
    args = parser.parse_args()

    mc = MainClass(config_file=args.config)
    mc.handle_command(args)

