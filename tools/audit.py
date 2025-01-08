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
import os
import re
import smtplib
import traceback
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler

import yaml
from fabric_mb.message_bus.messages.slice_avro import SliceAvro

from fabric_cf.actor.core.kernel.slice import SliceTypes
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
from fabric_cf.actor.core.util.smtp import send_email, load_and_update_template
from fabric_cf.actor.fim.fim_helper import FimHelper


class MainClass:
    """
    Audit CLI provides following capabilities:
    - Remove/Delete slices older than specified number of days
    - Remove/Delete dangling network services which connect the ports to deleted/closed VMs
    """
    def __init__(self, config_file: str, am_config_file: str):
        self.am_config_dict = None
        with open(config_file) as f:
            config_dict = yaml.safe_load(f)

        if am_config_file is not None and os.path.exists(am_config_file):
            with open(am_config_file) as f:
                self.am_config_dict = yaml.safe_load(f)

        # Load the config file
        self.log_config = config_dict[Constants.CONFIG_LOGGING_SECTION]

        # create the logger
        self.logger = logging.getLogger(self.log_config[Constants.PROPERTY_CONF_LOGGER])
        file_handler = RotatingFileHandler(f"{self.log_config[Constants.PROPERTY_CONF_LOG_DIRECTORY]}/audit.log",
                                           backupCount=self.log_config[Constants.PROPERTY_CONF_LOG_RETAIN],
                                           maxBytes=self.log_config[Constants.PROPERTY_CONF_LOG_SIZE])
        logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
                            handlers=[logging.StreamHandler(), file_handler])

        # Neo4j Config
        self.neo4j_config = config_dict[Constants.CONFIG_SECTION_NEO4J]

        # Database Config
        self.database_config = config_dict[Constants.CONFIG_SECTION_DATABASE]

        # Actor Config
        self.actor_config = config_dict[Constants.CONFIG_SECTION_ACTOR]

        self.smtp_config = config_dict.get(Constants.CONFIG_SECTION_SMTP)

        # Load config in the GlobalsSingleton
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        GlobalsSingleton.get().config_file = config_file
        GlobalsSingleton.get().load_config()
        GlobalsSingleton.get().initialized = True
        GlobalsSingleton.get().log = self.logger

        self.mgmt_actor = None

        # For Orchestrator; initialize a management actor
        actor_type = self.actor_config[Constants.TYPE]
        if actor_type.lower() == ActorType.Orchestrator.name.lower():
            actor_topic = self.actor_config[Constants.KAFKA_TOPIC]
            actor_topic = actor_topic.replace("^", "")
            actor_topic = actor_topic.replace("*", "1")
            actor_guid = GlobalsSingleton.get().get_config().get_actor_config().get_guid()

            self.audit_config = config_dict['audit']
            audit_topic = self.audit_config[Constants.KAFKA_TOPIC]

            # Setup Kafka producer config
            producer_config = GlobalsSingleton.get().get_kafka_config_producer()
            if Constants.SASL_USERNAME in producer_config:
                producer_config[Constants.SASL_USERNAME] = self.audit_config[Constants.PROPERTY_CONF_KAFKA_SASL_PRODUCER_USERNAME]
                producer_config[Constants.SASL_PASSWORD] = self.audit_config[Constants.PROPERTY_CONF_KAFKA_SASL_PRODUCER_PASSWORD]
                producer_config[Constants.SASL_MECHANISM] = self.audit_config[Constants.PROPERTY_CONF_KAFKA_SASL_MECHANISM]

            key_schema_loc = GlobalsSingleton.get().get_config().get_kafka_key_schema_location()
            val_schema_loc = GlobalsSingleton.get().get_config().get_kafka_value_schema_location()

            # Create a producer
            from fabric_mb.message_bus.producer import AvroProducerApi
            producer = AvroProducerApi(producer_conf=producer_config, key_schema_location=key_schema_loc,
                                       value_schema_location=val_schema_loc, logger=self.logger)

            # Setup Kafka consumer config
            consumer_config = GlobalsSingleton.get().get_kafka_config_consumer()
            if Constants.SASL_USERNAME in consumer_config:
                consumer_config[Constants.SASL_USERNAME] = self.audit_config[Constants.PROPERTY_CONF_KAFKA_SASL_CONSUMER_USERNAME]
                consumer_config[Constants.SASL_PASSWORD] = self.audit_config[Constants.PROPERTY_CONF_KAFKA_SASL_CONSUMER_PASSWORD]
                consumer_config[Constants.SASL_MECHANISM] = self.audit_config[Constants.PROPERTY_CONF_KAFKA_SASL_MECHANISM]

            # Setup Kafka Message processor to handle responses back from the actor
            auth = AuthAvro()
            auth.name = self.audit_config[Constants.NAME]
            auth.guid = self.audit_config[Constants.GUID]

            message_processor = KafkaMgmtMessageProcessor(consumer_conf=consumer_config,
                                                          key_schema_location=key_schema_loc,
                                                          value_schema_location=val_schema_loc, logger=self.logger,
                                                          topics=[audit_topic])

            self.mgmt_actor = KafkaActor(guid=ID(uid=actor_guid), kafka_topic=actor_topic,
                                         auth=auth, logger=self.logger, producer=producer,
                                         message_processor=message_processor)
            self.mgmt_actor.callback_topic = audit_topic

    def delete_dangling_network_slivers(self):
        """
        Delete dangling network slivers connected to VMs in Closed state
        It has been observed that some times failure to renew VMs but successful renew of Network Services leaves
        dangling NS attached to ports on VMs in closed state. This results in future NS
        provisioning when the same port is now assigned to a new VM. This command identifies such NS and cleans them.
        """
        actor_type = self.actor_config[Constants.TYPE]
        if actor_type.lower() != ActorType.Orchestrator.name.lower():
            return

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

        # Get the Active NS Slivers
        slivers = actor_db.get_reservations(states=states, rsv_type=resource_type)
        for s in slivers:
            # Check dependencies
            closed_preds = 0
            for pred_state in s.get_redeem_predecessors():
                if pred_state.get_reservation().is_closed() or pred_state.get_reservation().is_closing():
                    closed_preds += 1

            # Close the sliver if at least one of the dependencies are in closed state
            if closed_preds:
                self.logger.info(f"Closing Sliver: {s.get_reservation_id()};"
                                 f" Found dependencies# {closed_preds} in closed/closing state")
                # Trigger close
                self.mgmt_actor.close_reservation(rid=s.get_reservation_id())

    def delete_dead_closing_slice(self, *, days: int):
        """
        Delete Dead/Closing slice older than days* from postgres and neo4j database
        @param days: Number of days
        """

        # Create Db object
        actor_db = ActorDatabase(user=self.database_config[Constants.PROPERTY_CONF_DB_USER],
                                 password=self.database_config[Constants.PROPERTY_CONF_DB_PASSWORD],
                                 database=self.database_config[Constants.PROPERTY_CONF_DB_NAME],
                                 db_host=self.database_config[Constants.PROPERTY_CONF_DB_HOST],
                                 logger=self.logger)

        # Get Closing/Dead slices older than days
        states = [SliceState.Dead.value, SliceState.Closing.value]
        lease_end = datetime.now(timezone.utc) - timedelta(days=days)
        slices = actor_db.get_slices(states=states, lease_end=lease_end)
        actor_type = self.actor_config[Constants.TYPE]
        for s in slices:
            try:
                # Delete the slice from Neo4J - only done on orchestrator
                if actor_type.lower() == ActorType.Orchestrator.name.lower():
                    try:
                        FimHelper.delete_graph(graph_id=s.get_graph_id(), neo4j_config=self.neo4j_config)
                    except Exception as e:
                        self.logger.error(f"Failed to delete graph {s.get_graph_id()} for Slice# {s.get_slice_id()}: e: {e}")
                        self.logger.error(traceback.format_exc())

                # Fetch the slivers and delete them from Postgres
                reservations = actor_db.get_reservations(slice_id=s.get_slice_id())
                for r in reservations:
                    try:
                        actor_db.remove_reservation(rid=r.get_reservation_id())
                    except Exception as e:
                        self.logger.error(
                            f"Failed to delete reservation {r.get_reservation_id()} for Slice# {s.get_slice_id()}: e: {e}")
                        self.logger.error(traceback.format_exc())

                # Remove the slice from Postgres
                actor_db.remove_slice(slice_id=s.get_slice_id())
            except Exception as e:
                self.logger.error(f"Failed to delete slice: {s.get_slice_id()}: e: {e}")
                self.logger.error(traceback.format_exc())

    def execute_ansible(self, *, inventory_path: str, playbook_path: str, extra_vars: dict,
                        ansible_python_interpreter: str, sources: str = None, private_key_file: str = None,
                        host_vars: dict = None, host: str = None, user: str = None):
        from fabric_am.util.ansible_helper import AnsibleHelper
        ansible_helper = AnsibleHelper(inventory_path=inventory_path, logger=self.logger,
                                       ansible_python_interpreter=ansible_python_interpreter,
                                       sources=sources)

        ansible_helper.set_extra_vars(extra_vars=extra_vars)

        if host is not None and host_vars is not None and len(host_vars) > 0:
            for key, value in host_vars.items():
                ansible_helper.add_vars(host=host, var_name=key, value=value)

        self.logger.info(f"Executing playbook {playbook_path} extra_vars: {extra_vars} host_vars: {host_vars}")
        ansible_helper.run_playbook(playbook_path=playbook_path, private_key_file=private_key_file, user=user)
        return ansible_helper.get_result_callback()

    def clean_sliver_close_fail(self):
        """
        Clean the slivers in Close Fail state
        @return:
        """
        try:
            actor_type = self.actor_config[Constants.TYPE]
            if actor_type.lower() != ActorType.Broker.name.lower():
                return
            actor_db = ActorDatabase(user=self.database_config[Constants.PROPERTY_CONF_DB_USER],
                                     password=self.database_config[Constants.PROPERTY_CONF_DB_PASSWORD],
                                     database=self.database_config[Constants.PROPERTY_CONF_DB_NAME],
                                     db_host=self.database_config[Constants.PROPERTY_CONF_DB_HOST],
                                     logger=self.logger)

            states = [ReservationStates.CloseFail.value]
            slivers = actor_db.get_reservations(states=states)
            for s in slivers:
                term = s.get_term()
                end = term.get_end_time() if term else None
                now = datetime.now(timezone.utc)
                if end and end < now:
                    actor_db.remove_reservation(rid=s.get_reservation_id())

        except Exception as e:
            self.logger.error(f"Failed to cleanup inconsistencies: {e}")
            self.logger.error(traceback.format_exc())

    def send_slice_expiry_email_warnings(self):
        """
        Sends warning emails to users whose slices are about to expire within 12 hours or 6 hours.

        This function checks the expiration times of active slices and sends warning emails to the
        slice owners if the slice is set to expire in less than 12 hours or 6 hours. The function is
        intended to run periodically (e.g., once an hour) and uses a template for email content.

        @return: None: This function does not return any value but sends out emails and logs the process.
        """
        actor_type = self.actor_config[Constants.TYPE]
        if actor_type.lower() != ActorType.Orchestrator.name.lower():
            return

        actor_db = ActorDatabase(user=self.database_config[Constants.PROPERTY_CONF_DB_USER],
                                 password=self.database_config[Constants.PROPERTY_CONF_DB_PASSWORD],
                                 database=self.database_config[Constants.PROPERTY_CONF_DB_NAME],
                                 db_host=self.database_config[Constants.PROPERTY_CONF_DB_HOST],
                                 logger=self.logger)

        # Get the currently active slices
        slices = actor_db.get_slices(states=SliceState.list_values_ex_closing_dead(),
                                     slc_type=[SliceTypes.ClientSlice])

        now = datetime.now(timezone.utc)

        for s in slices:
            email = s.get_owner().get_email()
            if s.get_lease_end():
                diff = s.get_lease_end() - now
                hours_left = diff.total_seconds() // 3600

                # Check if it's 12 hours or 6 hours before expiration
                if 11 < hours_left <= 12:
                    # Send a 12-hour prior warning email
                    try:
                        subject, body = load_and_update_template(
                            template_path=self.smtp_config.get("template_path"),
                            user=email,
                            slice_name=f"{s.get_name()}/{s.get_slice_id()}",
                            hours_left=12
                        )
                        send_email(smtp_config=self.smtp_config, to_email=email, subject=subject, body=body)
                        self.logger.info(f"Sent 12-hour prior warning to {email} for slice {s.get_name()}")
                    except smtplib.SMTPAuthenticationError as e:
                        self.logger.error(f"Failed to send email: Error: {e.smtp_code} Message: {e.smtp_error}")
                    except Exception as e:
                        self.logger.error(f"Failed to send email: Error: {e}")

                elif 5 < hours_left <= 6:
                    # Send a 6-hour prior warning email
                    try:
                        subject, body = load_and_update_template(
                            template_path=self.smtp_config.get("template_path"),
                            user=email,
                            slice_name=f"{s.get_name()}/{s.get_slice_id()}",
                            hours_left=6
                        )
                        send_email(smtp_config=self.smtp_config, to_email=email, subject=subject, body=body)
                        self.logger.info(f"Sent 6-hour prior warning to {email} for slice {s.get_name()}")
                    except smtplib.SMTPAuthenticationError as e:
                        self.logger.error(f"Failed to send email: Error: {e.smtp_code} Message: {e.smtp_error}")
                    except Exception as e:
                        self.logger.error(f"Failed to send email: Error: {e}")

    def clean_sliver_inconsistencies(self):
        """
        Clean up any sliver inconsistencies between CF, Libvirt and Openstack
        @return:
        """
        try:
            actor_type = self.actor_config[Constants.TYPE]
            if actor_type.lower() != ActorType.Authority.name.lower() or self.am_config_dict is None:
                return

            from fabric_am.util.am_constants import AmConstants
            pb_section = self.am_config_dict.get(AmConstants.PLAYBOOK_SECTION)
            if pb_section is None:
                return
            inventory_location = pb_section.get(AmConstants.PB_INVENTORY)
            pb_dir = pb_section.get(AmConstants.PB_LOCATION)
            vm_playbook_name = pb_section.get("VM")
            if inventory_location is None or pb_dir is None or vm_playbook_name is None:
                return

            vm_playbook_path = f"{pb_dir}/{vm_playbook_name}"

            ansible_python_interpreter = None
            ansible_section = self.am_config_dict.get(AmConstants.ANSIBLE_SECTION)
            if ansible_section:
                ansible_python_interpreter = ansible_section.get(AmConstants.ANSIBLE_PYTHON_INTERPRETER)

            actor_db = ActorDatabase(user=self.database_config[Constants.PROPERTY_CONF_DB_USER],
                                     password=self.database_config[Constants.PROPERTY_CONF_DB_PASSWORD],
                                     database=self.database_config[Constants.PROPERTY_CONF_DB_NAME],
                                     db_host=self.database_config[Constants.PROPERTY_CONF_DB_HOST],
                                     logger=self.logger)

            states = [ReservationStates.Active.value,
                      ReservationStates.ActiveTicketed.value,
                      ReservationStates.Ticketed.value,
                      ReservationStates.Nascent.value]
            
            resource_type = ["VM"]

            # Get the Active Slivers from CF
            slivers = actor_db.get_reservations(states=states, rsv_type=resource_type)
            cf_active_sliver_ids = []
            if slivers:
                for s in slivers:
                    cf_active_sliver_ids.append(str(s.get_reservation_id()))

            self.logger.info(f"Active Slivers: {cf_active_sliver_ids}")

            # Get the VMs from Openstack
            result_callback_1 = self.execute_ansible(inventory_path=inventory_location,
                                                     playbook_path=vm_playbook_path,
                                                     extra_vars={"operation": "list"},
                                                     ansible_python_interpreter=ansible_python_interpreter)
            result_1 = result_callback_1.get_json_result_ok()

            os_vms = {}
            if result_1 and result_1.get('openstack_servers'):
                servers = result_1.get('openstack_servers')
                for s in servers:
                    if s.get('OS-EXT-SRV-ATTR:instance_name') and s.get('name'):
                        os_vms[s.get('OS-EXT-SRV-ATTR:instance_name')] = s.get('name')

            uuid_pattern = r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})'

            # Cleanup inconsistencies between CF and Open Stack
            if len(cf_active_sliver_ids):
                for instance, vm_name in os_vms.items():
                    try:
                        # Search for UUID in the input string
                        match = re.search(uuid_pattern, vm_name)

                        # Extract UUID if found
                        if match:
                            sliver_id = match.group(1)
                            if sliver_id not in cf_active_sliver_ids:
                                result_2 = self.execute_ansible(inventory_path=inventory_location,
                                                                playbook_path=vm_playbook_path,
                                                                extra_vars={"operation": "delete", "vmname": vm_name},
                                                                ansible_python_interpreter=ansible_python_interpreter)
                                self.logger.info(f"Deleted instance: {vm_name}; result: {result_2.get_json_result_ok()}")
                        else:
                            self.logger.error(f"Sliver Id not found in the input string: {vm_name}")
                    except Exception as e:
                        self.logger.error(f"Failed to cleanup CF and openstack inconsistencies instance: {instance} vm: {vm_name}: {e}")
                        self.logger.error(traceback.format_exc())

            # Cleanup inconsistencies between Open Stack and Virsh
            result_3 = self.execute_ansible(inventory_path=inventory_location,
                                            playbook_path=f"{pb_dir}/worker_libvirt_operations.yml",
                                            extra_vars={"operation": "listall"},
                                            ansible_python_interpreter=ansible_python_interpreter)

            for host, ok_result in result_3.host_ok.items():
                try:
                    if ok_result and ok_result._result:
                        virsh_vms = ok_result._result.get('stdout_lines', [])
                        self.logger.info(f"Host: {host} has VMs: {virsh_vms}")
                        for instance in virsh_vms:
                            if instance not in os_vms:
                                results_4 = self.execute_ansible(inventory_path=inventory_location,
                                                                 playbook_path=vm_playbook_path,
                                                                 extra_vars={"operation": "delete", "host": str(host)},
                                                                 ansible_python_interpreter=ansible_python_interpreter)
                                self.logger.info(f"Deleted instance: {instance}; result: {results_4.get_json_result_ok()}")
                except Exception as e:
                    self.logger.error(f"Failed to cleanup openstack and virsh inconsistencies on {host}: {e}")
                    self.logger.error(traceback.format_exc())
        except Exception as e:
            self.logger.error(f"Failed to cleanup inconsistencies: {e}")
            self.logger.error(traceback.format_exc())

    def handle_command(self, args):
        """
        Command Handler
        @param args Command arguments
        """
        # Slice commands
        if args.command == "slices":
            # Remove operation
            if args.operation is not None and args.operation == "remove":
                self.delete_dead_closing_slice(days=args.days)
            else:
                print(f"Unsupported operation: {args.operation}")
        # Slivers
        elif args.command == "slivers":
            # Close operation
            if args.operation is not None and args.operation == "cleanup":
                self.clean_sliver_inconsistencies()
            else:
                print(f"Unsupported operation: {args.operation}")
        elif args.command == "audit":
            # Close operation
            if args.operation is not None and args.operation == "audit":
                self.delete_dead_closing_slice(days=args.days)
                self.clean_sliver_close_fail()
                self.clean_sliver_inconsistencies()
                self.send_slice_expiry_email_warnings()
            else:
                print(f"Unsupported operation: {args.operation}")
        else:
            print(f"Unsupported command: {args.command}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", dest='amconfig', required=True, type=str)
    parser.add_argument("-f", dest='config', required=True, type=str)
    parser.add_argument("-d", dest='days', required=False, type=int, default=30)
    parser.add_argument("-c", dest='command', required=True, type=str)
    parser.add_argument("-o", dest='operation', required=True, type=str)
    args = parser.parse_args()

    mc = MainClass(config_file=args.config, am_config_file=args.amconfig)
    mc.handle_command(args)

