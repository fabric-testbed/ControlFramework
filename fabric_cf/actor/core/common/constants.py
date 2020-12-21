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
from enum import Enum


class ErrorCodes(Enum):
    """
    Error code enumeration
    """
    ErrorInvalidArguments = 1
    ErrorInvalidActor = 2
    ErrorInvalidReservation = 3
    ErrorDatabaseError = 4
    ErrorInternalError = 5
    ErrorNoSuchSlice = 6
    ErrorNoSuchResourcePool = 7
    ErrorNoSuchReservation = 8
    ErrorNoSuchBroker = 9
    ErrorInvalidSlice = 10
    ErrorNoSuchActor = 11
    ErrorTransportFailure = 12
    ErrorTransportTimeout = 13
    ErrorNoSuchDelegation = 14


class Constants:
    """
    Constants
    """
    reservation_has_pending_operation = -200001
    extend_same_units = -1
    all_reservation_states = -1
    management_api_timeout_in_seconds = 120.0

    container_managment_object_id = "manager"

    property_pickle_properties = "properties"

    config_section_runtime = "runtime"
    property_conf_kafka_server = "kafka-server"
    property_conf_kafka_schema_registry = "kafka-schema-registry-url"
    property_conf_kafka_key_schema = "kafka-key-schema"
    property_conf_kafka_value_schema = "kafka-value-schema"
    property_conf_kafka_s_sl_ca_location = "kafka-ssl-ca-location"
    property_conf_kafka_ssl_certificate_location = "kafka-ssl-certificate-location"
    property_conf_kafka_ssl_key_location = "kafka-ssl-key-location"
    property_conf_kafka_ssl_key_password = "kafka-ssl-key-password"
    property_conf_kafka_security_protocol = "kafka-security-protocol"
    property_conf_kafka_group_id = "kafka-group-id"
    property_conf_kafka_sasl_producer_username = "kafka-sasl-producer-username"
    property_conf_kafka_sasl_producer_password = "kafka-sasl-producer-password"
    property_conf_kafka_sasl_consumer_username = "kafka-sasl-consumer-username"
    property_conf_kafka_sasl_consumer_password = "kafka-sasl-consumer-password"
    property_conf_kafka_sasl_mechanism = "kafka-sasl-mechanism"

    kafka_topic = "kafka-topic"
    name = "name"
    type = "type"
    guid = "guid"
    credmgr_host = "credmgr-host"

    property_class_name = "ObjectClassName"
    property_module_name = "ModuleName"
    property_id = "MOID"
    property_type_id = "MOTYPEID"
    property_actor_name = "MOActorName"
    property_proxies_length = "MOProxiesLength"
    property_proxies_prefix = "MOProxiesPrefix."
    property_proxies_protocol = ".protocol"
    property_proxies_class = ".class"
    property_proxies_module = ".module"

    property_conf_prometheus_rest_port = "prometheus.port"
    property_conf_controller_rest_port = "orchestrator.rest.port"
    property_conf_controller_create_wait_time_ms = "orchestrator.create.wait.time.ms"
    property_conf_controller_delay_resource_types = "orchestrator.delay.resource.types"

    property_substrate_file = "substrate.file"
    property_aggregate_resource_model = "AggregateResourceModel"

    property_conf_plugin_dir = "plugin-dir"

    config_logging_section = 'logging'

    config_section_pdp = 'pdp'

    property_conf_log_file = 'log-file'
    property_conf_log_level = 'log-level'
    property_conf_log_retain = 'log-retain'
    property_conf_log_size = 'log-size'
    property_conf_log_directory = 'log-directory'
    property_conf_logger = "logger"

    config_section_container = "container"
    property_conf_container_guid = "container.guid"

    config_section_time = "time"
    property_conf_time_start_time = "time.startTime"
    property_conf_time_cycle_millis = "time.cycleMillis"
    property_conf_time_manual = "time.manual"

    config_section_o_auth = "oauth"
    property_conf_o_auth_jwks_url = "jwks-url"
    property_conf_o_auth_token_public_key = "token-public-key"

    config_section_database = "database"
    property_conf_db_user = "db-user"
    property_conf_db_password = "db-password"
    property_conf_db_name = "db-name"
    property_conf_db_host = "db-host"

    config_section_neo4j = "neo4j"

    protocol_local = "local"
    protocol_kafka = "kafka"

    home_directory = '/usr/src/app/'
    superblock_location = home_directory + "state_recovery.lock"
    controller_lock_location = home_directory + "controller_recovery.lock"
    configuration_file = "/etc/fabric/actor/config/config.yaml"
    state_file_location = '/tmp/fabric_actor.tmp'

    test_directory = "."
    test_broker_configuration_file = test_directory + "config/config.broker.yaml"
    test_net_am_configuration_file = test_directory + "config/config.net-am.yaml"
    test_vm_am_configuration_file = test_directory + "config/config.site.am.yaml"
    test_controller_configuration_file = test_directory + "config/config.orchestrator.yaml"

    elastic_time = "request.elasticTime"
    elastic_size = "request.elasticSize"

    pool_name = 'pool.name'
    pool_prefix = "pool."
    pools_count = "pools.count"
    query_action_discover_pools = "discover.pools"
    query_action = "query.action"
    query_response = "query.response"
    broker_query_model = "bqm"
    pool_type = "neo4j"

    config_handler = "config.handler"
    config_victims = "config.victims"
    config_image_guid = "config.image.guid"
    config_ssh_key_pattern = "config.ssh.user%d.keys"
    config_ssh_login_pattern = "config.ssh.user%d.login"
    config_ssh_sudo_pattern = "config.ssh.user%d.sudo"
    config_ssh_urn_pattern = "config.ssh.user%d.urn"
    config_ssh_num_logins = "config.ssh.numlogins"
    config_ssh_prefix = "config.ssh.user"
    config_ssh_key_suffix = ".keys"
    config_ssh_login_suffix = ".login"
    config_ssh_sudo_suffix = ".sudo"
    config_ssh_urn_suffix = ".urn"
    config_unit_tag = "config.unit.tag"

    resource_memory = "resource.memory"
    resource_cpu = "resource.cpu"
    resource_bandwidth = "resource.bandwidth"
    resource_num_cpu_cores = "resource.numCPUCores"
    resource_memory_capacity = "resource.memeoryCapacity"
    resource_storage_capacity = "resource.storageCapacity"
    resource_class_inventory_for_type = "resource.class.invfortype"
    resource_available_units = "resource.units.now"
    resource_start_iface = "resource.siface"
    resource_end_iface = "resource.eiface"
    resource_domain = "resource.domain"
    resource_neo4j_abstract_domain = "resource.neo4j.adomain"

    unit_management_ip = "unit.manage.ip"
    unit_management_port = "unit.manage.port"
    unit_manage_subnet = "unit.manage.subnet"
    unit_manage_gateway = "unit.manage.gateway"
    unit_data_subnet = "unit.data.subnet"
    unit_parent_host_name = "unit.parent.hostname"
    unit_host_name = "unit.hostname"
    unit_control = "unit.control"
    unit_memory = "unit.memory"
    unit_vlan_tag = "unit.vlan.tag"
    unit_vlan_qo_s_rate = "unit.vlan.qos.rate"
    unit_vlan_qo_s_burst_size = "unit.vlan.qos.burst.size"
    unit_eth_prefix = "unit.eth"
    unit_number_interface = "unit.number.interface"
    unit_modify_prop_message_suffix = ".message"
    unit_modify_prop_code_suffix = ".code"
    unit_modify_prop_prefix = "unit.modify."
    unit_lun_tag = "unit.target.lun"
    unit_storage_capacity = "unit.target.capacity"

    property_start_vlan = "vlan.tag.start"
    property_end_vlan = "vlan.tag.end"
    property_vlan_range_num = "vlan.range.num"

    property_start_lun = "lun.tag.start"
    property_end_lun = "lun.tag.end"
    property_lun_range_num = "lun.range.num"

    property_delegation_slice_id = 'dlg_slc_id'
    property_reservation_slice_id = 'rsv_slc_id'
    property_reservation_id = "rsv_resid"

    sasl_username = 'sasl.username'
    sasl_password = 'sasl.password'
    sasl_mechanism = 'sasl.mechanism'
    bootstrap_servers = 'bootstrap.servers'
    security_protocol = 'security.protocol'
    group_id = 'group.id'
    ssl_ca_location = 'ssl.ca.location'
    ssl_certificate_location = 'ssl.certificate.location'
    ssl_key_location = 'ssl.key.location'
    ssl_key_password = 'ssl.key.password'
    schema_registry_url = 'schema.registry.url'

    invalid_argument = "Invalid argument"
    uninitialized_state = "Uninitialized state"
    invalid_actor_state = "Invalid state, actor cannot receive calls"
    object_not_found = "{} not found in datatabase e: {}"
    failed_to_add = "Failed to add {} to database e: {}"
    failed_to_remove = "Failed to remove {} from database e: {}"
    not_specified_prefix = "Not {} specified"
    recovery = "[recovery]"
    issue_operation = "Issued {} request for reservation #{} State={}"
    invalid_pending_state = "Invalid pending state"
    restarting_actions = "Restarting configuration actions for reservation #{}"
    restarting_actions_complete = "Restarting configuration actions for reservation #{} complete"
    extend_ticket = "[extend ticket]"

    management_inter_actor_outbound_message = "Outbound [Kafka] [Management] Message {} written to {}"
    management_inter_actor_inbound_message = "Inbound [Kafka] [Management] Message received {}"
    management_api_timeout_occurred = "Management API Timeout Occurred"
    management_inter_actor_message_failed = "Outbound [Kafka] [Management] Message {} could not be written to {}"

    invalid_management_object_type = "Invalid Management Object type {}"
    no_pool = "there is no pool to satisfy this request"
    invalid_recovery_state = "This state should not be reached during recovery"

    invalid_state = "Invalid state"
    invalid_ip = "Invalid ip address: {}"
    exception_occurred = "Exception occurred {}"
    not_implemented = "Not Implemented"
