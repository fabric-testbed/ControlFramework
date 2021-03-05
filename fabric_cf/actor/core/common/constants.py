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
    ErrorInvalidToken = 15
    ErrorSliceExists = 16

    def interpret(self, exception=None):
        interpretations = {
            1: "Invalid Arguments",
            2: "Invalid Actor",
            3: "Invalid Reservation",
            4: "Database Error occurred",
            5: "Internal Error occurred",
            6: "No such slice found",
            7: "No such resource pool found",
            8: "No such reservation found",
            9: "No such broker",
            10: "Invalid Slice",
            11: "No such actor",
            12: "Transport failure",
            13: "Transport timeout",
            14: "No such delegation found",
            15: "Invalid Token",
            16: "Slice exists"
          }
        if exception is None:
            return interpretations[self.value]
        else:
            return str(exception) + ". " + interpretations[self.value]


class Constants:
    """
    Constants
    """
    RESERVATION_HAS_PENDING_OPERATION = -200001
    EXTEND_SAME_UNITS = -1
    ALL_RESERVATION_STATES = -1
    MANAGEMENT_API_TIMEOUT_IN_SECONDS = 120.0

    CONTAINER_MANAGMENT_OBJECT_ID = "manager"

    PROPERTY_PICKLE_PROPERTIES = "properties"

    CONFIG_SECTION_RUNTIME = "runtime"
    PROPERTY_CONF_KAFKA_SERVER = "kafka-server"
    PROPERTY_CONF_KAFKA_SCHEMA_REGISTRY = "kafka-schema-registry-url"
    PROPERTY_CONF_KAFKA_KEY_SCHEMA = "kafka-key-schema"
    PROPERTY_CONF_KAFKA_VALUE_SCHEMA = "kafka-value-schema"
    PROPERTY_CONF_KAFKA_S_SL_CA_LOCATION = "kafka-ssl-ca-location"
    PROPERTY_CONF_KAFKA_SSL_CERTIFICATE_LOCATION = "kafka-ssl-certificate-location"
    PROPERTY_CONF_KAFKA_SSL_KEY_LOCATION = "kafka-ssl-key-location"
    PROPERTY_CONF_KAFKA_SSL_KEY_PASSWORD = "kafka-ssl-key-password"
    PROPERTY_CONF_KAFKA_SECURITY_PROTOCOL = "kafka-security-protocol"
    PROPERTY_CONF_KAFKA_GROUP_ID = "kafka-group-id"
    PROPERTY_CONF_KAFKA_SASL_PRODUCER_USERNAME = "kafka-sasl-producer-username"
    PROPERTY_CONF_KAFKA_SASL_PRODUCER_PASSWORD = "kafka-sasl-producer-password"
    PROPERTY_CONF_KAFKA_SASL_CONSUMER_USERNAME = "kafka-sasl-consumer-username"
    PROPERTY_CONF_KAFKA_SASL_CONSUMER_PASSWORD = "kafka-sasl-consumer-password"
    PROPERTY_CONF_KAFKA_SASL_MECHANISM = "kafka-sasl-mechanism"

    KAFKA_TOPIC = "kafka-topic"
    NAME = "name"
    TYPE = "type"
    GUID = "guid"
    CREDMGR_HOST = "credmgr-host"

    PROPERTY_CLASS_NAME = "ObjectClassName"
    PROPERTY_MODULE_NAME = "ModuleName"
    PROPERTY_ID = "MOID"
    PROPERTY_TYPE_ID = "MOTYPEID"
    PROPERTY_ACTOR_NAME = "MOActorName"
    PROPERTY_PROXIES_LENGTH = "MOProxiesLength"
    PROPERTY_PROXIES_PREFIX = "MOProxiesPrefix."
    PROPERTY_PROXIES_PROTOCOL = ".protocol"
    PROPERTY_PROXIES_CLASS = ".class"
    PROPERTY_PROXIES_MODULE = ".module"

    PROPERTY_CONF_PROMETHEUS_REST_PORT = "prometheus.port"
    PROPERTY_CONF_CONTROLLER_REST_PORT = "orchestrator.rest.port"
    PROPERTY_CONF_CONTROLLER_CREATE_WAIT_TIME_MS = "orchestrator.create.wait.time.ms"

    PROPERTY_SUBSTRATE_FILE = "substrate.file"
    PROPERTY_AGGREGATE_RESOURCE_MODEL = "AggregateResourceModel"

    PROPERTY_CONF_PLUGIN_DIR = "plugin-dir"

    CONFIG_SECTION_PDP = 'pdp'

    CONFIG_LOGGING_SECTION = 'logging'
    PROPERTY_CONF_LOG_FILE = 'log-file'
    PROPERTY_CONF_LOG_LEVEL = 'log-level'
    PROPERTY_CONF_LOG_RETAIN = 'log-retain'
    PROPERTY_CONF_LOG_SIZE = 'log-size'
    PROPERTY_CONF_LOG_DIRECTORY = 'log-directory'
    PROPERTY_CONF_LOGGER = "logger"

    CONFIG_SECTION_CONTAINER = "container"
    PROPERTY_CONF_CONTAINER_GUID = "container.guid"

    CONFIG_SECTION_TIME = "time"
    PROPERTY_CONF_TIME_START_TIME = "time.startTime"
    PROPERTY_CONF_TIME_CYCLE_MILLIS = "time.cycleMillis"
    PROPERTY_CONF_TIME_MANUAL = "time.manual"

    CONFIG_SECTION_O_AUTH = "oauth"
    PROPERTY_CONF_O_AUTH_JWKS_URL = "jwks-url"
    PROPERTY_CONF_O_AUTH_KEY_REFRESH = "key-refresh"
    PROPERTY_CONF_O_AUTH_VERIFY_EXP = "verify-exp"

    CONFIG_SECTION_DATABASE = "database"
    PROPERTY_CONF_DB_USER = "db-user"
    PROPERTY_CONF_DB_PASSWORD = "db-password"
    PROPERTY_CONF_DB_NAME = "db-name"
    PROPERTY_CONF_DB_HOST = "db-host"

    CONFIG_SECTION_NEO4J = "neo4j"

    PROTOCOL_LOCAL = "local"
    PROTOCOL_KAFKA = "kafka"

    HOME_DIRECTORY = '/usr/src/app/'
    SUPERBLOCK_LOCATION = HOME_DIRECTORY + "state_recovery.lock"
    CONTROLLER_LOCK_LOCATION = HOME_DIRECTORY + "controller_recovery.lock"
    CONFIGURATION_FILE = "/etc/fabric/actor/config/config.yaml"
    STATE_FILE_LOCATION = '/tmp/fabric_actor.tmp'

    ELASTIC_TIME = "request.elasticTime"
    ELASTIC_SIZE = "request.elasticSize"

    POOL_NAME = 'pool.name'
    POOL_PREFIX = "pool."
    POOLS_COUNT = "pools.count"
    QUERY_ACTION_DISCOVER_POOLS = "discover.pools"
    QUERY_ACTION = "query.action"
    QUERY_RESPONSE = "query.response"
    QUERY_RESPONSE_STATUS = "query.response.status"
    QUERY_RESPONSE_MESSAGE = "query.response.message"
    BROKER_QUERY_MODEL = "bqm"
    POOL_TYPE = "neo4j"

    QUEUE_THRESHOLD = "queue.threshold"

    RESOURCE_MEMORY = "resource.memory"
    RESOURCE_CLASS_INVENTORY_FOR_TYPE = "resource.class.invfortype"
    RESOURCE_AVAILABLE_UNITS = "resource.units.now"

    UNIT_MODIFY_PROP_MESSAGE_SUFFIX = ".message"
    UNIT_MODIFY_PROP_CODE_SUFFIX = ".code"
    UNIT_MODIFY_PROP_PREFIX = "unit.modify."

    SASL_USERNAME = 'sasl.username'
    SASL_PASSWORD = 'sasl.password'
    SASL_MECHANISM = 'sasl.mechanism'
    BOOTSTRAP_SERVERS = 'bootstrap.servers'
    SECURITY_PROTOCOL = 'security.protocol'
    GROUP_ID = 'group.id'
    SSL_CA_LOCATION = 'ssl.ca.location'
    SSL_CERTIFICATE_LOCATION = 'ssl.certificate.location'
    SSL_KEY_LOCATION = 'ssl.key.location'
    SSL_KEY_PASSWORD = 'ssl.key.password'
    SCHEMA_REGISTRY_URL = 'schema.registry.url'

    INVALID_ARGUMENT = "Invalid argument"
    UNINITIALIZED_STATE = "Uninitialized state"
    INVALID_ACTOR_STATE = "Invalid state, actor cannot receive calls"
    OBJECT_NOT_FOUND = "{} not found in datatabase e: {}"
    FAILED_TO_ADD = "Failed to add {} to database e: {}"
    FAILED_TO_REMOVE = "Failed to remove {} from database e: {}"
    NOT_SPECIFIED_PREFIX = "Not {} specified"
    RECOVERY = "[recovery]"
    ISSUE_OPERATION = "Issued {} request for reservation #{} State={}"
    INVALID_PENDING_STATE = "Invalid pending state"
    RESTARTING_ACTIONS = "Restarting configuration actions for reservation #{}"
    RESTARTING_ACTIONS_COMPLETE = "Restarting configuration actions for reservation #{} complete"
    EXTEND_TICKET = "[extend ticket]"

    MANAGEMENT_INTER_ACTOR_OUTBOUND_MESSAGE = "Outbound [Kafka] [Management] Message {} written to {}"
    MANAGEMENT_INTER_ACTOR_INBOUND_MESSAGE = "Inbound [Kafka] [Management] Message received {}"
    MANAGEMENT_API_TIMEOUT_OCCURRED = "Management API Timeout Occurred"
    MANAGEMENT_INTER_ACTOR_MESSAGE_FAILED = "Outbound [Kafka] [Management] Message {} could not be written to {}"

    INVALID_MANAGEMENT_OBJECT_TYPE = "Invalid Management Object type {}"
    NO_POOL = "there is no pool to satisfy this request"
    INVALID_RECOVERY_STATE = "This state should not be reached during recovery"

    INVALID_STATE = "Invalid state"
    INVALID_IP = "Invalid ip address: {}"
    EXCEPTION_OCCURRED = "Exception occurred {}"
    NOT_IMPLEMENTED = "Not Implemented"

    UNSUPPORTED_RESOURCE_TYPE = "Unsupported resource type: {}"

    CLAIMS_SUB = "sub"

    SLIVER_PROPERTY_CORE = 'core'
    SLIVER_PROPERTY_RAM = 'ram'
    SLIVER_PROPERTY_DISK = 'disk'
    SLIVER_PROPERTY_UNIT = 'unit'
    SLIVER_PROPERTY_GRAPH_NODE_ID = 'graph_node_id'

    RESOURCE_TYPE_VM = 'VM'
    RESOURCE_TYPE_BAREMETAL = 'Baremetal'
    RESOURCE_TYPE_CONTAINER = 'Container'

    PROPERTY_EXCEPTION_MESSAGE = "exception.message"
    PROPERTY_TARGET_NAME = "target.name"
    PROPERTY_TARGET_RESULT_CODE = "target.code"
    PROPERTY_TARGET_RESULT_CODE_MESSAGE = "target.code.message"
    PROPERTY_ACTION_SEQUENCE_NUMBER = "action.sequence"

    RESULT_CODE_EXCEPTION = -1
    RESULT_CODE_OK = 0
    TARGET_CREATE = "create"
    TARGET_DELETE = "delete"
    TARGET_MODIFY = "modify"

    RSV_SLC_ID = 'rsv_slc_id'
    DLG_SLC_ID = 'dlg_slc_id'

    BQM_NODE_ID = "bqm_node_id"
    WORKER_NODE_NAME = 'worker_node_name'
    INSTANCE_STATE = 'state'
    INSTANCE_NAME = 'instance_name'
    MANAGEMENT_INTERFACE_MAC_ADDRESS = 'management_interface_mac_address'
