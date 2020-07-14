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
    ErrorInvalidArguments = -7000
    ErrorInvalidActor = -8000
    ErrorInvalidReservation = -9000
    ErrorDatabaseError = -10000
    ErrorInternalError = -11000
    ErrorNoSuchSlice = -15000
    ErrorNoSuchResourcePool = -15010
    ErrorNoSuchReservation = -17200
    ErrorNoSuchBroker = -19000
    ErrorInvalidSlice = -16000
    ErrorNoSuchActor = -12000


class Constants:
    ReservationHasPendingOperation = -200001

    ActorTypeSiteAuthority = 3
    ActorTypeBroker = 2
    ActorTypeController = 1
    ActorTypeAll = 0

    ContainerManagmentObjectID = "manager"

    PropertyPickleProperties = "properties"

    ConfigSectionRuntime = "runtime"
    PropertyConfKafkaServer = "kafka-server"
    PropertyConfKafkaSchemaRegistry = "kafka-schema-registry-url"
    PropertyConfKafkaKeySchema = "kafka-key-schema"
    PropertyConfKafkaValueSchema = "kafka-value-schema"
    PropertyConfPluginDir = "plugin-dir"

    ConfigLoggingSection = 'logging'

    PropertyConfLogFile = 'log-file'
    PropertyConfLogLevel = 'log-level'
    PropertyConfLogRetain = 'log-retain'
    PropertyConfLogSize = 'log-size'
    PropertyConfLogDirectory = 'log-directory'
    PropertyConfLogger = "logger"

    ConfigSectionContainer = "container"
    PropertyConfContainerGuid = "container.guid"

    ConfigSectionTime = "time"
    PropertyConfTimeStartTime = "time.startTime"
    PropertyConfTimeCycleMillis = "time.cycleMillis"
    PropertyConfTimeManual = "time.manual"

    ConfigSectionOAuth = "oauth"
    PropertyConfOAuthJwksUrl = "oauth-jwks-url"

    ConfigSectionDatabase = "database"
    PropertyConfDbUser = "db-user"
    PropertyConfDbPassword = "db-password"
    PropertyConfDbName = "db-name"
    PropertyConfDbHost = "db-host"

    # Type code for proxies using local communication.
    ProtocolLocal = "local"
    # Type code for proxies using Kafka communication.
    ProtocolKafka = "kafka"

    # TODO change after testing
    HomeDirectory = '/usr/src/app/'
    #HomeDirectory = '/Users/komalthareja/renci/code/fabric/ActorBase/actor/test/'
    SuperblockLocation = HomeDirectory + "state_recovery.lock"
    ControllerLockLocation = HomeDirectory + "controller_recovery.lock"
    ConfigurationFile = "/etc/fabric/actor/config/config.yaml"
    StateFileLocation = '/tmp/fabric_actor.tmp'

    TestBrokerConfigurationFile = HomeDirectory + "config/config.broker.yaml"
    TestNetAmConfigurationFile = HomeDirectory + "config/config.net-am.yaml"
    TestVmAmConfigurationFile = HomeDirectory + "config/config.vm-am.yaml"

    ElasticTime = "request.elasticTime"
    ElasticSize = "request.elasticSize"

    SITE = "site"
    BROKER = "broker"
    AUTHORITY = "authority"
    CONTROLLER = "controller"

    PoolPrefix = "pool."
    PoolsCount = "pools.count"
    QueryActionDisctoverPools = "discover.pools"
    QueryAction = "query.action"
    QueryResponse = "query.response"

    ConfigHandler = "config.handler"
    ConfigVictims = "config.victims"
    ConfigImageGuid = "config.image.guid"
    ConfigSSHKeyPattern = "config.ssh.user%d.keys"
    ConfigSSHLoginPattern = "config.ssh.user%d.login"
    ConfigSSHSudoPattern = "config.ssh.user%d.sudo"
    ConfigSSHUrnPattern = "config.ssh.user%d.urn"
    ConfigSSHNumLogins = "config.ssh.numlogins"
    ConfigSSHPrefix = "config.ssh.user"
    ConfigSSHKeySuffix = ".keys"
    ConfigSSHLoginSuffix = ".login"
    ConfigSSHSudoSuffix = ".sudo"
    ConfigSSHUrnSuffix = ".urn"

    ResourceMemory = "resource.memory"
    ResourceCPU = "resource.cpu"
    ResourceBandwidth = "resource.bandwidth"
    ResourceNumCPUCores = "resource.numCPUCores"
    ResourceMemoryCapacity = "resource.memeoryCapacity"
    ResourceStorageCapacity = "resource.storageCapacity"
    ResourceClassInventoryForType = "resource.class.invfortype"
    ResourceAvailableUnits = "resource.units.now"
    ResourceStartIface = "resource.siface"
    ResourceEndIface = "resource.eiface"
    ResourceDomain = "resource.domain"

    UnitManagementIP = "unit.manage.ip"
    UnitManagementPort = "unit.manage.port"
    UnitManageSubnet = "unit.manage.subnet"
    UnitManageGateway = "unit.manage.gateway"

    UnitDataSubnet = "unit.data.subnet"

    UnitParentHostName = "unit.parent.hostname"
    UnitHostName = "unit.hostname"
    UnitControl = "unit.control"
    UnitMemory = "unit.memory"

    UnitVlanTag = "unit.vlan.tag"
    UnitVlanQoSRate = "unit.vlan.qos.rate"
    UnitVlanQoSBurstSize = "unit.vlan.qos.burst.size"

    ConfigUnitTag = "config.unit.tag"

    PropertyStartVlan = "vlan.tag.start"
    PropertyEndVlan = "vlan.tag.end"
    PropertyVlanRangeNum = "vlan.range.num"

    PropertyStartLUN = "lun.tag.start"
    PropertyEndLUN = "lun.tag.end"
    PropertyLunRangeNum = "lun.range.num"

    UnitLUNTag = "unit.target.lun"
    UnitStorageCapacity = "unit.target.capacity"

    KafkaTopic = "kafka-topic"
    Name = "name"
    Type = "type"
    Guid = "guid"

    PropertyClassName = "ObjectClassName"
    PropertyModuleName = "ModuleName"
    PropertyID = "MOID"
    PropertyTypeID = "MOTYPEID"
    PropertyActorName = "MOActorName"
    PropertyProxiesLength = "MOProxiesLength"
    PropertyProxiesPrefix = "MOProxiesPrefix."
    PropertyProxiesProtocol = ".protocol"
    PropertyProxiesClass = ".class"
    PropertyProxiesModule = ".module"

    ExtendSameUnits = -1

    ErrorInvalidArguments = -7000
    ErrorInvalidActor = -8000
    ErrorInvalidReservation = -9000
    ErrorDatabaseError = -10000
    ErrorInternalError = -11000
    ErrorNoSuchActor = -12000
    ErrorNoSuchSlice = -15000
    ErrorNoSuchResourcePool = -15010
    ErrorInvalidSlice = -16000
    ErrorNoSuchReservation = -17200
    ErrorNoSuchBroker = -19000
    ErrorTransportFailure = -20000

    AllReservationStates = -1

    ManagementApiTimeoutInSeconds = 120.0