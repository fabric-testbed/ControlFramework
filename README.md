# Control Framework
This repository contains Fabric Control Framework and Actor implementations.

## Overview
Fabric Control Framework has 3 actors
- Controller
- Broker
- Aggregate Manager

## Broker
Broker is an agent of CF that collects resource availability information from multiple aggregate managers and can make resource promises on their behalf.

## Aggregate Manager
AM is a CF agent responsible for managing aggregate resources. Is under the control of the owner of the aggregate. Provides promises of resources to brokers and controllers/ orchestrators.

## Orchestrator
Orchestrator is an agent of CF that makes allocation decisions (embedding) of user requests into available resources. Communicates with user to collect slice requests, communicates with broker or aggregate managers to collect resource promises, communicates with aggregate managers to provision promised resources. Creates slices, configures resources, maintains their state, modifies slices and slivers.  

## Requirements
Python 3.7+

## Configuration
Example configuration files for Network AM, VM AM and Broker can be found under config directory:
```
$ ls -ltr config
total 40
-rw-r--r--  1 komalthareja  staff  4312 Jul 14 14:38 config.net-am.yaml
-rw-r--r--  1 komalthareja  staff  7277 Jul 14 14:38 config.vm-am.yaml
-rw-r--r--  1 komalthareja  staff  3746 Jul 14 14:38 config.broker.yaml
-rw-r--r--  1 komalthareja  staff  3746 Jul 14 14:38 config.orchestrator.yaml
```

## Build Docker Images

### Authority Docker Image
```
docker build -f Dockerfile-auth -t authority .
```

### Broker Docker Image
```
docker build -f Dockerfile-broker -t broker .
```

### Orchestrator Docker Image
```
docker build -f Dockerfile-orchestrator -t orchestrator .
```

## Running with Docker
### Generate Credentials
You must generate CA certificates (or use yours if you already have one) and then generate a keystore and truststore for brokers and clients.
```
cd $(pwd)/secrets
./create-certs.sh
(Type yes for all "Trust this certificate? [no]:" prompts.)
cd -
```
Set the environment variable for the secrets directory. This is used in later commands. Make sure that you are in the MessageBus directory.
```
export KAFKA_SSL_SECRETS_DIR=$(pwd)/secrets
```
### Bring up the containers
You can use the docker-compose.yaml file to bring up a simple Kafka cluster containing
- broker
- zookeeper 
- schema registry

Use the below command to bring up the cluster
```
docker-compose up -d
```

This should bring up following containers:
```
     NAMES
01d8824bc88a        broker:latest                            "python3 -m fabric.b…"   7 minutes ago       Up 7 minutes                                                           fabric-broker
9790175929e7        authority:latest                         "python3 -m fabric.a…"   7 minutes ago       Up 7 minutes                                                           fabric-site-am
df0e3be0b641        confluentinc/cp-schema-registry:latest   "/etc/confluent/dock…"   7 minutes ago       Up 7 minutes        0.0.0.0:8081->8081/tcp                             schemaregistry
82a0a59c117b        confluentinc/cp-kafka:latest             "/etc/confluent/dock…"   7 minutes ago       Up 7 minutes        0.0.0.0:9092->9092/tcp, 0.0.0.0:19092->19092/tcp   broker1
1fea39fedf6a        fabrictestbed/postgres:12.3              "docker-entrypoint.s…"   7 minutes ago       Up 7 minutes        0.0.0.0:8432->5432/tcp                             actordb
c6b824b7d3c6        confluentinc/cp-zookeeper:latest         "/etc/confluent/dock…"   7 minutes ago       Up 7 minutes        2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp         zookeeper
```
Use docker-compose.yml to bring all the containers needed to run Site-AM, Broker and Orchestrator. Add any additional AMs as needed by pointing to the correct config files.
