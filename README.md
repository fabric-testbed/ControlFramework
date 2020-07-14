# Actors
Fabric Control Framework Actors

## Overview
Fabric Control Framework has 3 actors
- Controller
- Broker
- Aggregate Manager

## Broker
Broker is an agent of CF that collects resource availability information from multiple aggregate managers and can make resource promises on their behalf.

## Aggregate Manager
AM is a CF agent responsible for managing aggregate resources. Is under the control of the owner of the aggregate. Provides promises of resources to brokers and controllers/ orchestrators.

## Controller
Controller is an agent of CF that makes allocation decisions (embedding) of user requests into available resources. Communicates with user to collect slice requests, communicates with broker or aggregate managers to collect resource promises, communicates with aggregate managers to provision promised resources. Creates slices, configures resources, maintains their state, modifies slices and slivers.  

### Requirements
Python 3.7+

### Configuration
Example configuration files for Network AM, VM AM and Broker can be found under config directory:
```
$ ls -ltr config
total 40
-rw-r--r--  1 komalthareja  staff  4312 Jul 14 14:38 config.net-am.yaml
-rw-r--r--  1 komalthareja  staff  7277 Jul 14 14:38 config.vm-am.yaml
-rw-r--r--  1 komalthareja  staff  3746 Jul 14 14:38 config.broker.yaml
```

### Build Docker Images

#### Authority Docker Image
```
docker build -f Dockerfile-auth -t authority .
```

#### Broker Docker Image
```
docker build -f Dockerfile-broker -t broker .
```

### Running with Docker
AM container can be brought up via docker-compose as indicated below:
```
    fabric-vm-am:
        image: authority:latest 
        container_name: fabric-vm-am 
        restart: always
        depends_on:
        - database 
        volumes:
            - "./config/config.vm-am.yaml:/etc/fabric/actor/config/config.yaml"
            - "./logs/vm-am:/var/log/actor"
```
Broker container can be brought up via docker-compose as indicated below:
```
    fabric-broker:
        image: broker:latest
        container_name: fabric-broker
        restart: always
        depends_on:
            - database
            - fabric-vm-am 
        volumes:
            - "./config/config.broker.yaml:/etc/fabric/actor/config/config.yaml"
            - "./logs/broker:/var/log/actor"
```
Use docker-compose.yml to bring all the containers needed to run VM-AM and Broker. Add any additional AMs as needed by pointing to the correct config files.

### Plugins
The actor loads all the plugins in "plugins" directory. Each plugin must inherit from IBasePlugin class and have an info file. 
