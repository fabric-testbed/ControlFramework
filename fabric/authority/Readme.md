# Aggregate Manager
An aggregate manager(AM) controls access to the substrate components. It controls some set of infrastructure resources in a particular site consisting of a set of servers, storage units, network elements or other components under common ownership and control. AMs inform brokers about available resources by passing to the resource advertisement information models. AMs may be associated with more than one broker and the partitioning of resources between brokers is the decision left to the AM. Oversubscription is possible, depending on the deployment needs.
FABRIC enables a substrate provider to outsource resource arbitration and calendar scheduling to a broker. By delegating resources to the broker, the AM consents to the broker’s policies, and agrees to try to honor reservations issued by the broker if the user has authorization on the AM. 

Besides common code, each AM type has specific plugins that determine its resource allocation behavior (Resource Management Policy) and the specific actions it takes to provision a sliver (Resource Handler). Both plugins are invoked by AM common core code based on the resource type or type of request being considered.

## Configuration
`config.site.am.yaml` depicts an example config file for an Aggregate Manager.

## Deployment
Aggregate Manager must deploy following containers:
- Neo4j
- Postgres Database
- Policy Enforcement Function (TBD)
- Authority

`docker-compose.yml` file present in this directory brings up all the required containers

### Environment and Configuration

Your Project must be configured prior to running it for the first time. Example configuration files have been provided as templates to start from.

Do not check any of your configuration files into a repository as they will contain your projects secrets (use .gitignore to exclude any files containing secrets).

1. .env from [env.template](env.template) - Environment variables for `docker-compose.yml` to use

#### .env
A file named `env.template` has been provided as an example, and is used by the `docker-compose.yml` file.
```
cp env.template .env
```
Once copied, modify the default values for each to correspond to your desired deployment. The UID and GID based entries should correspond to the values of the user responsible for running the code as these will relate to shared volumes from the host to the running containers.
```
# docker-compose environment file
#
# When you set the same environment variable in multiple files,
# here’s the priority used by Compose to choose which value to use:
#
#  1. Compose file
#  2. Shell environment variables
#  3. Environment file
#  4. Dockerfile
#  5. Variable is not defined

# Neo4J configuration
NEO4J_DATA_PATH_DOCKER=/data
NEO4J_DATA_PATH_HOST=./neo4j/data
NEO4J_GID=1000
NEO4J_HOST=neo4j
NEO4J_IMPORTS_PATH_DOCKER=/imports
NEO4J_IMPORTS_PATH_HOST=./neo4j/imports
NEO4J_LOGS_PATH_DOCKER=/logs
NEO4J_LOGS_PATH_HOST=./neo4j/logs
NEO4J_PASS=password
NEO4J_UID=1000
NEO4J_USER=neo4j
NEO4J_dbms_connector_bolt_advertised__address=0.0.0.0:7687
NEO4J_dbms_connector_bolt_listen__address=0.0.0.0:7687
NEO4J_dbms_connector_http_advertised__address=0.0.0.0:7474
NEO4J_dbms_connector_http_listen__address=0.0.0.0:7474
NEO4J_dbms_connector_https_advertised__address=0.0.0.0:7473
NEO4J_dbms_connector_https_listen__address=0.0.0.0:7473

# postgres configuration
POSTGRES_HOST=database
POSTGRES_PORT=5432
POSTGRES_USER=fabric
POSTGRES_PASSWORD=fabric
PGDATA=/var/lib/postgresql/data/pgdata
POSTGRES_DB=am
```
### Build
Once all configuration has been done, the user can build the necessary containers by issuing:
```
docker-compose build
```
### Run
#### database
Create the database directories if they do not exist
```
mkdir -p pg_data/data pg_data/logs

```
Start the pre-defined PostgreSQL database in Docker
```
docker-compose up -d database
```
Validate that the database container is running.
```
$ docker-compose ps
  Name                Command              State           Ports
-------------------------------------------------------------------------
database   docker-entrypoint.sh postgres   Up      0.0.0.0:8432->5432/tcp

```
#### neo4j
Create the neo4j directories if they do not exist
```
mkdir -p neo4j/data neo4j/imports neo4j/logs
echo password > neo4j/password
```
Start the pre-defined Neo4j database in Docker
```
docker-compose up -d neo4j
```
Validate that the database container is running.
```
docker-compose ps
     Name                   Command               State                                   Ports
--------------------------------------------------------------------------------------------------------------------------------
site1-am-neo4j   /sbin/tini -g -- /docker-e ...   Up      0.0.0.0:7473->7473/tcp, 0.0.0.0:7474->7474/tcp, 0.0.0.0:7687->7687/tcp
```
#### am
Update `docker-compose.yml` to point to correct volumes for the AM.

```
    volumes:
      - ./neo4j:/usr/src/app/neo4j
      - ./config.site.am.yaml:/etc/fabric/actor/config/config.yaml
      - ./logs/:/var/log/actor
      - ../../secrets/snakeoil-ca-1.crt:/etc/fabric/message_bus/ssl/cacert.pem
      - ../../secrets/kafkacat1.client.key:/etc/fabric/message_bus/ssl/client.key
      - ../../secrets/kafkacat1-ca1-signed.pem:/etc/fabric/message_bus/ssl/client.pem
      - ../../config/neo4j/site-am-2broker-ad-enumerated.graphml:/etc/fabric/actor/config/neo4j/site-am-2broker-ad-enumerated.graphml
      - ./pubkey.pem:/etc/fabric/message_bus/ssl/credmgr.pem
```
Start the pre-defined AM container in Docker
```
docker-compose up -d am
```
Validate that the database container is running.
```
rciadmins-MacBook-Pro-3:authority komalthareja$ docker-compose ps
     Name                   Command                 State                                      Ports
-------------------------------------------------------------------------------------------------------------------------------------
site1-am         /usr/src/app/authority.sh        Up           0.0.0.0:11000->11000/tcp
```
