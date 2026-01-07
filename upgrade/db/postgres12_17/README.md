# PostgreSQL 12 to 17 Upgrade Guide

PostgreSQL major versions are not binary-compatible. To move from **12.3** to **17.7**, you must migrate the data directory using the `pg_upgrade` utility. This process is automated via the `migrate_pg.sh` script.

## Prerequisites

* All commands must be executed as the **root** user (or using `sudo`).
* Ensure you have enough disk space to accommodate a temporary copy of the database (if not using link mode).
* Verify that you are in the correct environment where the Docker containers are running.


## Migration Steps

### 1. Prepare the Workspace

Navigate to the project directory where your database container was originally spawned and copy the migration script to that location.

```bash
# Navigate to the service directory
cd /home/nrig-service/ControlFramework/fabric_cf/authority/renc-am

# Copy the migration script from the upgrade repository
cp /home/nrig-service/ControlFramework/upgrade/db/postgres12_17/migrate_pg.sh .

# Ensure the script is executable
chmod +x migrate_pg.sh

```

### 2. Identify Data Volumes

Before running the script, you must determine the absolute path to your PostgreSQL data. Open your `docker-compose.yml` file and look for the `volumes` section under the database service.

**Example structure to look for:**

```yaml
volumes:
  - /opt/data/beta/cf/renc-am/postgres/pgdata:/var/lib/postgresql/data

```

### 3. Execute the Migration

Run the script by providing two arguments:

1. **Base Path:** The directory containing the `docker-compose.yml`.
2. **Relative Data Subpath:** The path from the base to the directory containing the `PG_VERSION` file.

```bash
# Syntax: ./migrate_pg.sh <base_path> <relative_data_subpath>
./migrate_pg.sh /opt/data/beta/cf/renc-am/ postgres/pgdata

```