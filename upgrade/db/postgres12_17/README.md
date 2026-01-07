# Upgrading from PostgreSQL 12 to 17

PostgreSQL major versions (12 to 17) are not binary-compatible. You must migrate the data directory using `pg_upgrade`.

## Migration Steps

1. **Perform the Upgrade** Run this as a user with `sudo` privileges to ensure correct file ownership:
```bash
docker run --rm \
  -e POSTGRES_INITDB_ARGS="--username=fabric" \
  -v /opt/data/beta/cf/orchestrator/postgres:/var/lib/postgresql/12/data \
  -v /opt/data/beta/cf/orchestrator/postgres_new:/var/lib/postgresql/17/data \
  tianon/postgres-upgrade:12-to-17 \
  --username=fabric

```

2. **Swap Data Directories**
```bash
cd /opt/data/beta/cf/orchestrator/
mv postgres postgres_v12_old
mv postgres_new postgres

```

3. **Start the New Container** Update your `docker-compose.yml` image to `postgres:17` and run:
```bash
docker compose up -d

```

4. **Run Post-Migration Maintenance** Run the maintenance script (below) to refresh collations, reindex, and update network permissions.

---

## 2. Maintenance Script (`pg_finalize.sh`)

This script automates the collation refresh for all databases, reindexing, and HBA configuration.

```bash
./pg_finalize.sh <container_name> <path of the volume>

```
