#!/bin/bash

# Usage: sudo ./migrate_pg.sh /opt/data/beta/cf/orchestrator
BASE_PATH=$1

# --- CONFIGURATION ---
DB_USER="fabric"
OLD_IMG="fabrictestbed/postgres:12.3"
NEW_IMG="fabrictestbed/postgres:17.7"

if [[ -z "$BASE_PATH" ]]; then
    echo "❌ Error: BASE_PATH not provided."
    echo "Usage: sudo $0 <absolute_path_to_service_folder>"
    echo "Example: sudo $0 /opt/data/beta/cf/orchestrator"
    exit 1
fi

if [[ ! -f "$BASE_PATH/docker-compose.yml" ]]; then
    echo "❌ Error: No docker-compose.yml found in $BASE_PATH"
    exit 1
fi

cd "$BASE_PATH" || exit 1

echo "🚀 Starting migration in $BASE_PATH"

# 1. Stop the old stack
echo "📥 Stopping containers..."
docker-compose down

# 2. Cleanup
echo "🧹 Cleaning up lock files and old attempts..."
sudo rm -f "$BASE_PATH/postgres/postmaster.pid"
sudo rm -rf "$BASE_PATH/postgres_new"
mkdir -p "$BASE_PATH/postgres_new"

# 3. Perform Upgrade
echo "⚙️ Running pg_upgrade (Link Mode)..."
docker run --rm \
  -e POSTGRES_INITDB_ARGS="--username=$DB_USER" \
  -v "$BASE_PATH":/var/lib/postgresql/migration \
  tianon/postgres-upgrade:12-to-17 \
  --old-datadir=/var/lib/postgresql/migration/postgres \
  --new-datadir=/var/lib/postgresql/migration/postgres_new \
  --username="$DB_USER" --link

if [ $? -ne 0 ]; then
    echo "❌ Upgrade failed! Check logs above."
    exit 1
fi

# 4. Swap Directories
echo "📂 Swapping data directories..."
mv "$BASE_PATH/postgres" "$BASE_PATH/postgres_v12_old"
mv "$BASE_PATH/postgres_new" "$BASE_PATH/postgres"

# 5. Update docker-compose.yml
echo "📝 Updating image name in docker-compose.yml..."
sed -i "s|$OLD_IMG|$NEW_IMG|g" docker-compose.yml

# 6. Start the new stack
echo "🆙 Starting updated stack..."
docker-compose up -d

# 7. Run Post-Migration Maintenance
echo "🛠️ Running maintenance (Collations, Reindex, HBA)..."
echo "Waiting for database to initialize..."
sleep 10

# Identify the container name dynamically
CONTAINER_NAME=$(docker-compose ps -q database 2>/dev/null || docker-compose ps -q postgres 2>/dev/null)
if [[ -z "$CONTAINER_NAME" ]]; then
    # Fallback: find any running postgres 17 container
    CONTAINER_NAME=$(docker ps --filter "ancestor=$NEW_IMG" --format "{{.Names}}")
fi

if [[ -n "$CONTAINER_NAME" ]]; then
    echo "Using container: $CONTAINER_NAME"

    # Fix Collations for all DBs
    DBS=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -qAt -c "SELECT datname FROM pg_database WHERE datallowconn")
    for db in $DBS; do
        echo "  Refreshing collation: $db"
        docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$db" -c "ALTER DATABASE \"$db\" REFRESH COLLATION VERSION;"
    done

    # Analyze and Reindex
    docker exec "$CONTAINER_NAME" vacuumdb -U "$DB_USER" --all --analyze-in-stages
    docker exec "$CONTAINER_NAME" reindexdb -U "$DB_USER" --all

    # HBA Check & Reload
    HBA_FILE="$BASE_PATH/postgres/pg_hba.conf"
    if ! grep -q "0.0.0.0/0" "$HBA_FILE"; then
        echo "host all all 0.0.0.0/0 md5" | sudo tee -a "$HBA_FILE"
        docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -c "SELECT pg_reload_conf();"
    fi
else
    echo "⚠️ Warning: Could not find running container to perform maintenance."
fi

echo "✅ Migration Complete!"