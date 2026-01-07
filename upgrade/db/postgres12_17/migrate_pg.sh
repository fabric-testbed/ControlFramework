#!/bin/bash

# Usage: sudo ./migrate_pg.sh <base_path> <relative_data_subpath>
BASE_PATH=$1
DATA_SUBPATH=$2

# --- CONFIGURATION ---
DB_USER="fabric"
OLD_IMG="fabrictestbed/postgres:12.3"
NEW_IMG="fabrictestbed/postgres:17.7"
POSTGRES_UID=999

if [[ -z "$BASE_PATH" || -z "$DATA_SUBPATH" ]]; then
    echo "❌ Error: Missing arguments."
    echo "Usage: sudo $0 <base_path> <relative_data_subpath>"
    echo "Example: sudo $0 /opt/data/beta/cf/renc-am postgres/pgdata"
    exit 1
fi

# Construct full path to old data
OLD_DATA_FULL="$BASE_PATH/$DATA_SUBPATH"
NEW_DATA_FULL="$BASE_PATH/${DATA_SUBPATH}_new"

if [[ ! -d "$OLD_DATA_FULL" ]]; then
    echo "❌ Error: Data directory not found at $OLD_DATA_FULL"
    exit 1
fi

if [[ ! -f "docker-compose.yml" ]]; then
    echo "❌ Error: docker-compose.yml not found in $BASE_PATH"
    exit 1
fi

echo "🚀 Starting migration"
echo "📍 Base: $BASE_PATH"
echo "📂 Old Data: $OLD_DATA_FULL"
echo "📂 New Data: $NEW_DATA_FULL"

# 1. Stop the old stack
echo "📥 Stopping containers..."
docker compose down

# 2. Cleanup & Permissions
echo "🧹 Preparing directories and fixing permissions..."
sudo rm -f "$OLD_DATA_FULL/postmaster.pid"
sudo rm -rf "$NEW_DATA_FULL"
mkdir -p "$NEW_DATA_FULL"

# Fix ownership so the container (UID 999) can write logs and data
sudo chown -R $POSTGRES_UID:$POSTGRES_UID "$OLD_DATA_FULL"
sudo chown -R $POSTGRES_UID:$POSTGRES_UID "$NEW_DATA_FULL"

# 3. Perform Upgrade
echo "⚙️ Running pg_upgrade..."
docker run --rm \
  -e POSTGRES_INITDB_ARGS="--username=$DB_USER" \
  -v "$OLD_DATA_FULL":/var/lib/postgresql/12/data \
  -v "$NEW_DATA_FULL":/var/lib/postgresql/17/data \
  tianon/postgres-upgrade:12-to-17 \
  --username="$DB_USER"

if [ $? -ne 0 ]; then
    echo "❌ Upgrade failed! Reverting permissions..."
    sudo chown -R root:root "$OLD_DATA_FULL"
    exit 1
fi

# 4. Swap Directories
echo "📂 Swapping data directories..."
mv "$OLD_DATA_FULL" "${OLD_DATA_FULL}_v12_old"
mv "$NEW_DATA_FULL" "$OLD_DATA_FULL"

# 5. Update docker-compose.yml
echo "📝 Updating image name in docker-compose.yml..."
sed -i "s|$OLD_IMG|$NEW_IMG|g" docker-compose.yml

# 6. Start the new stack
echo "🆙 Starting updated stack..."
docker compose up -d database

# 7. Post-Migration Maintenance
echo "🛠️ Running maintenance (Collations, Reindex, HBA)..."
sleep 10

CONTAINER_NAME=$(docker compose ps -q database 2>/dev/null || docker compose ps -q postgres 2>/dev/null)
if [[ -z "$CONTAINER_NAME" ]]; then
    CONTAINER_NAME=$(docker ps --filter "ancestor=$NEW_IMG" --format "{{.Names}}" | head -n 1)
fi

if [[ -n "$CONTAINER_NAME" ]]; then
    echo "Using container: $CONTAINER_NAME"
    # Fix Collations
    DBS=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -qAt -c "SELECT datname FROM pg_database WHERE datallowconn")
    for db in $DBS; do
        docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$db" -c "ALTER DATABASE \"$db\" REFRESH COLLATION VERSION;"
    done
    # Optimize
    docker exec "$CONTAINER_NAME" vacuumdb -U "$DB_USER" --all --analyze-in-stages
    docker exec "$CONTAINER_NAME" reindexdb -U "$DB_USER" --all

    # HBA Check & Reload
    HBA_FILE="$OLD_DATA_FULL/pg_hba.conf"
    if ! grep -q "0.0.0.0/0" "$HBA_FILE"; then
        echo "host all all 0.0.0.0/0 md5" | sudo tee -a "$HBA_FILE"
        docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -c "SELECT pg_reload_conf();"
    fi
fi

# 6. Start the new stack
echo "🆙 Starting updated stack..."
docker compose stop
docker compose up -d

echo "✅ Migration Complete!"