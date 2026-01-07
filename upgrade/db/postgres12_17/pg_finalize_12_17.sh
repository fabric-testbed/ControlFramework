#!/bin/bash

# Usage: ./pg_finalize.sh <container_name> <volume_path>
CONTAINER_NAME=$1
VOLUME_PATH=$2
DB_USER="fabric"

if [[ -z "$CONTAINER_NAME" || -z "$VOLUME_PATH" ]]; then
    echo "Usage: $0 <container_name> <volume_path>"
    echo "Example: $0 broker-db /opt/data/beta/cf/broker/postgres"
    exit 1
fi

echo "--- Starting Post-Migration Cleanup for $CONTAINER_NAME ---"

# 1. Refresh Collation Versions for all databases
echo "Refreshing collation versions..."
DBS=$(docker exec $CONTAINER_NAME psql -U $DB_USER -qAt -c "SELECT datname FROM pg_database WHERE datallowconn")

for db in $DBS; do
    echo "  Processing: $db"
    docker exec $CONTAINER_NAME psql -U $DB_USER -d "$db" -c "ALTER DATABASE \"$db\" REFRESH COLLATION VERSION;"
done

# 2. Rebuild Statistics
echo "Updating database statistics (analyze)..."
docker exec $CONTAINER_NAME vacuumdb -U $DB_USER --all --analyze-in-stages

# 3. Reindex all databases
echo "Reindexing all databases (this may take a moment)..."
docker exec $CONTAINER_NAME reindexdb -U $DB_USER --all

# 4. Update pg_hba.conf for network access
echo "Updating pg_hba.conf..."
HBA_FILE="$VOLUME_PATH/pg_hba.conf"
if [[ -f "$HBA_FILE" ]]; then
    # Check if rule already exists to avoid duplicates
    if ! grep -q "0.0.0.0/0" "$HBA_FILE"; then
        echo "host all all 0.0.0.0/0 md5" | sudo tee -a "$HBA_FILE"
        docker exec $CONTAINER_NAME psql -U $DB_USER -c "SELECT pg_reload_conf();"
        echo "Restarting container to apply network changes..."
        docker restart $CONTAINER_NAME
    else
        echo "Network rule already exists in pg_hba.conf."
    fi
else
    echo "Warning: pg_hba.conf not found at $HBA_FILE"
    echo "host all all 0.0.0.0/0 md5" | sudo tee -a "$HBA_FILE"
    docker exec $CONTAINER_NAME psql -U $DB_USER -c "SELECT pg_reload_conf();"
    echo "Restarting container to apply network changes..."
    docker restart $CONTAINER_NAME
fi

echo "--- Migration Finalized Successfully ---"
