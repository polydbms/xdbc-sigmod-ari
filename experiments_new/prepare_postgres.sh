#!/bin/bash

# ==============================================================================
#  PostgreSQL Multi-Table/CSV Import Script (from Shared Volume)
#
#  This script automates creating tables and importing multiple CSV files
#  into a PostgreSQL database running inside a Docker container.
#
#  It assumes the directory containing the SQL and CSV files on the host is
#  mounted as a shared volume inside the container.
#
#  For each table, it will:
#  1. Find the corresponding .sql file to create the table structure.
#  2. Execute the .sql file to create the table.
#  3. Find the corresponding .csv file.
#  4. Execute the psql `\copy` command to load the data into the new table.
#
#  Prerequisites:
#  - Docker must be running.
#  - The 'pg1' container must be running.
#  - The host directory (e.g., /dev/shm) must be mounted as a volume in the
#    'pg1' container.
#  - The source .sql and .csv files must exist in the shared directory.
# ==============================================================================
#!/bin/bash

# ==============================================================================
#  PostgreSQL Multi-Table/CSV Import Script
#  MODIFIED to copy files into the target container before execution.
# ==============================================================================

# --- Configuration ---
SHARED_DIR="/dev/shm"
SHARED_SQL_DIR="$SHARED_DIR/sql_scripts"
SHARED_CSV_DIR="$SHARED_DIR"
SQL_DIR="/app/sql_scripts"
CONTAINER_NAME="pg1"
DB_USER="postgres"
DB_NAME="db1"
TABLES=("ss13husallm" "iotm" "inputeventsm")

# --- Table and Script Mapping ---
declare -A TABLE_TO_SQL_MAP
TABLE_TO_SQL_MAP["ss13husallm"]="create_ss13husall.sql"
TABLE_TO_SQL_MAP["iotm"]="create_iot.sql"
TABLE_TO_SQL_MAP["inputeventsm"]="create_inputevents.sql"

# --- Script Logic ---
set -e
echo "Starting bulk table creation and CSV import process..."
echo "======================================================"

# This section prepares the files inside the current container (xdbcexpt)
echo "--------------------------------------------------"
echo "Copying sql scripts to local accessible location (${SHARED_DIR}/sql_scripts)..."
echo "--------------------------------------------------"
mkdir -p "${SHARED_DIR}/sql_scripts"
cp "${SQL_DIR}"/*.sql "${SHARED_DIR}/sql_scripts/"
echo "sql_scripts copied."
echo ""

# Loop through each table name defined in the map keys.
for TABLE_NAME in "${!TABLE_TO_SQL_MAP[@]}"; do
    SQL_FILE_NAME=${TABLE_TO_SQL_MAP[$TABLE_NAME]}
    echo ""
    echo "--- Processing table: $TABLE_NAME ---"

    # Define source paths (in this container) and destination paths (in pg1)
    SRC_SQL_PATH="$SHARED_SQL_DIR/$SQL_FILE_NAME"
    SRC_CSV_PATH="$SHARED_CSV_DIR/$TABLE_NAME.csv"
    DEST_SQL_PATH="/tmp/$SQL_FILE_NAME"
    DEST_CSV_PATH="/tmp/$TABLE_NAME.csv"

    # --- NEW: Step 1/4: Copy SQL file to pg1 ---
    echo "Step 1/4: Copying '$SQL_FILE_NAME' to '$CONTAINER_NAME:$DEST_SQL_PATH'..."
    docker cp "$SRC_SQL_PATH" "$CONTAINER_NAME:$DEST_SQL_PATH"

    # --- MODIFIED: Step 2/4: Create table from .sql script inside pg1 ---
    echo "Step 2/4: Creating table '$TABLE_NAME'..."
    docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -f "$DEST_SQL_PATH";
    echo "Table creation script executed successfully."
    echo "Truncating table to ensure it is empty before import..."
    docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -c "TRUNCATE TABLE $TABLE_NAME;"

    # --- NEW: Step 3/4: Copy CSV file to pg1 ---
    echo "Step 3/4: Copying '$TABLE_NAME.csv' to '$CONTAINER_NAME:$DEST_CSV_PATH'..."
    docker cp "$SRC_CSV_PATH" "$CONTAINER_NAME:$DEST_CSV_PATH"

    # --- MODIFIED: Step 4/4: Import data from .csv file inside pg1 ---
    echo "Step 4/4: Importing data into table '$TABLE_NAME'..."
    COPY_COMMAND="\copy $TABLE_NAME FROM '$DEST_CSV_PATH' WITH (FORMAT csv, HEADER true);"

    if docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -c "$COPY_COMMAND"; then
        echo "Data for '$TABLE_NAME' imported successfully!"
    else
        echo "Error: Failed to import data for table '$TABLE_NAME'."
    fi
    
    # --- NEW: Optional cleanup of temporary files inside pg1 ---
    echo "Cleaning up temporary files from '$CONTAINER_NAME'..."
    docker exec -u root "$CONTAINER_NAME" rm "$DEST_SQL_PATH" "$DEST_CSV_PATH"
done

echo ""
echo "===================================="
echo "Process complete."


echo "--- Verifying All Tables ---"

# Loop through the array of table names
for TABLE in "${TABLES[@]}"; do
    echo ""
    echo "========================================"
    echo "Checking table: $TABLE"
    echo "========================================"

    # Attempt to select one row from the current table
    if docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -c "SELECT * FROM $TABLE LIMIT 1;"; then
        echo "✅ Check successful for table: $TABLE"
    else
        echo "❌ ERROR checking table: $TABLE. It might not exist or be empty."
    fi
done

echo ""
echo "--- Verification complete. ---"