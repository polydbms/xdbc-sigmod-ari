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

# --- Configuration ---
# The shared directory path, which must be accessible from both the host
# and inside the container.
SHARED_DIR="/dev/shm"
SHARED_SQL_DIR="$SHARED_DIR/sql_scripts"
SHARED_CSV_DIR="$SHARED_DIR"
SQL_DIR="/app/sql_scripts"

# The name of your PostgreSQL Docker container.
CONTAINER_NAME="pg1"

# PostgreSQL connection details.
DB_USER="postgres"
DB_NAME="db1"

# --- Table and Script Mapping ---
# Use a Bash associative array to map table names to their SQL creation scripts.
declare -A TABLE_TO_SQL_MAP
# TABLE_TO_SQL_MAP["ss13husallm"]="create_ss13husall.sql"
TABLE_TO_SQL_MAP["iotm"]="create_iot.sql"
TABLE_TO_SQL_MAP["inputeventsm"]="create_inputevents.sql"


# --- Script Logic ---

# Exit immediately if a command exits with a non-zero status.
set -e

echo "Starting bulk table creation and CSV import process..."
echo "======================================================"

# Copy the schemas directory to /dev/shm to make it accessible
echo "--------------------------------------------------"
echo "Copying sql scripts to accessible location (${SHARED_DIR}/sql_scripts)..."
echo "--------------------------------------------------"
# Create the schemas directory if it doesn't exist
mkdir -p "${SHARED_DIR}/sql_scripts"
# Copy all JSON schema files to the directory
cp "${SQL_DIR}"/*.sql "${SHARED_DIR}/sql_scripts/"
echo "sql_scripts copied."
echo ""

# Loop through each table name defined in the map keys.
for TABLE_NAME in "${!TABLE_TO_SQL_MAP[@]}"; do
    SQL_FILE_NAME=${TABLE_TO_SQL_MAP[$TABLE_NAME]}
    echo ""
    echo "--- Processing table: $TABLE_NAME ---"

    # --- Step 1: Create table from .sql script ---
    SQL_FILE_PATH="$SHARED_SQL_DIR/$SQL_FILE_NAME"

    # Check if the source SQL file exists from the host's perspective.
    if [ ! -f "$SQL_FILE_PATH" ]; then
        echo "Error: SQL script not found at '$SQL_FILE_PATH'. Skipping this table."
        continue
    fi

    echo "Step 1/2: Creating table '$TABLE_NAME' using '$SQL_FILE_NAME'..."
    # Use psql -f to execute the script file. Any "table already exists" errors
    # will be shown but won't stop the script if the table is already there.
    if docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -f "$SQL_FILE_PATH"; then
        echo "Table creation script executed successfully."
    else
        echo "Warning: Issue executing '$SQL_FILE_NAME'. The table might already exist, or there could be a syntax error. Continuing..."
    fi


    # --- Step 2: Import data from .csv file ---
    CSV_FILE_PATH="$SHARED_CSV_DIR/$TABLE_NAME.csv"

    # Check if the source CSV file exists from the host's perspective.
    if [ ! -f "$CSV_FILE_PATH" ]; then
        echo "Error: Source CSV file not found at '$CSV_FILE_PATH'. Skipping data import."
        continue
    fi

    # Since the directory is shared, we directly execute the \copy command
    # inside the container, referencing the file's path in the shared volume.
    echo "Step 2/2: Importing data into table '$TABLE_NAME' from '$CSV_FILE_PATH'..."
    COPY_COMMAND="\copy $TABLE_NAME FROM '$CSV_FILE_PATH' WITH (FORMAT csv, HEADER true);"

    if docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -c "$COPY_COMMAND"; then
        echo "Data for '$TABLE_NAME' imported successfully!"
    else
        echo "Error: Failed to import data for table '$TABLE_NAME'. Check PostgreSQL logs, data format, or file permissions inside the container."
        continue
    fi
done

echo ""
echo "===================================="
echo "Process complete."
