#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
# The container where the cleaning script will be executed.
# Based on your logs, 'xdbcexpt' is the main experiment container.
TARGET_CONTAINER="xdbcexpt"

# The Python script that performs the cleaning and conversion.
CLEANER_SCRIPT="./clean_and_convert2tbl.py"

# Define the input and output file paths on your local machine (the "host").
HOST_INPUT_CSV="/dev/shm/lineitem_sf10.csv"
HOST_OUTPUT_TBL="/dev/shm/lineitem_sf10.tbl"

# Define the temporary working directory and paths *inside* the container.
CONTAINER_WORKDIR="/tmp"
CONTAINER_SCRIPT_PATH="${CONTAINER_WORKDIR}/clean_and_convert.py"
CONTAINER_INPUT_PATH="${CONTAINER_WORKDIR}/lineitem_sf10.csv"
CONTAINER_OUTPUT_PATH="${CONTAINER_WORKDIR}/lineitem_sf10.tbl"


# --- Main Script Logic ---

echo "=========================================================="
echo "      Manual Data Cleaning and Conversion Script"
echo "=========================================================="
echo

# --- 1. Pre-flight Checks ---
echo "--- Step 1: Verifying required files on host ---"
if [ ! -f "$CLEANER_SCRIPT" ]; then
    echo "Error: Cleaner script not found at ${CLEANER_SCRIPT}"
    exit 1
fi
if [ ! -f "$HOST_INPUT_CSV" ]; then
    echo "Error: Input CSV file not found at ${HOST_INPUT_CSV}"
    exit 1
fi
echo "All required files found."
echo

# --- 2. File Preparation ---
echo "--- Step 2: Copying files into container '${TARGET_CONTAINER}' ---"
docker cp "$CLEANER_SCRIPT" "${TARGET_CONTAINER}:${CONTAINER_SCRIPT_PATH}"
docker cp "$HOST_INPUT_CSV" "${TARGET_CONTAINER}:${CONTAINER_INPUT_PATH}"
echo "Files copied successfully."
echo

# --- 3. Execution ---
echo "--- Step 3: Running the cleaning script inside the container ---"
# Execute the python script inside the container using the container-side paths
docker exec -i "${TARGET_CONTAINER}" python3 "${CONTAINER_SCRIPT_PATH}" \
    "${CONTAINER_INPUT_PATH}" \
    "${CONTAINER_OUTPUT_PATH}"
echo "Script executed successfully."
echo

# --- 4. Retrieve Result ---
echo "--- Step 4: Copying the clean .tbl file back to the host ---"
docker cp "${TARGET_CONTAINER}:${CONTAINER_OUTPUT_PATH}" "${HOST_OUTPUT_TBL}"
echo "Clean file retrieved."
echo

# --- 5. Final Confirmation ---
echo "=========================================================="
echo "          Conversion Complete!"
echo "----------------------------------------------------------"
echo " -> Messy Input:  ${HOST_INPUT_CSV}"
echo " -> Clean Output: ${HOST_OUTPUT_TBL}"
echo "=========================================================="