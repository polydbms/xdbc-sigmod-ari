#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
# Base directory for shared memory where data is stored and served.
DATA_DIR="/dev/shm"
SCHEMA_DIR="/app/schemas"
CONVERSION_SCRIPT_PATH="./convert_csv_to_parquet.py" 
PYTHON_CONTAINER="xdbcpython"
HTTP_PORT=1234

# --- Function to Prepare a Dataset ---
prepare_dataset() {
  local dataset_name=$1
  local max_file_size_mb=$2
  
  local csv_file="${DATA_DIR}/${dataset_name}.csv"
  local schema_file="${DATA_DIR}/schemas/${dataset_name}.json" # Updated path
  local output_dir="${DATA_DIR}/${dataset_name}"

  echo "--------------------------------------------------"
  echo "Preparing dataset: ${dataset_name}"
  echo "--------------------------------------------------"

  # Check if required files exist before proceeding
  if [ ! -f "$csv_file" ]; then
    echo "Error: CSV file not found at ${csv_file}"
    exit 1
  fi
  if [ ! -f "$schema_file" ]; then
    echo "Error: Schema file not found at ${schema_file}"
    exit 1
  fi

  # Create the output directory for the Parquet files
  echo "Creating output directory: ${output_dir}"
  mkdir -p "${output_dir}"

  # Run the Python script to convert CSV to Parquet
  echo "Converting ${csv_file} to Parquet format inside container '${PYTHON_CONTAINER}'..."
  docker exec -i "${PYTHON_CONTAINER}" python3.9 /workspace/convert_csv_to_parquet.py \
    --csv_file "${csv_file}" \
    --schema_file "${schema_file}" \
    --output_dir "${output_dir}" \
    --max_size_mb "${max_file_size_mb}"
  
  echo "Successfully prepared dataset: ${dataset_name}"
  echo ""
}

# Copy the schemas directory to /dev/shm to make it accessible
echo "--------------------------------------------------"
echo "Copying schemas to accessible location (${DATA_DIR}/schemas)..."
echo "--------------------------------------------------"
# Create the schemas directory if it doesn't exist
mkdir -p "${DATA_DIR}/schemas"
# Copy all JSON schema files to the directory
cp "${SCHEMA_DIR}"/*.json "${DATA_DIR}/schemas/"
echo "Schemas copied."
echo ""

echo "--------------------------------------------------"
echo "Copying conversion script to container..."
echo "--------------------------------------------------"
docker cp "${CONVERSION_SCRIPT_PATH}" "${PYTHON_CONTAINER}":/workspace/
echo "Script copied."
echo ""

# --- 1. Data Preparation Phase ---
# Convert the required CSV files into Parquet datasets.
# You can add or remove datasets here as needed.
prepare_dataset "lineitem_sf10" 64
prepare_dataset "iotm" 64
prepare_dataset "ss13husallm" 64
prepare_dataset "inputeventsm" 64

# # --- 2. Experiment Execution Phase ---
# echo "--------------------------------------------------"
# echo "Starting Experiment"
# echo "--------------------------------------------------"

# # Navigate to the directory that needs to be served
# cd "${DATA_DIR}"

# # Start the HTTP server in the background
# echo "Starting HTTP server on port ${HTTP_PORT}..."
# python3 -m RangeHTTPServer ${HTTP_PORT} &
# # Get the process ID (PID) of the background server
# SERVER_PID=$!

# # Give the server a moment to start up
# sleep 2

# echo "Server started with PID: ${SERVER_PID}. Ready to serve data."
# echo "Running the main experiment script..."

# # --- Run your actual experiment command here ---
# # This example is based on your initial error log.
# # The client script connects to http://xdbcserver:1234/lineitem_sf10
# # NOTE: Ensure 'xdbcserver' correctly resolves to this container's IP.
# python /workspace/tests/pandas_parquet.py \
#   --table 'lineitem_sf10' \
#   --debug=0 \
#   --system pyarrow

# # You could add other experiment runs here, for example:
# # python /workspace/tests/pandas_parquet.py --table 'iotm' ...
# # --- 3. Cleanup Phase ---
# echo ""
# echo "--------------------------------------------------"
# echo "Experiment finished. Cleaning up..."
# echo "--------------------------------------------------"

# # Stop the background HTTP server
# kill ${SERVER_PID}
# echo "Stopped HTTP server (PID: ${SERVER_PID})."

echo "Script completed successfully."
