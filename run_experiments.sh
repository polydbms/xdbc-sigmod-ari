#!/bin/bash

# This script automates the process of running the experiments.
# It performs the following steps:
# 1. Clones the necessary XDBC repositories.
# 2. Sets up the XDBC environment.
# 3. Downloads datasets from the TUB cloud.
# 4. Builds the Docker environment.
# 5. Prepares the data sources (Postgres, Parquet).
# 6. Runs all individual Python scripts to generate experiment data for each figure.
# 7. Runs the plotter script to generate PDF plots and copies all results.
#
# The script will exit immediately if any command fails.

set -e

# --- Configuration: Select which steps to run ---
# Set the following variables to 'true' to run the corresponding step,
# or 'false' to skip it.
RUN_STEP_1_CLONE=true
RUN_STEP_2_SETUP=true
RUN_STEP_3_DOWNLOAD=true
RUN_STEP_4_BUILD=true
RUN_STEP_5_PREPARE=true
RUN_STEP_6_EXPERIMENTS=true
RUN_STEP_7_PLOT=true

    
# --- Introduction ---
echo "Starting the experiment and plotting pipeline..."
echo "----------------------------------------------------"


# --- Step 1: Cloning XDBC Repositories ---
if [ "$RUN_STEP_1_CLONE" = true ]; then
    echo "Step 1/7: Cloning XDBC repository..."
    cd ..
    # remove if cloned already before
    rm -rf xdbc-client
    git clone --branch test/reproduce --single-branch https://github.com/polydbms/xdbc-client.git
    rm -rf xdbc-server
    git clone --branch test/reproduce --single-branch https://github.com/polydbms/xdbc-server.git
    rm -rf xdbc-python
    git clone --branch midhun/test/dima_cluster --single-branch https://github.com/polydbms/xdbc-python.git 
    rm -rf xdbc-spark
    git clone  --single-branch https://github.com/polydbms/xdbc-spark.git
    rm -rf pg_xdbc_fdw
    git clone --branch adjusting --single-branch https://github.com/polydbms/pg_xdbc_fdw.git
    cd xdbc-sigmod-ari

    echo "Cloned XDBC repository successfully."
else
    echo "Skipping Step 1: Cloning XDBC Repositories."
fi
echo "----------------------------------------------------"


# --- Step 2: Setting up XDBC  ---
if [ "$RUN_STEP_2_SETUP" = true ]; then
    echo "Step 2/7: Setting up XDBC..."

    # Create Docker network if it doesn't exist
    # docker network create xdbc-net 2>/dev/null || true
    make -C .././xdbc-client
    make -C .././xdbc-server
    make -C .././xdbc-python
    make -C .././xdbc-spark
    make -C .././pg_xdbc_fdw
    docker compose -f .././xdbc-client/docker-xdbc.yml up -d
    docker compose -f .././xdbc-client/docker-tc.yml up -d
    docker run -d -it --rm --name xdbcspark --network xdbc-net -p 4040:4040 -p 18080:18080 spark3io-sbt:latest

    echo "XDBC setup completed successfully."
else
    echo "Skipping Step 2: Setting up XDBC."
fi
echo "----------------------------------------------------"


# --- Step 3: Downloading Datasets ---
if [ "$RUN_STEP_3_DOWNLOAD" = true ]; then
    echo "Step 3/7: Downloading datasets from TUB cloud..."

    mkdir -p datasets
    cd datasets
    for f in inputeventsm.csv.tar.gz iotm.csv.tar.gz lineitem.tbl.tar.gz lineitem_sf10.csv.tar.gz ss13husallm.csv.tar.gz; do
        if [ ! -f "$f" ]; then
            wget -O "$f" "https://tubcloud.tu-berlin.de/s/FTxy2w6mGt4f5cK/download?path=%2F&files=$f"
        fi
    done
    cd ..
    echo "Extracting datasets..."
    find datasets -type f -name "*.tar.gz" -exec tar --overwrite -xzf {} -C /dev/shm \;

    echo "Datasets downloaded and extracted successfully."
else
    echo "Skipping Step 3: Downloading Datasets."
fi
echo "----------------------------------------------------"


# --- Step 4: Build Docker Environment ---
if [ "$RUN_STEP_4_BUILD" = true ]; then
    echo "Step 4/7: Building Docker environment with 'make build'..."
    make build
    echo "Docker environment built successfully."
else
    echo "Skipping Step 4: Building Docker Environment."
fi
echo "----------------------------------------------------"


# --- Step 5: Prepare Data and Experiments ---
if [ "$RUN_STEP_5_PREPARE" = true ]; then
    echo "Step 5/7: Preparing data sources and experiments..."
    make prepare_postgres
    make prepare_parquet
    make prepare_tbl
    echo "Data sources prepared."
else
    echo "Skipping Step 5: Preparing Data and Experiments."
fi
echo "----------------------------------------------------"


# --- Step 6: Run All Figure Experiments ---
if [ "$RUN_STEP_6_EXPERIMENTS" = true ]; then
    echo "Step 6/7: Running all figure generation scripts..."
    # Clean previous CSV results
    make clean_csvs

    # Run experiments for each figure
    # Uncomment the lines below to run specific figures

    make run_figure6
    make run_figure6b
    make run_figure7a
    make run_figure7b
    make run_figure8PandasPGCPUNet
    make run_figure10ZParquetCSV
    make run_figure11 
    make run_figure12aCSVCSV
    make run_figure12bCSVPG
    make run_figure13aCSVCSVOpt
    make run_figure13bCSVPGOpt
    make run_figure14aXArrow
    make run_figure14bYParquet
    make run_figure1516a
    make run_figure1516b
    make run_figure17aMemoryManagement
    make run_figure17b
    make run_figure18
    make run_figure19
    make run_figure20

    echo "All figure experiments completed successfully."
else
    echo "Skipping Step 6: Running Figure Experiments."
fi
echo "----------------------------------------------------"


# --- Step 7: Generate Plots and Copy Results ---
if [ "$RUN_STEP_7_PLOT" = true ]; then
    echo "Step 7/7: Generating plots and copying results with 'make run_plot'..."
    make clean_pdfs
    make run_plot
    echo "Plotting and file copying complete."
else
    echo "Skipping Step 7: Generating Plots and Copying Results."
fi
echo "----------------------------------------------------"


echo "All selected steps finished successfully!"