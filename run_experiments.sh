#!/bin/bash

# This script automates the process of running the experiments.
# It performs the following steps:
# 1. Builds the Docker environment.
# 2. Prepares the data sources (Postgres, Parquet).
# 3. Runs all individual Python scripts to generate experiment data for each figure.
# 4. Runs the plotter script to generate PDF plots and copies all results.
#
# The script will exit immediately if any command fails.

set -e

# --- Introduction ---
echo "Starting the experiment and plotting pipeline..."
echo "----------------------------------------------------"


# --- Step 1: Build Docker Environment ---
echo "Step 1/4: Building Docker environment with 'make build'..."
make build
echo " Docker environment built successfully."
echo "----------------------------------------------------"

# --- Step 2: Prepare Data and Experiments ---
echo "🛠️  Step 2/4: Preparing data sources and experiments..."
make prepare_postgres
make prepare_parquet
echo "Data sources prepared."
echo "----------------------------------------------------"

# --- Step 3: Run All Figure Experiments ---
echo "Step 3/4: Running all figure generation scripts..."

# make run_figure7
# make run_figure7b
# make run_figure8a
# make run_figure8b
# make run_figurePandasPGCPUNet
# make run_figureZParquetCSV
# make run_figure11
# make run_figureACSVCSV
# make run_figureBCSVPG
# make run_figureACSVCSVOpt
# make run_figureBCSVPGOpt
# make run_figureXArrow
# make run_figureYParquet
make run_figure1516a
# make run_figure1516b
# make run_figureMemoryManagement
# make run_figure17b
# make run_figure18
# make run_figure19

echo "All figure experiments completed successfully."
echo "----------------------------------------------------"

# --- Step 4: Generate Plots and Copy Results ---
echo "Step 4/4: Generating plots and copying results with 'make run_plot'..."
make run_plot
echo "Plotting and file copying complete."
echo "----------------------------------------------------"

echo "All steps finished successfully!"
