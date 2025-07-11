# profiling_phase.py

import subprocess
import datetime
import os
import csv
import json
import time
from itertools import product

# Assuming these imports are available from your project structure
from experiment_envs import test_envs
from optimizer.runner import run_xdbserver_and_xdbclient
from experiment_helpers import set_env, create_conf # Assuming create_conf is available or similar utility

# --- Configuration for Profiling Run ---

# Define the test environment for profiling
# This should match an entry in your experiment_envs.py
# Using 'figurePandasPG' as per your previous context
profiling_env_name = "figurePandasPG"
test_env = next((env for env in test_envs if env['name'] == profiling_env_name), None)

if test_env is None:
    print(f"Error: Environment '{profiling_env_name}' not found in test_envs. Please check experiment_envs.py.")
    exit(1)

env = test_env['env']

# Set up the environment (e.g., Docker CPU limits, network)
set_env(env)

# Directory where profiling data (xdbc_server_timings.csv, xdbc_client_timings.csv) will be stored
# This should match the 'perf_dir' expected by optimize.py
perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements_new'))

# Create the performance directory if it doesn't exist
os.makedirs(perf_dir, exist_ok=True)

# Define the parameters and their values to sweep for profiling
# These values are chosen to provide a range of data for the optimizer
# You might need to adjust these based on your specific system and desired profiling depth
parallelism_values = [1, 2, 4, 8] # Common parallelism degrees
buffer_sizes_kb = [1024, 4096]   # Buffer sizes in KB
compression_libs = ['nocomp', 'snappy', 'lz4'] # Common compression libraries

# Define a base configuration. Other parameters will be set to defaults.
# These defaults should be reasonable for your environment.
# Note: 'create_conf' is assumed to exist. If not, you'll need to define a base dict.
# For simplicity, if create_conf is not available, you can define a dictionary directly:
# base_config = {
#     'read_par': 1, 'deser_par': 1, 'comp_par': 1, 'send_par': 1,
#     'rcv_par': 1, 'decomp_par': 1, 'ser_par': 1, 'write_par': 1,
#     'buffer_size': 1024, 'server_buffpool_size': 65536 * 10, 'client_buffpool_size': 65536 * 10,
#     'format': 1, 'compression_lib': 'nocomp', 'skip_ser': 0, 'skip_deser': 0
# }
base_config = create_conf(read_par=1, deser_par=1, comp_par=1, send_par=1, rcv_par=1, decomp_par=1, ser_par=1,
                          write_par=1, buffer_size=1024, server_buffpool_size=65536 * 10, client_buffpool_size=65536 * 10,
                          format=1, compression_lib='nocomp', skip_ser=0, skip_deser=0)


# --- Profiling Loop ---

print(f"Starting XDBC Profiling Phase for environment: {profiling_env_name}")
print(f"Profiling data will be saved to: {perf_dir}")

# Generate all combinations of parameters to profile
# We'll sweep parallelism for read, deserialize, serialize, and write,
# and also sweep buffer size and compression.
# This will create a large number of runs; adjust `parallelism_values`, etc., as needed.
profiling_params = product(
    parallelism_values, # for read_par
    parallelism_values, # for deser_par
    parallelism_values, # for ser_par
    parallelism_values, # for write_par
    buffer_sizes_kb,
    compression_libs
)

total_runs = len(parallelism_values)**4 * len(buffer_sizes_kb) * len(compression_libs)
current_run_count = 0

# Clear existing timing files to ensure a fresh profiling run
# This is important if you want to regenerate the profiling data from scratch
# Otherwise, new data will be appended by runner.py
try:
    os.remove(os.path.join(perf_dir, 'xdbc_server_timings.csv'))
    os.remove(os.path.join(perf_dir, 'xdbc_client_timings.csv'))
    print(f"Removed existing profiling data in {perf_dir} to start fresh.")
except OSError:
    print(f"No existing profiling data found in {perf_dir} to remove. Starting fresh.")
    pass # File might not exist, which is fine

for rp, dp, sp, wp, bs, comp_lib in profiling_params:
    current_run_count += 1
    print(f"\n--- Running Profile {current_run_count}/{total_runs} ---")

    # Create a mutable copy of the base config for the current run
    current_config = base_config.copy()

    # Apply the swept parameters
    current_config['read_par'] = rp
    current_config['deser_par'] = dp
    current_config['ser_par'] = sp
    current_config['write_par'] = wp
    current_config['buffer_size'] = bs
    current_config['compression_lib'] = comp_lib

    # For simplicity, keeping other parallelism parameters at 1 for this sweep,
    # or you could include them in the product if you need more exhaustive profiling.
    current_config['comp_par'] = 1
    current_config['send_par'] = 1
    current_config['rcv_par'] = 1
    current_config['decomp_par'] = 1

    # Adjust format based on compression, as per optimize.py logic
    if (env['src_format'] == 2 or env['target_format'] == 2) or current_config['compression_lib'] != 'nocomp':
        current_config['format'] = 2
    else:
        current_config['format'] = 1

    # Adjust for Pandas target peculiarities, as per optimize.py logic
    if env['target'] == 'pandas':
        current_config['send_par'] = current_config['rcv_par'] = 1
        # The optimize.py code sets ser_par = write_par for pandas, so we'll follow that
        current_config['ser_par'] = current_config['write_par']


    # Execute the data transfer with the current configuration
    # show_output=(True, True) can be useful for debugging individual runs
    print(f"Running with config: {current_config}")
    try:
        # run_xdbserver_and_xdbclient returns the total time, but its side effect
        # is to write the detailed metrics to xdbc_server_timings.csv and xdbc_client_timings.csv
        run_time = run_xdbserver_and_xdbclient(current_config, env, perf_dir, show_output=(False, False))
        if run_time == -1:
            print(f"Run failed for config: {current_config}")
        else:
            print(f"Run completed in {run_time:.2f} seconds.")
    except Exception as e:
        print(f"An error occurred during run for config {current_config}: {e}")
        # Continue to the next configuration even if one fails

print("\n--- Profiling Phase Completed ---")
print(f"Profiling data generated in: {perf_dir}")
print("You can now run figure8a.py, and the optimizer should be able to load this data.")