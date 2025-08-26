import subprocess
import datetime
import os
import csv
import json
from optimizer.runner import run_xdbserver_and_xdbclient
from experiment_helpers import set_env, create_conf, create_file
import copy
import pprint
import random

import argparse
parser = argparse.ArgumentParser(description='Run XDB experiment with configurable parameters')
parser.add_argument('--max-configurations', type=int, default=12, 
                   help='Maximum number of configurations to generate')
args = parser.parse_args()

# --- Experiment Configuration ---
max_configurations = args.max_configurations  # Use this instead of the hardcoded value

# --- Experiment Configuration ---


# 1. Create environments
dataset_map = {
    'lineitem_sf10': 'lineitem',
}

environments = []
for dataset, table in dataset_map.items():
    environments.extend([
        {
            'name': f"env_8s_2c_500net_{dataset}",
            'env': {
                'server_cpu': 8,
                'client_cpu': 2,
                'network': 500,
                'network_latency': 0,
                'network_loss': 0,
                'src': 'csv',
                'src_format': 1,
                'target': 'csv',
                'target_format': 1,
                'server_container': 'xdbcserver',
                'client_container': 'xdbcclient',
                'table': dataset,
            }
        },
    ])

# --- 1. Define Fixed Environment Settings ---
compression_types = ['nocomp', 'zstd', 'lzo', 'lz4', 'snappy']
format_types = [1]
buffer_sizes = [1024]
repetitions = 1

# --- 2. Define the Baseline & Sweep Parameters ---

# These are the possible values that each swept parameter can take.
sweep_values = [1, 2, 4, 8]

# --- 3. Generate Random Configurations ---
# max_configurations = 1150
configurations = []
# This set will store a tuple representation of each generated config to prevent duplicates.
seen_configurations = set()

# All parallelism parameters to be randomized
all_parallelism_params = [
    "client_write_par",
    "client_ser_par",
    "client_decomp_par",
    "server_read_par",
    "server_deser_par",
    "server_comp_par",
]

print(f"--- Generating up to {max_configurations} random configurations... ---")

while len(configurations) < max_configurations:
    # A) Randomly create a baseline configuration for parallelism
    current_config = {
        'network_parallelism': 1  # Always use 1 for network parallelism as requested
    }
    for param in all_parallelism_params:
        current_config[param] = random.choice(sweep_values)

    # B) Randomly select other parameters
    comp_lib = random.choice(compression_types)
    fmt = 1
    buff_size = 1024

    # C) Create a unique key to check for duplicate configurations
    config_key = (
        comp_lib,
        fmt,
        buff_size,
        current_config['client_write_par'],
        current_config['client_ser_par'],
        current_config['client_decomp_par'],
        current_config['server_read_par'],
        current_config['server_deser_par'],
        current_config['server_comp_par']
    )

    # If this exact configuration already exists, skip and generate a new one
    if config_key in seen_configurations:
        continue
    
    seen_configurations.add(config_key)

    # D) Calculate buffer pool sizes using the specified formulas
    server_values = [current_config['server_read_par'], current_config['server_deser_par'], current_config['server_comp_par']]
    client_values = [current_config['client_write_par'], current_config['client_ser_par'], current_config['client_decomp_par']]
    
    server_buffpool_size = (sum(server_values) + 5) * buff_size * 3
    client_buffpool_size = (sum(client_values) + 5) * buff_size * 3

    # E) Create a descriptive name and the final run configuration
    run_name = f"random_run_{len(configurations) + 1}"

    run_config = {
        'read_par': current_config['server_read_par'],
        'deser_par': current_config['server_deser_par'],
        'write_par': current_config['client_write_par'],
        'send_par': current_config['network_parallelism'],
        'rcv_par': current_config['network_parallelism'],
        'comp_par': current_config['server_comp_par'],
        'decomp_par': current_config['client_decomp_par'],
        'ser_par': current_config['client_ser_par'], # Correctly maps to client serialization
        'buffer_size': buff_size,
        'server_buffpool_size': server_buffpool_size,
        'client_buffpool_size': client_buffpool_size,
        'format': fmt,
        'skip_ser': 0,
        'skip_deser': 0,
        'compression_lib': comp_lib
    }

    # F) Add the fully defined experiment to the list
    configurations.append({
        'name': run_name,
        'run_config': run_config
    })

# Optional: Print the first few configurations to verify
print(f"Generated {len(configurations)} unique configurations.")
pprint.pprint(configurations[:3])
print("...")
pprint.pprint(configurations[-3:])

# --- Script Execution ---

# Setup output file
csv_file_path = "res/figure7b.csv"
if not os.path.exists("res"):
    os.makedirs("res")
if not os.path.exists("res/xdbc_plans"):
    os.makedirs("res/xdbc_plans")

# Create CSV with a header that matches the logged data
sample_run_config = configurations[0]['run_config']
sample_cur_conf = create_conf(**sample_run_config) # Generate a sample to get its structure
if not os.path.exists(csv_file_path):
    header_base = [
        "timestamp", "env_name", "repetition"
    ]
    # Get keys from the first generated run_config
    if configurations:
        run_config_keys = list(sample_cur_conf.keys())
        full_header = header_base + run_config_keys + ["time"] + ["data_size"]
        with open(csv_file_path, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(full_header)
    else:
        print("Warning: No configurations were generated. CSV file will be empty.")


total_iterations = len(environments) * len(configurations) * repetitions
global_iteration_counter = 0
print(f"--- Starting {total_iterations} experiments. ---")

# Main experiment loop
for env_config in environments:
    env = env_config['env']
    print(f"--- Preparing environment: {env_config['name']} ---")
    set_env(env)

    for transfer_config in configurations:
        run_config = transfer_config['run_config']
        print(f"--- Starting experiments with the configuration : {transfer_config['name']} ---")

        for i in range(repetitions):
            print(f"Running: Rep {i+1}/{repetitions}")

            cur_conf = create_conf(**run_config)

            # Define perf directory for measurements
            perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
            
            # Run the client-server and get the execution time
            t, data_size = run_xdbserver_and_xdbclient(copy.deepcopy(cur_conf), env, perf_dir, show_output=(False, False), return_size=True)
            print(f"Result: {t:.4f} seconds\n")

            # Log the results
            timestamp = int(datetime.datetime.now().timestamp())

            with open(csv_file_path, mode="a", newline="") as file:
                writer = csv.writer(file)

                # Start with the base values
                row_data = [
                    timestamp,
                    env_config['name'],
                    i + 1
                ]
                row_data.extend(cur_conf.values())
                row_data.append(t)
                row_data.append(data_size)

                writer.writerow(row_data)

            # Save the plan for this specific run
            with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
                json.dump(cur_conf, file, indent=4)

            global_iteration_counter += 1
            print(f"--- Overall Progress: {global_iteration_counter}/{total_iterations} ---")
            
print("--- All experiments completed. ---")