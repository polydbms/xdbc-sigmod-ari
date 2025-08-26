import subprocess
import datetime
import os
import csv
import json
from optimizer.runner import run_xdbserver_and_xdbclient
from experiment_helpers import set_env, create_conf, create_file
import copy

# --- Experiment Configuration ---

# 1. Define the environments to test
# Network is in Mbps. The 'name' is used for logging.
environments = [
    {
        'name': "env_16c_16c_0net",
        'env': {
            'server_cpu': 16,
            'client_cpu': 16,
            'network': 0,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'csv',
            'src_format': 1,
            'target': 'csv',
            'target_format': 1,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcclient',
            'tables': ['lineitem_sf10'],
            'table': 'lineitem_sf10'
        }
    },
    {
        'name': "env_16c_16c_1000net",
        'env': {
            'server_cpu': 16,
            'client_cpu': 16,
            'network': 1000,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'csv',
            'src_format': 1,
            'target': 'csv',
            'target_format': 1,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcclient',
            'tables': ['lineitem_sf10'],
            'table': 'lineitem_sf10'
        }
    }
]

# 2. Define the parameters to iterate over
# Note: Assuming 'zsttd' was a typo and 'zstd' was intended.
compression_types = ['nocomp', 'lz4', 'lzo', 'snappy', 'zstd']
parallelism_levels = [1, 2, 4, 8, 16]
repetitions = 1 # Set how many times each experiment should be repeated

# 3. Define fixed configuration parameters
# These are applied to all experiment runs.
fixed_config = {
    'read_par': 1,
    'deser_par': 8,
    'write_par': 16,
    'send_par': 1,  # Kept from original base config
    'rcv_par': 1,   # Kept from original base config
    'ser_par': 8,   # Kept from original base config
    'buffer_size': 1024,
    'server_buffpool_size': 81920,
    'client_buffpool_size': 81920,
    'format': 1,
    'skip_ser': 0,
    'skip_deser': 0
}

# --- Script Execution ---

# Setup output file
csv_file_path = "res/figure19.csv"
if not os.path.exists("res"):
    os.makedirs("res")
if not os.path.exists("res/xdbc_plans"):
    os.makedirs("res/xdbc_plans")

# Create CSV with a new header that includes 'compression'
if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "timestamp", "env", "repetition", "compression",
            "read_par", "deser_par", "comp_par",
            "send_par", "rcv_par", "decomp_par",
            "ser_par", "write_par",
            "table", "time"
        ])

# Main experiment loop
for env_config in environments:
    env = env_config['env']
    print(f"--- Starting experiments for environment: {env_config['name']} ---")
    
    # Prepare environment
    set_env(env)
    
    for comp_lib in compression_types:
        for par_level in parallelism_levels:
            for i in range(repetitions):
                print(f"Running: Rep {i+1}/{repetitions}, Compression: {comp_lib}, Parallelism: {par_level}")

                # Create the specific configuration for this run
                run_config = fixed_config.copy()
                run_config['compression_lib'] = comp_lib
                # Set both compression and decompression parallelism to the same value
                run_config['comp_par'] = par_level
                run_config['decomp_par'] = par_level
                
                cur_conf = create_conf(**run_config)

                # Define perf directory for measurements
                perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
                
                # Run the client-server and get the execution time
                t = run_xdbserver_and_xdbclient(cur_conf, env, perf_dir, show_output=(False, False))
                print(f"Result: {t:.4f} seconds\n")

                # Log the results
                timestamp = int(datetime.datetime.now().timestamp())
                with open(csv_file_path, mode="a", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow([
                        timestamp, env_config['name'], i + 1, cur_conf['compression_lib'],
                        cur_conf['read_par'], cur_conf['deser_par'], cur_conf['comp_par'],
                        cur_conf['send_par'], cur_conf['rcv_par'], cur_conf['decomp_par'],
                        cur_conf['ser_par'], cur_conf['write_par'],
                        env['table'], t
                    ])

                # Save the plan for this specific run
                with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
                    json.dump(cur_conf, file, indent=4)

print("--- All experiments completed. ---")