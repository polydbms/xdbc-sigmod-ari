import subprocess
import datetime
import os
import csv
import json
from optimizer.runner import run_xdbserver_and_xdbclient
from experiment_helpers import set_env, create_conf, create_file
import copy

# --- Experiment Configuration ---

# 1. Define the parameters to iterate over
compression_types = ['nocomp', 'lz4', 'lzo', 'snappy', 'zstd']
network_speeds = [15.75, 31.5, 125, 250, 500] # In Mbps
repetitions = 1 # Set how many times each experiment should be repeated

# 2. Define the fixed configuration for the new experiment
config = {
    'server_cpu': 16,
    'client_cpu': 16,
    'table': 'lineitem_sf10',
    "network_parallelism": 1,
    "client_write_par": 16,
    "client_decomp_par": 4,
    "server_read_partitions": 1, # Note: Mapped to server_read_par
    "server_read_par": 1,
    "server_deser_par": 4,
    "server_comp_par": 4,
    "format": 2,
    "bufpool_size": 40960,
    "buff_size": 512
}

# 3. Dynamically create environments based on network speeds
environments = []
for net_speed in network_speeds:
    environments.append({
        'name': f"env_16c_16c_{net_speed}net",
        'env': {
            'server_cpu': config['server_cpu'],
            'client_cpu': config['client_cpu'],
            'network': net_speed,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'csv',
            'src_format': config['format'],
            'target': 'csv',
            'target_format': config['format'],
            'server_container': 'xdbcserver',
            'client_container': 'xdbcclient',
            'tables': [config['table']],
            'table': config['table']
        }
    })

# --- Script Execution ---

# Setup output file
csv_file_path = "res/figure17b.csv"
if not os.path.exists("res"):
    os.makedirs("res")
if not os.path.exists("res/xdbc_plans"):
    os.makedirs("res/xdbc_plans")

# Create CSV with a header that matches the logged data
if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "timestamp", "env", "repetition", "compression", "network_mbps",
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
        for i in range(repetitions):
            print(f"Running: Rep {i+1}/{repetitions}, Compression: {comp_lib}, Network: {env['network']}Mbps")

            # Create the specific configuration for this run from the fixed config
            run_config = {
                'read_par': config['server_read_par'],
                'deser_par': config['server_deser_par'],
                'write_par': config['client_write_par'],
                'send_par': config['network_parallelism'],
                'rcv_par': config['network_parallelism'],
                'comp_par': config['server_comp_par'],
                'decomp_par': config['client_decomp_par'],
                'ser_par': config['server_deser_par'], # Assuming ser_par should match deser_par
                'buffer_size': config['buff_size'],
                'server_buffpool_size': config['bufpool_size'],
                'client_buffpool_size': config['bufpool_size'],
                'format': config['format'],
                'skip_ser': 0,
                'skip_deser': 0,
                'compression_lib': comp_lib
            }
            

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
                    timestamp, env_config['name'], i + 1, cur_conf['compression_lib'], env['network'],
                    cur_conf['read_par'], cur_conf['deser_par'], cur_conf['comp_par'],
                    cur_conf['send_par'], cur_conf['rcv_par'], cur_conf['decomp_par'],
                    cur_conf['ser_par'], cur_conf['write_par'],
                    env['table'], t
                ])

            # Save the plan for this specific run
            with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
                json.dump(cur_conf, file, indent=4)

print("--- All experiments completed. ---")