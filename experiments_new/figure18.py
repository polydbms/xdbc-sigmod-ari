import subprocess
import datetime
import os
import csv
import json
from optimizer.runner import run_xdbserver_and_xdbclient
from experiment_helpers import set_env, create_conf, create_file
import copy

# --- Experiment Configuration ---


#1 .  create environments 
environments = [
    {
        'name': "env_16s_16c_1000net_Lineitem",
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
            'table': 'lineitem_sf10',
        }
    },
    {
        'name': "env_16s_16c_1000net_ICU",
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
            'table': 'inputeventsm',
        }
    },
    {
        'name': "env_16s_1c_10net_Lineitem",
        'env': {
            'server_cpu': 16,
            'client_cpu': 1,
            'network': 10,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'csv',
            'src_format': 1,
            'target': 'csv',
            'target_format': 1,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcclient',
            'table': 'lineitem_sf10',
        }
        },
        {
            'name': "env_16s_1c_10net_ICU",
            'env': {
                'server_cpu': 16,
                'client_cpu': 1,
                'network': 10,
                'network_latency': 0,
                'network_loss': 0,
                'src': 'csv',
                'src_format': 1,
                'target': 'csv',
                'target_format': 1,
                'server_container': 'xdbcserver',
                'client_container': 'xdbcclient',
                'table': 'inputeventsm',
            }
        }
    ]

compression_types = ['nocomp', 'lz4', 'lzo', 'snappy', 'zstd']
format_types = [1,2]
buffer_pool_sizes = [40 * 2048]  # Example buffer pool sizes
buffer_sizes = [32, 64, 128, 256, 512, 1024, 2048]  # Example buffer sizes
repetitions = 1 # Set how many times each experiment should be repeated

fixed_configs = [
    {
        'name': "env_16s_16c_1000net",
        'config_type': {
                "network_parallelism": 1,
                "client_write_par": 16,
                "client_decomp_par": 4,
                "server_read_partitions": 1,
                "server_read_par": 1,
                "server_deser_par": 4,
                "server_comp_par": 4,
            }
    },
    {
        'name': "env_16s_1c_10net",
        'config_type': {
                "network_parallelism": 1,
                "client_write_par": 1,
                "client_decomp_par": 8,
                "server_read_partitions": 1,
                "server_read_par": 8,
                "server_deser_par": 1,
                "server_comp_par": 8,
            }
    }
]

configurations = []

# Iterate through all combinations to create the configuration list
for config in fixed_configs:
    for comp_lib in compression_types:
        for fmt in format_types:
            for bufpool_size in buffer_pool_sizes:
                for buff_size in buffer_sizes:
                    # Create a descriptive name for the configuration
                    run_name = (
                        f"{config['name']}_{comp_lib}_f{fmt}_"
                        f"bp{bufpool_size}_b{buff_size}"
                    )

                    # Create the run_config dictionary
                    run_config = {
                        'read_par': config['config_type']['server_read_par'],
                        'deser_par': config['config_type']['server_deser_par'],
                        'write_par': config['config_type']['client_write_par'],
                        'send_par': config['config_type']['network_parallelism'],
                        'rcv_par': config['config_type']['network_parallelism'],
                        'comp_par': config['config_type']['server_comp_par'],
                        'decomp_par': config['config_type']['client_decomp_par'],
                        'ser_par': config['config_type']['client_write_par'],
                        'buffer_size': buff_size,
                        'server_buffpool_size': bufpool_size,
                        'client_buffpool_size': bufpool_size,
                        'format': fmt,
                        'skip_ser': 0,
                        'skip_deser': 0,
                        'compression_lib': comp_lib
                    }

                    # Add the new configuration to the list
                    configurations.append({
                        'name': run_name,
                        'run_config': run_config
                    })

# --- Script Execution ---

# Setup output file
csv_file_path = "res/figure18.csv"
if not os.path.exists("res"):
    os.makedirs("res")
if not os.path.exists("res/xdbc_plans"):
    os.makedirs("res/xdbc_plans")

sample_run_config = configurations[0]['run_config']
sample_cur_conf = create_conf(**sample_run_config) # Generate a sample to get its structure
# Create CSV with a header that matches the logged data
if not os.path.exists(csv_file_path):
    header_base = [
        "timestamp", "env_name", "repetition"
    ]
    # Use keys from the ACTUAL data structure that will be written to the file
    run_config_keys = list(sample_cur_conf.keys())
    full_header = header_base + run_config_keys + ["time"] + ["data_size"]
    with open(csv_file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(full_header)


total_iterations = len(environments) * len(configurations) * repetitions
global_iteration_counter = 0
print(f"--- Starting {total_iterations}  experiments. ---")

# Main experiment loop
for env_config in environments:
    env = env_config['env']
    print(f"--- Preparing environment: {env_config['name']} ---")
    set_env(env)

    for transfer_config in configurations:
        if not transfer_config['name'].startswith(env_config['name'].split('_Lineitem')[0].split('_ICU')[0]):
            global_iteration_counter += repetitions
            continue
        run_config = transfer_config['run_config']
        print(f"--- Starting experiments with the configuration : {transfer_config['name']} ---")

        for i in range(repetitions):
            print(f"Running: Rep {i+1}/{repetitions}")
            

            cur_conf = create_conf(**run_config)

            # Define perf directory for measurements
            perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
            
            # Run the client-server and get the execution time
            t, data_size = run_xdbserver_and_xdbclient(copy.deepcopy(cur_conf), env, perf_dir, show_output=(False, False),return_size=True)
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