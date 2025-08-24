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
dataset_map = {
    'lineitem_sf10': 'lineitem',
}

environments = []
for dataset, table in dataset_map.items():
    environments.extend([
        {
            'name': f"env_2s_2c_125net_{dataset}",
            'env': {
                'server_cpu': 2,
                'client_cpu': 2,
                'network': 125,
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
        {
            'name': f"env_16s_2c_125net_{dataset}",
            'env': {
                'server_cpu': 16,
                'client_cpu': 2,
                'network': 125,
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
import pprint

# --- 1. Define Fixed Environment Settings ---
compression_types = ['snappy']
format_types = [1]
buffer_pool_sizes = [81920]
buffer_sizes = [1024]
repetitions = 1

# --- 2. Define the Baseline & Sweep Parameters ---

# These are the default values for the parameters.
baseline_config = {
    "network_parallelism": 1,
    "client_write_par": 16,
    "client_decomp_par": 4,
    "server_read_par": 1,
    "server_deser_par": 4,
    "server_comp_par": 4,
}

# These are the parameters you want to test individually.
params_to_sweep = [
    "client_write_par",
    "client_decomp_par",
    "server_read_par",
    "server_deser_par",
    "server_comp_par",
]

# These are the values that each swept parameter will take.
sweep_values = [1, 2, 4, 8, 16]

# --- 3. Generate Configurations ---
configurations = []

baseline_included = False
for param_name in params_to_sweep:
    for param_value in sweep_values:
        current_config = baseline_config.copy()
        current_config[param_name] = param_value
        
        if current_config == baseline_config:
            if baseline_included:
                continue
            else:
                baseline_included = True
        
        # (This inner part is for other fixed settings like compression, buffer size, etc.)
        for comp_lib in compression_types:
            for fmt in format_types:
                for bufpool_size in buffer_pool_sizes:
                    for buff_size in buffer_sizes:
                        
                        # Create a descriptive name for the specific run
                        run_name = (
                            f"sweep_{param_name}={param_value}_base_f{fmt}_"
                            f"bp{bufpool_size}_b{buff_size}"
                        )

                        # Map the parameter names to the final run configuration keys
                        run_config = {
                            'read_par': current_config['server_read_par'],
                            'deser_par': current_config['server_deser_par'],
                            'write_par': current_config['client_write_par'],
                            'send_par': current_config['network_parallelism'],
                            'rcv_par': current_config['network_parallelism'],
                            'comp_par': current_config['server_comp_par'],
                            'decomp_par': current_config['client_decomp_par'],
                            'ser_par': current_config['client_write_par'],
                            'buffer_size': buff_size,
                            'server_buffpool_size': bufpool_size,
                            'client_buffpool_size': bufpool_size,
                            'format': fmt,
                            'skip_ser': 0,
                            'skip_deser': 0,
                            'compression_lib': comp_lib
                        }

                        # Add the fully defined experiment to the list
                        configurations.append({
                            'name': run_name,
                            'run_config': run_config
                        })

# Optional: Print the first few configurations to verify
print(f"Generated {len(configurations)} configurations.")
pprint.pprint(configurations[:3])
print("...")
pprint.pprint(configurations[-3:])

# --- Script Execution ---

# Setup output file
csv_file_path = "res/figure1516a.csv"
if not os.path.exists("res"):
    os.makedirs("res")
if not os.path.exists("res/xdbc_plans"):
    os.makedirs("res/xdbc_plans")

# Create CSV with a header that matches the logged data
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
    with open(csv_file_path, mode="a", newline="") as file:
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
        # if not transfer_config['name'].startswith(env_config['name'].split('_')[0]):
        #     continue
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