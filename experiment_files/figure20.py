import subprocess
import datetime
import os
import csv
import json
from optimizer.runner import run_xdbserver_and_xdbclient
from optimizer.optimize import optimize
from experiment_helpers import set_env, create_conf, create_file
import copy
from profiling_phase import generate_historical_data


def calculate_config_throughput(env, config):
    """Calculate estimated throughput for a configuration"""
    from optimizer.optimizers import HeuristicsOptimizer
    from optimizer.config import loader
    from optimizer.config.helpers import Helpers
    import math
    import os
    
    perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements_new'))
    
    # Load throughput data
    throughput_data = Helpers.load_throughput(
        env, perf_dir, 
        compression=config.get('compression_lib', 'nocomp'),
        consider_skip_ser=False
    )
    
    if throughput_data is None:
        print("Warning: No throughput data found, using 0 as estimate")
        return 0
    
    # Set up optimizer parameters
    mode = 2
    params = {
        "f0": 0.3,
        "a": 0.02,
        "upper_bounds": loader.upper_bounds[f"{env['target']}_{env['src']}"][mode],
        "max_total_workers_server": math.floor(env["server_cpu"] * 1.2),
        "max_total_workers_client": math.floor(env["client_cpu"] * 1.2),
        "compression_libraries": ["lzo", "snappy", "nocomp", "lz4", "zstd"],
        "env": env
    }
    
    # Calculate throughput
    optimizer = HeuristicsOptimizer(params)
    estimated_thr = optimizer.calculate_throughput(config, throughput_data, False)
    return estimated_thr

# --- Experiment Configuration ---


#1 .  create environments 
environments = [
    {
        'name': "iot_analysis",
        'env': {
            'server_cpu': 16,
            'client_cpu': 8,
            'network': 100,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'postgres',
            'src_format': 1,
            'target': 'pandas',
            'target_format': 2,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcpython',
            'tables': ['iotm'], 
            'table': 'iotm',
        }
    },
    {
        'name': "backup",
        'env': {
            'server_cpu': 32,
            'client_cpu': 16,
            'network': 1000,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'postgres',
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
        'name': "icu_analysis",
        'env': {
            'server_cpu': 16,
            'client_cpu': 12,
            'network': 50,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'csv',
            'src_format': 1,
            'target': 'pandas',
            'target_format': 2,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcpython',
            'tables': ['inputeventsm'], 
            'table': 'inputeventsm'
        }
    },
    {
        'name': "copy",
        'env': {
            'server_cpu': 8,
            'client_cpu': 8,
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
        'name': "etl",
        'env': {
            'server_cpu': 8,
            'client_cpu': 8,
            'network': 500,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'postgres',
            'src_format': 1,
            'target': 'spark',
            'target_format': 1,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcspark',
            'tables': ['lineitem_sf10'], 
            'table': 'lineitem_sf10'
        }
    },
    # {
    #     'name': "pg",
    #     'env': {
    #         'server_cpu': 16,
    #         'client_cpu': 16,
    #         'network': 100,
    #         'network_latency': 0,
    #         'network_loss': 0,
    #         'src': 'postgres',
    #         'src_format': 1,
    #         'target': 'postgres',
    #         'target_format': 1,
    #         'server_container': 'xdbcserver',
    #         'client_container': 'xdbcpostgres',
    #         'tables': ['lineitem_sf10'], 
    #         'table': 'lineitem_sf10'
    #     }
    # }
]

# compression_types = ['nocomp', 'lz4', 'lzo', 'snappy', 'zstd']
# format_types = [1,2]
# buffer_pool_sizes = [40 * 2048]  # Example buffer pool sizes
# buffer_sizes = [32, 64, 128, 256, 512, 1024, 2048]  # Example buffer sizes
repetitions = 1 # Set how many times each experiment should be repeated

fixed_configs = [
    {
        'name': "iot_analysis",
        'config_type': {
            'read_par': 4,
            'deser_par': 4,
            'comp_par': 4,
            'send_par': 2,
            'rcv_par': 2,
            'decomp_par': 3,
            'write_par': 3,
            'compression_lib': 'zstd',
            'buffer_size': 128,
            'server_buffpool_size': 4 * 128 * 20,
            'client_buffpool_size': 2 * 128 * 20,
            'format': 2
        }
    },
    {
        'name': "backup",
        'config_type': {
            'read_par': 8,
            'deser_par': 8,
            'comp_par': 4,
            'send_par': 2,
            'rcv_par': 2,
            'decomp_par': 4,
            'write_par': 10,
            'compression_lib': 'snappy',
            'buffer_size': 256,
            'server_buffpool_size': 8 * 256 * 20,
            'client_buffpool_size': 4 * 256 * 20,
            'format': 1
        }
    },
    {
        'name': "icu_analysis",
        'config_type': {
            'read_par': 4,
            'deser_par': 4,
            'comp_par': 2,
            'send_par': 2,
            'rcv_par': 2,
            'decomp_par': 2,
            'write_par': 2,
            'compression_lib': 'zstd',
            'buffer_size': 64,
            'server_buffpool_size': 4 * 64 * 20,
            'client_buffpool_size': 2 * 64 * 20,
            'format': 2
        }
    },
    {
        'name': "copy",
        'config_type': {
            'read_par': 4,
            'deser_par': 4,
            'comp_par': 2,
            'send_par': 2,
            'rcv_par': 2,
            'decomp_par': 2,
            'write_par': 4,
            'compression_lib': 'lz4',
            'buffer_size': 512,
            'server_buffpool_size': 4 * 512 * 20,
            'client_buffpool_size': 4 * 512 * 20,
            'format': 1
        }
    },
    {
        'name': "etl",
        'config_type': {
            'read_par': 8,
            'deser_par': 6,
            'comp_par': 6,
            'send_par': 4,
            'rcv_par': 4,
            'decomp_par': 6,
            'write_par': 8,
            'compression_lib': 'zstd',
            'buffer_size': 256,
            'server_buffpool_size': 15000,
            'client_buffpool_size': 3 * 256 * 20,
            'format': 2
        }
    },
    {
        'name': "pg",
        'config_type': {
            'read_par': 4,
            'deser_par': 3,
            'comp_par': 3,
            'send_par': 2,
            'rcv_par': 2,
            'decomp_par': 4,
            'write_par': 1,
            'compression_lib': 'zstd',
            'buffer_size': 256,
            'server_buffpool_size': 2 * 256 * 20,
            'client_buffpool_size': 2 * 256 * 20,
            'format': 1
        }
    }
]

configurations = []

# Iterate through all combinations to create the configuration list
for config in fixed_configs:
    # for comp_lib in compression_types:
    #     for fmt in format_types:
    #         for bufpool_size in buffer_pool_sizes:
    #             for buff_size in buffer_sizes:
    # Create a descriptive name for the configuration
    run_name = (
        f"{config['name']}"
    )

    # Create the run_config dictionary
    run_config = {
        'read_par': config['config_type']['read_par'],
        'deser_par': config['config_type']['deser_par'],
        'write_par': config['config_type']['write_par'],
        'send_par': config['config_type']['send_par'],
        'rcv_par': config['config_type']['rcv_par'],
        'comp_par': config['config_type']['comp_par'],
        'decomp_par': config['config_type']['decomp_par'],
        'ser_par': config['config_type']['write_par'],
        'buffer_size': config['config_type']['buffer_size'],
        'server_buffpool_size': config['config_type']['server_buffpool_size'],
        'client_buffpool_size': config['config_type']['client_buffpool_size'],
        'format': config['config_type']['format'],
        'skip_ser': 0,
        'skip_deser': 0,
        'compression_lib': config['config_type']['compression_lib'],
    }

    # Add the new configuration to the list
    configurations.append({
        'name': run_name,
        'run_config': run_config
    })

# --- Script Execution ---

# Setup output file
csv_file_path = "res/figure20.csv"
if not os.path.exists("res"):
    os.makedirs("res")
if not os.path.exists("res/xdbc_plans"):
    os.makedirs("res/xdbc_plans")

sample_run_config = configurations[0]['run_config']
sample_cur_conf = create_conf(**sample_run_config) # Generate a sample to get its structure
run_config_keys = list(sample_cur_conf.keys())

# Create CSV with a header that matches the logged data
if not os.path.exists(csv_file_path):
    header_base = [
        "timestamp", "env_name", "config_name", "repetition"
    ]
    # Use keys from the ACTUAL data structure that will be written to the file
    run_config_keys = list(sample_cur_conf.keys())
    full_header = header_base + run_config_keys + ["time"] + ["data_size"] + ["est_throughput"]
    with open(csv_file_path, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(full_header)


total_iterations = len(environments) * (len(configurations)+2) * repetitions
global_iteration_counter = 0
print(f"--- Starting {total_iterations}  experiments. ---")

# Main experiment loop
for env_config in environments:
    env = env_config['env']
    env_name = env_config['name']
    print(f"--- Preparing environment: {env_config['name']} ---")
    set_env(env)

    # Generate historical data for optimization
    generate_historical_data(env,show_output=(True, True),all_compression_types = True) # Generate historical data for optimization and store in local_measurements_new
    
    print(f"--- Heuristics Optimize for environment: {env_name} ---")
    n, best_config, estimated_thr, opt_time = optimize(env, 'xdbc', 'heuristic', False, False)
    print(f"Optimizer found best configuration in {opt_time:.2f}s. Estimated throughput: {estimated_thr:.2f} MB/s")
    print(f"--- Running Heuristic configuration for {env_name} ---")
    for i in range(repetitions):
        print(f"Running Rep {i+1}/{repetitions}")
        
        perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
        
        # Use a deepcopy to avoid modifying the original config object
        t, data_size = run_xdbserver_and_xdbclient(copy.deepcopy(best_config), env, perf_dir, show_output=(True, True), return_size=True)
        print(f"Result: {t:.4f} seconds\n")
        
        # Log results for the optimal run
        timestamp = int(datetime.datetime.now().timestamp())
        with open(csv_file_path, mode="a", newline="") as file:
            writer = csv.writer(file)
            row_data = [
                timestamp, env_name, "xdbc", i + 1
            ]
            # row_data.extend(best_config.values())
            for key in run_config_keys:
                row_data.append(best_config[key])
            row_data.extend([t, data_size, estimated_thr])  # Add estimated throughput
            writer.writerow(row_data)
        
        with open(f"res/xdbc_plans/{timestamp}_optimal_{env_name}.json", "w") as file:
            json.dump(best_config, file, indent=4)
            


    print(f"--- Bruteforce Optimize for environment: {env_name} ---")
    n, best_config, estimated_thr, opt_time = optimize(env, 'xdbc', 'bruteforce', False, False)
    print(f"Optimizer found best configuration in {opt_time:.2f}s. Estimated throughput: {estimated_thr:.2f} MB/s")
    print(f"--- Running Bruteforce configuration for {env_name} ---")
    for i in range(repetitions):
        print(f"Running Rep {i+1}/{repetitions}")
        
        perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
        
        # Use a deepcopy to avoid modifying the original config object
        t, data_size = run_xdbserver_and_xdbclient(copy.deepcopy(best_config), env, perf_dir, show_output=(True, True), return_size=True)
        print(f"Result: {t:.4f} seconds\n")
        
        # Log results for the optimal run
        timestamp = int(datetime.datetime.now().timestamp())
        with open(csv_file_path, mode="a", newline="") as file:
            writer = csv.writer(file)
            row_data = [
                timestamp, env_name, "bf", i + 1
            ]
            # row_data.extend(best_config.values())
            for key in run_config_keys:
                row_data.append(best_config[key])
            row_data.extend([t, data_size, estimated_thr])  # Add estimated throughput
            writer.writerow(row_data)
        
        with open(f"res/xdbc_plans/{timestamp}_optimal_{env_name}.json", "w") as file:
            json.dump(best_config, file, indent=4)



    for transfer_config in configurations:
        # if not transfer_config['name'].startswith(env_config['name'].split('_Lineitem')[0].split('_ICU')[0]):
        #     global_iteration_counter += repetitions
        #     continue
        run_config = transfer_config['run_config']
        print(f"--- Starting experiments with the configuration : {transfer_config['name']} ---")

        for i in range(repetitions):
            print(f"Running: Rep {i+1}/{repetitions}")
            

            cur_conf = create_conf(**run_config)

            # Define perf directory for measurements
            perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
            
            # Run the client-server and get the execution time
            t, data_size = run_xdbserver_and_xdbclient(copy.deepcopy(cur_conf), env, perf_dir, show_output=(True, True),return_size=True)
            print(f"Result: {t:.4f} seconds\n")

            # Log the results
            timestamp = int(datetime.datetime.now().timestamp())

            with open(csv_file_path, mode="a", newline="") as file:
                writer = csv.writer(file)

                # Start with the base values
                row_data = [
                    timestamp,
                    env_config['name'],
                    transfer_config['name'],
                    i + 1
                ]
                row_data.extend(cur_conf.values())
                row_data.append(t)
                row_data.append(data_size)
                # estimated_thr = 0
                estimated_thr = calculate_config_throughput(env, cur_conf)
                print(f"Estimated throughput: {estimated_thr:.2f} MB/s")
                row_data.append(estimated_thr)
                writer.writerow(row_data)


            # Save the plan for this specific run
            with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
                json.dump(cur_conf, file, indent=4)

            global_iteration_counter += 1
            print(f"--- Overall Progress: {global_iteration_counter}/{total_iterations} ---")
            

print("--- All experiments completed. ---")
