import itertools
import os
from optimizer.runner import run_xdbserver_and_xdbclient

def generate_historical_data(env, show_output=(False, False)):
    """
    Generates historical performance data by running a series of data transfers
    with varying configurations. This populates the CSV files needed by the
    optimizer.
    """
    # # Define the specific environment for which to generate data.

    # Define the directory to store the performance measurement CSVs.
    perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements_new'))
    print(f"Outputting performance data to: {perf_dir}")

    # --- Configuration Ranges to Test ---
    # Define a range of parallelism settings to iterate through.
    # Includes '1' to ensure baseline data is generated.
    parallelism_options = [1]

    # Define different buffer sizes to test.
    buffer_size_options = [256, 1024, 4096, 16384]
    
    # Define the compression libraries to generate data for
    compression_options = ['nocomp', 'zstd', 'lz4', 'lzo', 'snappy']

    # Define different buffer pool sizes. These are often linked to buffer_size
    # and parallelism, so we'll calculate them dynamically.

    # Define valid combinations for skip_ser and skip_deser
    skip_options = [(0, 0), (1, 1)]

    # --- Calculate and Print Total Number of Runs ---
    num_tables = len(env.get('tables', []))
    num_buffer_sizes = len(buffer_size_options)
    # There are 8 parallelism parameters in the itertools.product call
    num_par_combos = len(parallelism_options) ** 8
    num_skip_options = len(skip_options)
    num_comp_options = len(compression_options)
    max_total_runs = num_tables * num_buffer_sizes * num_par_combos * num_skip_options * num_comp_options
    
    print(f"\n📈 Planning to generate historical data...")
    print(f"Maximum possible runs: {max_total_runs}")

    # --- Iteration and Execution ---
    run_counter = 1

    # Loop over each table defined in the environment.
    for table_name in env['tables']:
        # Set the 'table' key for the current iteration.
        env['table'] = table_name
        print(f"\n===== Generating data for table: {table_name} =====")

        # Iterate over all combinations of buffer sizes and parallelism settings.
        for buffer_size in buffer_size_options:
            # Create all possible combinations of parallelism settings for each component.
            # This creates a Cartesian product of all parallelism options for each stage.
            par_combinations = itertools.product(
                parallelism_options,  # read_par
                parallelism_options,  # deser_par
                parallelism_options,  # comp_par
                parallelism_options,  # send_par
                parallelism_options,  # rcv_par
                parallelism_options,  # decomp_par
                parallelism_options,  # ser_par
                parallelism_options   # write_par
            )

            for combo in par_combinations:
                # Unpack the combination into named variables for clarity.
                (read_par, deser_par, comp_par, send_par, 
                 rcv_par, decomp_par, ser_par, write_par) = combo

                # Basic sanity check: Total client parallelism should not exceed client CPU cores.
                # This helps prune unrealistic configurations.
                total_client_par = rcv_par + decomp_par + ser_par + write_par
                if total_client_par > env['client_cpu']:
                    continue 

                # Basic sanity check: Total server parallelism should not exceed server CPU cores.
                total_server_par = read_par + deser_par + comp_par + send_par
                if total_server_par > env['server_cpu']:
                    continue

                for skip_ser, skip_deser in skip_options:
                    # Add the new loop for compression libraries
                    for compression_lib in compression_options:
                        print(f"\n--- Starting Run {run_counter}/{max_total_runs} for table {table_name}   ---")
                        
                        # Dynamically calculate buffer pool sizes.
                        # A common heuristic is to have enough buffers for each parallel stage.
                        server_buffpool_size = buffer_size * (read_par + deser_par + comp_par + send_par) * 2
                        client_buffpool_size = buffer_size * (rcv_par + decomp_par + ser_par + write_par) * 2

                        # Construct the configuration dictionary for the current run.
                        config = {
                            'buffer_size': buffer_size,
                            'server_buffpool_size': server_buffpool_size,
                            'client_buffpool_size': client_buffpool_size,
                            'read_par': read_par,
                            'deser_par': deser_par,
                            'comp_par': comp_par,
                            'send_par': send_par,
                            'rcv_par': rcv_par,
                            'decomp_par': decomp_par,
                            'ser_par': ser_par,
                            'write_par': write_par,
                            'compression_lib': compression_lib,
                            'format': 1,
                            'skip_ser': skip_ser,
                            'skip_deser': skip_deser,
                        }

                        # Execute the data transfer with the current configuration.
                        # The run_xdbserver_and_xdbclient function will handle writing the results
                        # to the corresponding CSV files.
                        run_xdbserver_and_xdbclient(
                            config=config,
                            env=env,
                            perf_dir=perf_dir,
                            sleep=2,  # Give the server time to start
                            show_output=show_output # Suppress stdout from client/server
                        )
                        
                        run_counter += 1

    print("\n--- Historical data generation complete. ---")

if __name__ == '__main__':
    env_demo = {
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
        'tables': ['lineitem_sf10', 'ss13husallm', 'iotm', 'inputeventsm']
    }
    # To run this script, you would execute:
    # python your_script_name.py
    # Make sure you have the 'optimizer' package and its dependencies available.
    generate_historical_data(env_demo)