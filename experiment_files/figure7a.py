import subprocess
import datetime
import os
import csv
import json
from experiment_envs import test_envs
from optimizer.runner import run_xdbserver_and_xdbclient
from optimizer.optimize import optimize
from experiment_helpers import set_env, create_file
from profiling_phase import generate_historical_data

repetitions = 2
test_env = next((env for env in test_envs if env['name'] == "figurePandasPG"), None)
baselines = ['connectorx', 'duckdb', 'modin', 'turbodbc']
# baselines = ['connectorx']
# prepare environment
env = test_env['env']

set_env(env)

show_server_output = True
show_client_output = True

show_stdout_server = None if show_server_output else subprocess.DEVNULL
show_stdout_client = None if show_client_output else subprocess.DEVNULL

subprocess.Popen(["docker", "exec", "-it", env['server_container'], "bash", "-c",
                  f"cd /dev/shm && python3 -m RangeHTTPServer 1234"],
                 stdout=show_stdout_server)


print(test_env)

csv_file_path = "res/figure8a.csv"
create_file(csv_file_path)
# run baselines

for table in test_env['env']['tables']:
    for baseline in baselines:
        for i in range(repetitions):
            a = datetime.datetime.now()

            try:
                subprocess.run(["docker", "exec", "-it", env['client_container'], "bash", "-c",
                                f"""python3.9 /workspace/tests/pandas_baselines.py \
                                    --parallelism 8 \
                                    --table "{table}" \
                                    --chunksize 42 \
                                    --library "{baseline}"
                                    """], check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                print("Error running the baseline script.")
                print(f"Return code: {e.returncode}")
                print(f"Stdout: {e.stdout}")
                print(f"Stderr: {e.stderr}") # This will contain the error message from pandas_baselines.py
                raise # Re-raise the exception if you still want the script to stop

            b = datetime.datetime.now()
            c = (b - a).total_seconds()
            print(f"{baseline} for {table} : {c} s")

            with open(csv_file_path, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([int(a.timestamp()), test_env['name'], i + 1, baseline, table, c])

subprocess.run(["docker", "exec", "-it", env['server_container'], "pkill", "-f", "RangeHTTPServer"], check=True)

# run xdbc
generate_historical_data(env,show_output = (show_server_output,show_client_output)) # Generate historical data for optimization and store in local_measurements_new
perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
for table in test_env['env']['tables']:
    for i in range(repetitions):
        env['table'] = table

        n, best_config, estimated_thr, opt_time = optimize(env, 'xdbc', 'heuristic', False, 0)
        t = run_xdbserver_and_xdbclient(best_config, env, perf_dir)

        print(f"xdbc for {table}: {t} s")
        timestamp = int(datetime.datetime.now().timestamp())

        with open(csv_file_path, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, test_env['name'], i + 1, "xdbc", table, t])

        with open(f"res/xdbc_plans/{timestamp}.json", "w") as file:
            json.dump(best_config, file, indent=4)
