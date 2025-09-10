# XDBC

- [XDBC](https://dl.acm.org/doi/10.1145/3725294) is a holistic, high-performance framework for fast and scalable data transfers across heterogeneous data systems (e.g. DBMS to dataframes) aiming to combine the generality of generic solutions with performance of specialized connectors
- It decomposes data transfer into a configurable pipeline (read -> deserialize -> compress -> send/receive -> decompress -> serialize -> write) with pipeline-parallel execution and ring-buffer memory manager for low resource overhead.
- The core of the framework (xdbc-client and xdbc-server) are written in C++ with bindings available for Python and Spark. It includes built-in adapters to connect to PostgreSQL, CSV, Parquet and Pandas.
- The project includes a lightweight heuristic optimizer implemented in Python that automatically tunes the parallelism, buffer sizes, intermediate formats and compression algorithms to the current environment.


## Project Structure

XDBC consists of multiple repositories covering the cross-system functionality. For the reproducibility experiments the following repositories will be cloned and used :

- [`xdbc-client`](https://github.com/polydbms/xdbc-client) Client-side module, for loading data into the target system.
- [`xdbc-server`](https://github.com/polydbms/xdbc-server) Server-side module, for extracting the data from the source system.
- [`xdbc-python`](https://github.com/polydbms/xdbc-python) Python bindings for loading data into Pandas (through pybind).
- [`xdbc-spark`](https://github.com/polydbms/xdbc-spark) Spark bindings, for loading data into a Spark RDD (through a custom DataSource with JNI).
- [`pg_xdbc_fdw`](https://github.com/polydbms/pg_xdbc_fdw) PostgreSQL Foreign Data Wrapper, for loading data into a table.


## Requirements

The experiments were validated on **Ubuntu 22.04 LTS**, and should work on **Ubuntu 22.04 or later** (and generally on recent Linux distributions).  
On Ubuntu, the following packages are required:

- `bash`
- `docker-ce` (tested with version 28.3.3)
- `docker-compose-plugin` (tested with version v2.39.1)
- `git`
- `make`

You can install them with:

```bash
sudo apt-get update
sudo apt-get install -y bash docker-ce docker-compose-plugin git make
```

## How to reproduce

To reproduce our experiments, one needs to execute the run_experiments.sh script located in the root project directory. The script contains 7 steps, of which each can be manually disabled between lines 20-26. 

1. The script clones all necessary repositories.
2. The XDBC infrastructure is set up via docker.
3. The datasets are downloaded, extracted and copied into memory.
4. The container for handling the experiments is set up.
5. The data for the transfer experiments is prepared.
6. The actual experiments will be executed. If specific experiments should be re-run, comment-out the specific commands between lines 135-154.
7. The plots are created and copied in a pdf file to xdbc-sigmod-ari/experiment_files/res/pdf_plots/ .


- The specific commands executed to run the experiments are located in the Makefile.
- The specific code the execute the experiments is located in ./experiment_files .
- The code to create the plots is located in ./experiment_files/e2e_plotter.py .
- The code for the heuristic optimizer used in the experiments is located under ./optimizer


## System specifications

We executed our data transfer experiments on compute nodes with the following specifications:

   | Component         | Details                                                                          |
   |-------------------|----------------------------------------------------------------------------------|
   | CPU               | Dual-socket Intel Xeon Silver 4216 @ 2.10 GHz with 16 cores and 32 threads each  |
   | Memory            | 512 GB of main memory and a 2 TB SSD disk drive                                  |
   | OS                | Ubuntu v20.04.2 LTS                                                              |

