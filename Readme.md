Instructions to carry out experiments and generate results:
**********************************************************
./run_experiments.sh     

---------------------------------------------------------


Notes: 
a) Refer Makefile for actual commands executed
b) Results of experiments are available in experiments_new/res folder
pdf_plots: contain plots in pdf
xdbc_plans: contain json files
c) Refer experiment_envs.py for the configurations used in the experimentrun.





# XDBC

- XDBC is a holistic, high-performance framework for fast and scalable data transfers across heterogeneous data systems (e.g. DBMS to dataframes) aiming to combine the generality of generic solutions with performance of specialized connectors
- It decomposes data transfer into a configurable pipeline (read -> deserialize -> compress -> send/receive -> decompress -> serialize -> write) with pipeline-parallel execution and ring-buffer memory manager for low ressource overhead.
- The core of the framework (xdbc-client and xdbc-server) are written in C++ with bindings available for Python and Spark. It includes built in adapters to connect to PostgreSQL, MySQL, Clickhouse, CSV, Parquet and Pandas.
- The project includes a lightweights heuristic optimizer implemented in Python that automatically tunes the parallelism, buffer sizes, intermediate formats and copmression algorithms to the current environment.


## Project Structure

XDBC consists of multiple repositories covering the cross-system functionality. For the reproducibility experiments the following repositories will be cloned and used :

- `xdbc-client` Client-side of the application initiating data transfers.
- `xdbc-server` Server-side providing the requested datasets to different data systems.
- `xdbc-python` Python bindings for XDBC with pandas integration.
- `xdbc-spark` Apache Spark integration for XDBC.
- `pg_xdbc_fdw` Foreign Data Wrapper for PostgreSQL to enable interaction with XDBC.


## System specifications

We executed our data transfer experiments on compute nodes with the following specifications:

   | Component         | Details                                                                          |
   |-------------------|----------------------------------------------------------------------------------|
   | CPU               | Dual-socket Intel Xeon Silver 4216 @ 2.10 GHz with 16 cores and 32 Threads each  |
   | Memory            | 512 GB of main memory and a 2 TB SSD disk drive                                  |
   | OS                | Ubuntu v20.04.2 LTS                                                              |


## How to reproduce

To reproduce our experiments, one needs to execute the run_experiments.sh script located in the root project directory. The sript contains 7 steps, of which each can be manually disabled between lines 20 - 26. 

1. The script clones all neccessary repositories.
2. The XDBC infrastruture is set up via docker.
3. The datasets are downloaded, extracted and copied into memory.
4. The container for handling the experiments is set up.
5. The data for the transfer experiments is prepared.
6. The actual experiments will be executed. If specific experiments should be re-run, comment-out the specific commands between lines 135-154.
7. The plots are created and copied in a pdf file to xdbc-sigmod-ari/experiments_new/res/pdf_plots/ .


- The specific commands executed to run the epxeriments are located in the Makefile.
- The specific code the execute the experiments is located in ./experiments_new .
- The code to create the plots is located in ./experiments_new/e2e_plotter.py .
- The code for the heuristic optimizer used in the experiments is located under ./optimizer
