Instructions to carry out experiments and generate results:

step1: make build                #   setup the docker container
step2: make run_figureACSVCSV    #   execute figureACSVCSV.py to run the experiment. Similarly execute other py files as well.
step3: make run_plot             #   execute e2e_plotter.py to generate the plots in a pdf file.

To do:
Execute .py files in experiments_new to run all the experiments. 
Copy the scripts from e2e.ipynb to e2_plotter.py to plot the results of all the experiments.

Reminder: 
Copy the databases to dev/shm before starting the experiments

Notes: 
a) Refer Makefile for actual commands executed
b) Results of experiments are available in res folder
pdf_plots: contain plots in pdf
xdbc_plans: contain json files
c) Refer experiment_envs.py for the configurations used in the experiment
d) If the experiment get stuck at thread = 16. This mightbe due to the unavailability of sufficent buffers in the pool. Cntl+C skips that run.

Current status:

Figure in paper   |      Py file                    | Status
----------------- | --------------------------------| ----------------------
Figure 7          |figure8a.py                      | Couldnt run pandas_xdbc due to import error pyxdbc,pyxdbcparquet
                  |figure8b.py                      | Couldnt run pandas_xdbc due to import error pyxdbc,pyxdbcparquet
Figure 8          |figure8a.py                      | Couldnt run pandas_xdbc due to import error pyxdbc,pyxdbcparquet
Figure 9          | xxxx                            | Not Done
Figure 10         |figureZParquetCSV.py             | Completed. (check duckdb bar)
Figure 11         | a. figure11.py                  | Completed
                    b. custom/manual run            | Completed
Figure 12         | a. figureACSVCSV.py             | Completed 
                  | b. figureBCSVPG.py              | Completed
Figure 13         | a. figureACSVCSVOpt.py          | Completed 
                  | b. figureBCSVPGOpt.py           | Completed 
Figure 14         | a. figureXArrow.py              | Completed
                  | b. figureYParquet.py            | Couldnt run pandas_xdbc due to import error pyxdbc,pyxdbcparquet
Figure 15         |                                 | Not Done
Figure 16         |                                 | Not Done
Figure 17         | a. figureMemoryManagement.py    | Completed 
Figure 18         |                                 | Not Done
Figure 19         | figure19.py                     | Completed
Figure 20         |                                 | Not Done
Figure 21         |                                 | Not Done
Figure 22         |                                 | Not Done
