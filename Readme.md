Instructions to carry out experiments and generate results:

step1: make build                #   setup the docker container
step2: make run_figureACSVCSV    #   execute figureACSVCSV.py to run the experiment. Similarly execute other py files as well.
step3: make run_plot             #   execute e2e_plotter.py to generate the plots in a pdf file.

To do:
Execute .py files in experiments_new to run all the experiments. 
Copy the scripts from e2e.ipynb to e2_plotter.py to plot the results of all the experiments.

Reminder: 
Copy the csv databases to dev/shm before starting the experiments

Notes: 
a) Refer Makefile for actual commands executed
b) Results of experiments are available in res folder
pdf_plots: contain plots in pdf
xdbc_plans: contain json files
c) Refer experiment_envs.py for the configurations used in the experimentrun.

Current status:

Figure in paper   |      Py file                    | Status
----------------- | --------------------------------| ----------------------
Figure 6          |figure7.py                       | Completed
                  |figure7b.py                      | Completed
Figure 7          |figure8a.py                      | xdbcpy is stuck even though server runs fine
                  |figure8b.py                      | xdbcpy is stuck even though server runs fine
Figure 8          |figure8a.py                      | xdbcpy is stuck even though server runs fine
Figure 9          |figurePandasPGCPUNet.py          | xdbcpy is stuck even though server runs fine
Figure 10         |figureZParquetCSV.py             | Completed. (check duckdb bar)
Figure 11         | a. figure11.py                  | Completed
                    b. custom/manual run            | Completed
Figure 12         | a. figureACSVCSV.py             | Completed 
                  | b. figureBCSVPG.py              | Completed
Figure 13         | a. figureACSVCSVOpt.py          | Completed 
                  | b. figureBCSVPGOpt.py           | Completed 
Figure 14         | a. figureXArrow.py              | Completed
                  | b. figureYParquet.py            | xdbcpy is stuck even though server runs fine
Figure 15         | a. figure1516a.py               | Completed*
                    b. figure1516b.py               | Completed*
Figure 16         | a. figure1516a.py               | Completed*
                    b. figure1516b.py               | Completed*
Figure 17         | a. figureMemoryManagement.py    | Completed 
                  | b. figure17b.py                 | Completed 
Figure 18         | figure18.py                     | In progress
Figure 19         | figure19.py                     | Completed
Figure 20         | xxxx                            | Need expert config
Figure 21         | xxxx                            | Need expert config
