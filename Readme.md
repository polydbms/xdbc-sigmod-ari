Instructions to carry out experiments and generate results:
**********************************************************
step1: ./run_experiments.sh     

----------------------------------------------------------

To do:
Need to execute 4 more commands successfully:
1) make run_figure8a
2) make run_figure8b
3) make run_figureYParquet
4) make run_figurePandasPGCPUNet

Reminder: 
Copy the csv databases to dev/shm before starting the experiments

Notes: 
a) Refer Makefile for actual commands executed
b) Results of experiments are available in experiments_new/res folder
pdf_plots: contain plots in pdf
xdbc_plans: contain json files
c) Refer experiment_envs.py for the configurations used in the experimentrun.

Current status:

Figure in paper   |      Py file                    | Status
----------------- | --------------------------------| ----------------------
Figure 6          |figure7.py                       | Verified with less samples
                  |figure7b.py                      | Verified with less samples
Figure 7          |figure8a.py                      | Completed*
                  |figure8b.py                      | Completed
Figure 8          |figurePandasPGCPUNet.py          | Completed*
Figure 9          |spark. manual run                | Verified
Figure 10         |figureZParquetCSV.py             | Verified. (check duckdb bar)
Figure 11         | a. figure11.py                  | Completed
                    b. custom/manual run            | Completed
Figure 12         | a. figureACSVCSV.py             | Verified 
                  | b. figureBCSVPG.py              | Verified
Figure 13         | a. figureACSVCSVOpt.py          | Verified 
                  | b. figureBCSVPGOpt.py           | Verified* 
Figure 14         | a. figureXArrow.py              | Completed
                  | b. figureYParquet.py            | Completed*
Figure 15         | a. figure1516a.py               | Completed
                    b. figure1516b.py               | Completed
Figure 16         | a. figure1516a.py               | Completed
                    b. figure1516b.py               | Completed
Figure 17         | a. figureMemoryManagement.py    | Completed 
                  | b. figure17b.py                 | Completed 
Figure 18         | figure18.py                     | Completed (a,b,c,d,e,f)
Figure 19         | figure19.py                     | Completed
Figure 20         | figure20.py                     | Completed*
Figure 21         | figure20.py                     | Need fix in plot
