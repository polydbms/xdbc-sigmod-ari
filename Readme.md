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

Figure in paper  |      py file         |  Status
---------------------------------------------------
Figure 8         |                      |  Not Done
Figure 9         |                      |  Not Done
Figure 10        |                      |  Not Done
Figure 11        |                      |  Not Done
Figure 12        |                      |  Not Done
Figure 13        | figureACSVCSV.py     |  Completed part a)
Figure 14        |                      |  Not Done
Figure 15        |                      |  Not Done
Figure 16        |                      |  Not Done
Figure 17        |                      |  Not Done
Figure 18        |                      |  Not Done
Figure 19        |                      |  Not Done
Figure 20        |                      |  Not Done
Figure 21        |                      |  Not Done
Figure 22        |                      |  Not Done
