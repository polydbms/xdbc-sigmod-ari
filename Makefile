build:
	docker compose -f docker-compose.yml up -d --build
restart:
	docker compose -f docker-compose.yml restart xdbcexpt
clean:
	docker compose -f docker-compose.yml down --remove-orphans
open_docker:
	docker exec -it xdbcexpt bash
clean_csvs:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files/res && rm -f *.csv"
clean_pdfs:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files/res/pdf_plots && rm -f *.pdf"
run_figure12aCSVCSV:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure12aCSVCSV.py"
run_figure6:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure6.py --max-configurations 23"
run_figure6b:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure6b.py --max-configurations 12"
run_figure11:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure11.py"
run_figure1516a:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure1516a.py"
run_figure1516b:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure1516b.py"
run_figure17b:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure17b.py"
run_figure18:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure18.py"
run_figure19:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure19.py"
run_figure20:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure20.py"
run_figure7a:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure7a.py"
run_figure7b:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure7b.py"
run_figure9a:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && ./spark_expt.sh"
run_figure9b:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && ./run_experiments_for_datasets.sh 1"
# 	@echo "Running Figure 9b experiments..."
# 	cd ../pg_xdbc_fdw/experiments && ./run_experiments_for_datasets.sh 1 && cd ../../xdbc-sigmod-ari
# 	@echo "Figure 9b experiments completed."
# 	@echo "Copying averages.csv from host to docker container..."
# 	docker cp ../pg_xdbc_fdw/averages.csv xdbcexpt:/app/experiment_files/res/
# 	@echo "averages.csv copied successfully to docker container."
run_figure13aCSVCSVOpt:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure13aCSVCSVOpt.py"
run_figure13bCSVPGOpt:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure13bCSVPGOpt.py"
run_figure12bCSVPG:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure12bCSVPG.py"
run_figureCloudFogCSV:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figureCloudFogCSV.py"
run_figureCloudFogPG:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figureCloudFogPG.py"
run_figure17aMemoryManagement:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure17aMemoryManagement.py"
run_figure8PandasPGCPUNet:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure8PandasPGCPUNet.py"
run_figure14aXArrow:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure14aXArrow.py"
run_figure14bYParquet:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure14bYParquet.py"
run_figure10ZParquetCSV:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 figure10ZParquetCSV.py"
run_prepare_experiments:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 prepare_experiments.py"
run_testPostgresCSV:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && PYTHONPATH=.. python3 testPostgresCSV.py"

prepare_postgres:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && ./prepare_postgres.sh"

prepare_parquet:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && ./prepare_parquet.sh"

prepare_tbl:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && ./prepare_tbl.sh"

run_expts:
	run_figureACSVCSV run_figure11 run_figure8a run_figure8b run_figureACSVCSVOpt run_figureBCSVPGOpt run_figureBCSVPG run_figureCloudFogCSV run_figureCloudFogPG run_figureMemoryManagement run_figurePandasPGCPUNet run_figureXArrow run_figureYParquet run_figureZParquetCSV
	
run_plotter:
	docker exec -it xdbcexpt bash -c "cd /app/experiment_files && python3 e2e_plotter.py"

copy-pdfs:
	@echo "Copying all PDFs from container to pdf_plots..."
	@for pdf in $$(docker exec xdbcexpt find /app/experiment_files -name "*.pdf"); do \
		filename=$$(basename "$$pdf"); \
		if [ -d "$$HOME/XDBC/xdbc-sigmod-ari/experiment_files/res/pdf_plots" ]; then \
			docker cp "xdbcexpt:$$pdf" "$$HOME/XDBC/xdbc-sigmod-ari/experiment_files/res/pdf_plots/$$filename"; \
		elif [ -d "$$HOME/xdbc-sigmod-ari/experiment_files/res/pdf_plots" ]; then \
			docker cp "xdbcexpt:$$pdf" "$$HOME/xdbc-sigmod-ari/experiment_files/res/pdf_plots/$$filename"; \
		else \
			echo "Error: Could not find pdf_plots directory in either $$HOME/XDBC/xdbc-sigmod-ari/ or $$HOME/xdbc-sigmod-ari/"; \
			exit 1; \
		fi; \
	done
	@echo "All PDFs copied successfully!"
copy-csvs:
	@echo "Copying all CSVs from container..."
	@echo "Copying res CSVs..."
	@for csv in $$(docker exec xdbcexpt find /app/experiment_files/res -name "*.csv"); do \
		filename=$$(basename "$$csv"); \
		if [ -d "$$HOME/XDBC/xdbc-sigmod-ari/experiment_files/res" ]; then \
			docker cp "xdbcexpt:$$csv" "$$HOME/XDBC/xdbc-sigmod-ari/experiment_files/res/$$filename"; \
		elif [ -d "$$HOME/xdbc-sigmod-ari/experiment_files/res" ]; then \
			docker cp "xdbcexpt:$$csv" "$$HOME/xdbc-sigmod-ari/experiment_files/res/$$filename"; \
		else \
			echo "Error: Could not find res directory in either $$HOME/XDBC/xdbc-sigmod-ari/ or $$HOME/xdbc-sigmod-ari/"; \
			exit 1; \
		fi; \
	done
	@echo "Copying local_measurements_new CSVs..."
	@for csv in $$(docker exec xdbcexpt find /app/experiment_files/local_measurements_new -name "*.csv"); do \
		filename=$$(basename "$$csv"); \
		if [ -d "$$HOME/XDBC/xdbc-sigmod-ari/experiment_files/local_measurements_new" ]; then \
			docker cp "xdbcexpt:$$csv" "$$HOME/XDBC/xdbc-sigmod-ari/experiment_files/local_measurements_new/$$filename"; \
		elif [ -d "$$HOME/xdbc-sigmod-ari/experiment_files/local_measurements_new" ]; then \
			docker cp "xdbcexpt:$$csv" "$$HOME/xdbc-sigmod-ari/experiment_files/local_measurements_new/$$filename"; \
		else \
			echo "Error: Could not find local_measurements_new directory in either $$HOME/XDBC/xdbc-sigmod-ari/ or $$HOME/xdbc-sigmod-ari/"; \
			exit 1; \
		fi; \
		echo "Copied local_measurements CSV: $$filename"; \
	done
	@echo "All CSVs copied successfully to their respective directories!"
	
run_plot: run_plotter copy-pdfs copy-csvs

sync: 
	rsync -avz --progress ~/XDBC/xdbc-sigmod-ari/ node21:~/xdbc-sigmod-ari/

import_pdfs: 
	scp node21:~/xdbc-sigmod-ari/experiment_files/res/pdf_plots/*.pdf ~/XDBC/xdbc-sigmod-ari/experiment_files/res/pdf_plots/

import_csvs: 
	scp node21:~/xdbc-sigmod-ari/experiment_files/res/*.csv ~/XDBC/xdbc-sigmod-ari/experiment_files/res/

import:
	import_pdfs import_csvs

run_experiments_and_plot: run_expts run_plot
