build:
	docker compose -f docker-compose.yml up -d --build
restart:
	docker compose -f docker-compose.yml restart xdbcexpt
clean:
	docker compose -f docker-compose.yml down --remove-orphans
open_docker:
	docker exec -it xdbcexpt bash
run_figureACSVCSV:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figureACSVCSV.py"
run_figure11:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figure11.py"
run_figure8a:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figure8a.py"
run_figure8b:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figure8b.py"
run_figureACSVCSVOpt:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figureACSVCSVOpt.py"
run_figureBCSVPGOpt:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figureBCSVPGOpt.py"
run_figureBCSVPG:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figureBCSVPG.py"
run_figureCloudFogCSV:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figureCloudFogCSV.py"
run_figureCloudFogPG:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figureCloudFogPG.py"
run_figureMemoryManagement:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figureMemoryManagement.py"
run_figurePandasPGCPUNet:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figurePandasPGCPUNet.py"
run_figureXArrow:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figureXArrow.py"
run_figureYParquet:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figureYParquet.py"
run_figureZParquetCSV:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 figureZParquetCSV.py"
run_prepare_experiments:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 prepare_experiments.py"
run_testPostgresCSV:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && PYTHONPATH=.. python3 testPostgresCSV.py"

run_expts:
	run_figureACSVCSV run_figure11 run_figure8a run_figure8b run_figureACSVCSVOpt run_figureBCSVPGOpt run_figureBCSVPG run_figureCloudFogCSV run_figureCloudFogPG run_figureMemoryManagement run_figurePandasPGCPUNet run_figureXArrow run_figureYParquet run_figureZParquetCSV
	
run_plotter:
	docker exec -it xdbcexpt bash -c "cd /app/experiments_new && python3 e2e_plotter.py"

copy-pdfs:
	@echo "Copying all PDFs from container to pdf_plots..."
	@for pdf in $$(docker exec xdbcexpt find /app/experiments_new -name "*.pdf"); do \
		filename=$$(basename "$$pdf"); \
		if [ -d "$$HOME/XDBC/xdbc-sigmod-ari/experiments_new/res/pdf_plots" ]; then \
			docker cp "xdbcexpt:$$pdf" "$$HOME/XDBC/xdbc-sigmod-ari/experiments_new/res/pdf_plots/$$filename"; \
		elif [ -d "$$HOME/xdbc-sigmod-ari/experiments_new/res/pdf_plots" ]; then \
			docker cp "xdbcexpt:$$pdf" "$$HOME/xdbc-sigmod-ari/experiments_new/res/pdf_plots/$$filename"; \
		else \
			echo "Error: Could not find pdf_plots directory in either $$HOME/XDBC/xdbc-sigmod-ari/ or $$HOME/xdbc-sigmod-ari/"; \
			exit 1; \
		fi; \
		echo "Copied: $$filename"; \
	done
	@echo "All PDFs copied successfully!"
copy-csvs:
	@echo "Copying all CSVs from container..."
	@echo "Copying res CSVs..."
	@for csv in $$(docker exec xdbcexpt find /app/experiments_new/res -name "*.csv"); do \
		filename=$$(basename "$$csv"); \
		if [ -d "$$HOME/XDBC/xdbc-sigmod-ari/experiments_new/res" ]; then \
			docker cp "xdbcexpt:$$csv" "$$HOME/XDBC/xdbc-sigmod-ari/experiments_new/res/$$filename"; \
		elif [ -d "$$HOME/xdbc-sigmod-ari/experiments_new/res" ]; then \
			docker cp "xdbcexpt:$$csv" "$$HOME/xdbc-sigmod-ari/experiments_new/res/$$filename"; \
		else \
			echo "Error: Could not find res directory in either $$HOME/XDBC/xdbc-sigmod-ari/ or $$HOME/xdbc-sigmod-ari/"; \
			exit 1; \
		fi; \
		echo "Copied res CSV: $$filename"; \
	done
	@echo "Copying local_measurements_new CSVs..."
	@for csv in $$(docker exec xdbcexpt find /app/experiments_new/local_measurements_new -name "*.csv"); do \
		filename=$$(basename "$$csv"); \
		if [ -d "$$HOME/XDBC/xdbc-sigmod-ari/experiments_new/local_measurements_new" ]; then \
			docker cp "xdbcexpt:$$csv" "$$HOME/XDBC/xdbc-sigmod-ari/experiments_new/local_measurements_new/$$filename"; \
		elif [ -d "$$HOME/xdbc-sigmod-ari/experiments_new/local_measurements_new" ]; then \
			docker cp "xdbcexpt:$$csv" "$$HOME/xdbc-sigmod-ari/experiments_new/local_measurements_new/$$filename"; \
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
	scp node21:~/xdbc-sigmod-ari/experiments_new/res/pdf_plots/*.pdf ~/XDBC/xdbc-sigmod-ari/experiments_new/res/pdf_plots/

import_csvs: 
	scp node21:~/xdbc-sigmod-ari/experiments_new/res/*.csv ~/XDBC/xdbc-sigmod-ari/experiments_new/res/

import:
	import_pdfs import_csvs

run_experiments_and_plot: run_expts run_plot
