build:
	docker-compose -f docker-compose.yml up -d --build
restart:
	docker-compose -f docker-compose.yml restart xdbcexpt
clean:
	docker-compose -f docker-compose.yml down --remove-orphans
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
		docker cp "xdbcexpt:$$pdf" ~/XDBC/xdbc-sigmod-ari/experiments_new/res/pdf_plots/$$filename; \
		echo "Copied: $$filename"; \
	done
	@echo "All PDFs copied successfully!"
run_plot: run_plotter copy-pdfs


run_experiments_and_plot: run_expts run_plot
