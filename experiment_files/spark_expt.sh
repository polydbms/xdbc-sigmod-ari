#!/bin/bash

# Script: spark_expt.sh
# Description: Runs Spark JDBC and XDBC benchmarks with error handling and progress tracking

# Configuration
OUTPUT_CSV="res/figure9a.csv"
# TABLES=("ss13husallm")
TABLES=("lineitem_sf10" "ss13husallm" "iotm" "inputeventsm")
NUM_PARTITIONS=4
LOG_FILE="benchmark.log"
XDBC_CONFIGS=(
    # Format: buffer_size,bufferpool_size,rcv_par,decomp_par,write_par,iformat
    "1024,65536,1,6,8,1"  # Current working config
)

# Initialize files
# mkdir -p res
echo "table_name,system,config,elapsed_time_ms,timestamp" > $OUTPUT_CSV
echo "Benchmark started at $(date)" > $LOG_FILE

# Function to start XDBC server
start_xdbc_server() {
    local table_name=$1
    echo "[$(date)] Starting XDBC server for: $table_name" | tee -a $LOG_FILE
    
    # Stop any existing server
    docker exec xdbcserver bash -c "pkill -f './xdbc-server/build/xdbc-server' || true" 2>/dev/null
    sleep 2
    
    # Start new server
    docker exec -d xdbcserver bash -c "./xdbc-server/build/xdbc-server --system postgres -b 1024 -p 65536 --deser-parallelism 4 --read-parallelism 8 -f1"
    
    # Wait for server to start
    sleep 5
    echo "XDBC server started" | tee -a $LOG_FILE
}

# Function to stop XDBC server
stop_xdbc_server() {
    echo "[$(date)] Stopping XDBC server" | tee -a $LOG_FILE
    docker exec xdbcserver bash -c "pkill -f './xdbc-server/build/xdbc-server' || true" 2>/dev/null
    sleep 2
}

# Function to run JDBC benchmark
run_jdbc_benchmark() {
    local table_name=$1
    local attempt=1
    local max_attempts=1
    local elapsed_time=""
    
    echo "[$(date)] Starting JDBC benchmark for: $table_name" | tee -a $LOG_FILE
    
    while [ $attempt -le $max_attempts ]; do
        echo "Attempt $attempt of $max_attempts for $table_name (JDBC)" | tee -a $LOG_FILE
        
        # Run Spark JDBC job
        local output=$(docker exec -it xdbcspark /spark/bin/spark-submit \
            --class "example.ReadPGJDBC" \
            --master "local" \
            --packages com.typesafe.play:play-json_2.12:2.9.4 \
            --conf spark.eventLog.enabled=true \
            --num-executors 1 \
            --executor-cores 8 \
            --executor-memory 16G \
            --conf spark.memory.storageFraction=0.8 \
            --conf spark.driver.memory=16g \
            --conf spark.executor.extraJavaOptions="-XX:+UseG1GC" \
            /app/target/spark3io-1.0.jar \
            "$table_name" $NUM_PARTITIONS 2>&1)
        
        # Check if successful
        if echo "$output" | grep -q "Elapsed time JDBC"; then
            elapsed_time=$(echo "$output" | grep -o "Elapsed time.*[0-9]\+ms" | grep -o "[0-9]\+" | head -1)
            echo "$table_name,jdbc,default,$elapsed_time,$(date +%Y-%m-%d\ %H:%M:%S)" >> $OUTPUT_CSV
            echo "SUCCESS: $table_name (JDBC) completed in $elapsed_time ms" | tee -a $LOG_FILE
            return 0
        else
            echo "ERROR: Attempt $attempt failed for $table_name (JDBC)" | tee -a $LOG_FILE
            echo "$output" >> $LOG_FILE
            attempt=$((attempt + 1))
            sleep 5
        fi
    done
    
    echo "$table_name,jdbc,ERROR,$(date +%Y-%m-%d\ %H:%M:%S)" >> $OUTPUT_CSV
    echo "FAILED: All JDBC attempts failed for $table_name" | tee -a $LOG_FILE
    return 1
}

# Function to run XDBC benchmark
run_xdbc_benchmark() {
    local table_name=$1
    local config=$2
    local attempt=1
    local max_attempts=1
    local elapsed_time=""
    
    IFS=',' read -r buffer_size bufferpool_size rcv_par decomp_par write_par iformat <<< "$config"
    local config_name="xdbc_${buffer_size}_${bufferpool_size}_${rcv_par}_${decomp_par}_${write_par}_${iformat}"
    
    echo "[$(date)] Starting XDBC benchmark for: $table_name with config: $config_name" | tee -a $LOG_FILE
    
    while [ $attempt -le $max_attempts ]; do
        echo "Attempt $attempt of $max_attempts for $table_name (XDBC: $config_name)" | tee -a $LOG_FILE
        
        # Start XDBC server
        start_xdbc_server "$table_name"
        
        # Run Spark XDBC job
        local output=$(docker exec -it xdbcspark bash -c " \
            cd /app && \
            ./run_xdbc_spark.sh \
            tableName=$table_name \
            buffer_size=$buffer_size \
            bufferpool_size=$bufferpool_size \
            rcv_par=$rcv_par \
            decomp_par=$decomp_par \
            write_par=$write_par \
            iformat=$iformat \
            transfer_id=\$(date +%s) \
            server_host=xdbcserver 2>&1")
        
        # Stop XDBC server
        stop_xdbc_server
        
        # Check if successful
        if echo "$output" | grep -q "Elapsed time XDBC"; then
            elapsed_time=$(echo "$output" | grep -o "Elapsed time.*[0-9]\+ms" | grep -o "[0-9]\+" | head -1)
            echo "$table_name,xdbc,$config_name,$elapsed_time,$(date +%Y-%m-%d\ %H:%M:%S)" >> $OUTPUT_CSV
            echo "SUCCESS: $table_name (XDBC: $config_name) completed in $elapsed_time ms" | tee -a $LOG_FILE
            return 0
        else
            echo "ERROR: Attempt $attempt failed for $table_name (XDBC: $config_name)" | tee -a $LOG_FILE
            echo "$output" >> $LOG_FILE
            attempt=$((attempt + 1))
            sleep 5
        fi
    done
    
    echo "$table_name,xdbc,ERROR,$(date +%Y-%m-%d\ %H:%M:%S)" >> $OUTPUT_CSV
    echo "FAILED: All XDBC attempts failed for $table_name with config: $config_name" | tee -a $LOG_FILE
    return 1
}

# Main execution
echo "Starting benchmark for ${#TABLES[@]} tables..."
echo "Tables: ${TABLES[*]}"
echo ""

# Ensure XDBC server is stopped initially
stop_xdbc_server

for table in "${TABLES[@]}"; do
    # Run JDBC benchmark
    run_jdbc_benchmark "$table"
    echo "----------------------------------------"
    
    # Run XDBC benchmarks for each configuration
    for config in "${XDBC_CONFIGS[@]}"; do
        run_xdbc_benchmark "$table" "$config"
        echo "----------------------------------------"
    done
done

echo ""
echo "Benchmark completed!"
echo "Results: $OUTPUT_CSV"
echo "Log: $LOG_FILE"
echo ""

# Display summary
echo "=== SUMMARY ==="
cat $OUTPUT_CSV