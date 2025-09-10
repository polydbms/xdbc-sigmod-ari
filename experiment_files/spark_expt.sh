#!/bin/bash

# Script: run_jdbc_benchmark_advanced.sh
# Description: Runs Spark JDBC benchmark with error handling and progress tracking

# Configuration
OUTPUT_CSV="res/figure9a.csv"
# TABLES=("ss13husallm")
TABLES=("lineitem_sf10" "ss13husallm" "iotm" "inputeventsm")
NUM_PARTITIONS=4
LOG_FILE="benchmark.log"

# Initialize files
echo "table_name,system,elapsed_time_ms,timestamp" > $OUTPUT_CSV
echo "JDBC Benchmark started at $(date)" > $LOG_FILE

# Function to run benchmark with error handling
run_benchmark() {
    local table_name=$1
    local attempt=1
    local max_attempts=1
    local elapsed_time=""
    
    echo "[$(date)] Starting benchmark for: $table_name" | tee -a $LOG_FILE
    
    while [ $attempt -le $max_attempts ]; do
        echo "Attempt $attempt of $max_attempts for $table_name" | tee -a $LOG_FILE
        
        # Run Spark job
        local output=$(docker exec -it xdbcspark /spark/bin/spark-submit \
            --class "example.ReadPGJDBC" \
            --master "local" \
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
            echo "$table_name,jdbc,$elapsed_time,$(date +%Y-%m-%d\ %H:%M:%S)" >> $OUTPUT_CSV
            echo "SUCCESS: $table_name completed in $elapsed_time ms" | tee -a $LOG_FILE
            return 0
        else
            echo "ERROR: Attempt $attempt failed for $table_name" | tee -a $LOG_FILE
            echo "$output" >> $LOG_FILE
            attempt=$((attempt + 1))
            sleep 5
        fi
    done
    
    echo "$table_name,ERROR,$(date +%Y-%m-%d\ %H:%M:%S)" >> $OUTPUT_CSV
    echo "FAILED: All attempts failed for $table_name" | tee -a $LOG_FILE
    return 1
}

# Main execution
echo "Starting JDBC benchmark for ${#TABLES[@]} tables..."
echo "Tables: ${TABLES[*]}"
echo ""

for table in "${TABLES[@]}"; do
    run_benchmark "$table"
    echo "----------------------------------------"
done

echo ""
echo "Benchmark completed!"
echo "Results: $OUTPUT_CSV"
echo "Log: $LOG_FILE"
echo ""

# Display summary
echo "=== SUMMARY ==="
cat $OUTPUT_CSV