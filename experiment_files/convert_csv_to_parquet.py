import duckdb
import argparse
import json
import os
import math

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Convert CSV to split Parquet files.")
parser.add_argument("--csv_file", required=True, help="Path to the input CSV file.")
parser.add_argument("--schema_file", required=True, help="Path to the JSON schema file.")
parser.add_argument("--output_dir", required=True, help="Directory to save Parquet files.")
parser.add_argument("--max_size_mb", type=int, default=64, help="Maximum size of each Parquet file in MB.")
parser.add_argument("--delimiter", default=",", help="The delimiter character in the CSV file.")
parser.add_argument("--quote", default='"', help="The quote character in the CSV file.")
args = parser.parse_args()

# --- Load Schema ---
with open(args.schema_file, 'r') as f:
    schema = json.load(f)

# --- Prepare for Processing ---
output_dir = args.output_dir
os.makedirs(output_dir, exist_ok=True)
base_name = os.path.splitext(os.path.basename(args.csv_file))[0]

conn = duckdb.connect()
try:
    # --- HYBRID APPROACH: Try auto-parsing first, then fall back to manual parsing ---

    print("--- Attempting automatic CSV parsing (Method 1) ---")
    
    # DuckDB type mapping
    db_type_mapping = {
        'INT': 'INTEGER',
        'DOUBLE': 'DOUBLE',
        'CHAR': 'VARCHAR',
        'STRING': 'VARCHAR',
        'DATE': 'DATE'
    }

    columns_dict = {
        col['name']: db_type_mapping.get(col['type'].upper(), 'VARCHAR')
        for col in schema
    }
    columns_sql_str = "{" + ", ".join([f"'{k}': '{v}'" for k, v in columns_dict.items()]) + "}"

    # Use read_csv_auto to parse the file with specified options.
    create_table_query = f"""
        CREATE OR REPLACE TABLE temp_table AS 
        SELECT * FROM read_csv_auto(
            '{args.csv_file}',
            header=false,
            delim='{args.delimiter}',
            quote='{args.quote}',
            columns={columns_sql_str},
            sample_size=-1,
            strict_mode=false
        );
    """
    conn.execute(create_table_query)
    print("Automatic CSV parsing successful.")

except duckdb.Error as e:
    print(f"\nAutomatic CSV parsing failed: {e}")
    print("--- Falling back to manual line-by-line parsing (Method 2) ---")
    
    # This method is more robust for files with inconsistent quoting or delimiters
    # especially in the final column, but may fail if delimiters exist in middle columns
    # without proper quoting.

    # 1. Load raw lines
    print("Step 1: Loading raw CSV lines...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw_lines(line VARCHAR);
        COPY raw_lines FROM '{args.csv_file}' (DELIMITER E'\x02', QUOTE E'\x01', HEADER false);
    """)
    print("Raw lines loaded.")

    # 2. Build the manual parsing query
    print("Step 2: Building the manual parsing query...")
    select_expressions = []
    num_columns = len(schema)

    for i, col_def in enumerate(schema):
        col_name = col_def['name']
        part_index = i + 1
        db_type = db_type_mapping.get(col_def['type'].upper(), 'VARCHAR')

        if part_index < num_columns:
            # For all columns except the last one, trim whitespace and cast.
            expression = f"trim(parts[{part_index}])::{db_type} AS {col_name}"
        else:
            # For the last column, join all remaining parts back together.
            # This correctly reconstructs the final field, even if it contains the delimiter.
            expression = f"array_to_string(array_slice(parts, {part_index}, array_length(parts)), '{args.delimiter}')::{db_type} AS {col_name}"
        select_expressions.append(expression)

    select_clause = ",\n        ".join(select_expressions)

    # 3. Create the final table using the generated query.
    parsing_query = f"""
        CREATE OR REPLACE TABLE temp_table AS
        WITH split_parts AS (
            SELECT string_split(line, '{args.delimiter}') AS parts
            FROM raw_lines
        )
        SELECT
            {select_clause}
        FROM split_parts;
    """
    print("Step 3: Executing manual parsing query...")
    conn.execute(parsing_query)
    print("Manual line-by-line parsing successful.")


# --- This section runs regardless of which parsing method was used ---
try:
    # --- Calculate rows per split ---
    row_count_result = conn.execute("SELECT COUNT(*) FROM temp_table").fetchone()
    if row_count_result is None:
        raise ValueError("Could not determine row count from the table. The table might be empty.")
    row_count = row_count_result[0]
    
    # Estimate bytes per row from schema 'size' property for splitting logic
    bytes_per_row = sum(col.get('size', 8) for col in schema) # Use default size if not present
    if bytes_per_row == 0:
        print("Warning: Schema contains no 'size' information. Estimating row size for splitting.")
        table_size_bytes = os.path.getsize(args.csv_file)
        bytes_per_row = table_size_bytes / row_count if row_count > 0 else 256

    if bytes_per_row > 0:
        rows_per_split = math.floor((args.max_size_mb * 1024 * 1024) / bytes_per_row)
    else:
        rows_per_split = row_count

    if rows_per_split == 0:
        rows_per_split = row_count

    # --- Split and write Parquet files ---
    print(f"\nTotal rows: {row_count}. Writing splits of up to {rows_per_split} rows each.")
    part_number = 0
    for start in range(0, row_count, rows_per_split):
        end = min(start + rows_per_split, row_count)
        parquet_file = os.path.join(output_dir, f"{base_name}_part{part_number}.parquet")
        
        print(f"Writing rows {start} to {end-1} to {parquet_file}...")
        query = f"""
            COPY (
                SELECT * FROM temp_table
                LIMIT {rows_per_split} OFFSET {start}
            ) TO '{parquet_file}' (FORMAT PARQUET);
        """
        conn.execute(query)
        print(f"Written: {parquet_file}")
        part_number += 1

    print("\nSplitting completed.")

except duckdb.Error as e:
    print(f"A DuckDB error occurred during processing or writing: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    conn.close()

