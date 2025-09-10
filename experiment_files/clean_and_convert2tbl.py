# clean_and_convert2tbl.py
import csv
import sys

if len(sys.argv) != 3:
    print("Usage: python clean_and_convert.py <input_csv> <output_tbl>")
    sys.exit(1)

input_file_path = sys.argv[1]
output_file_path = sys.argv[2]

print(f"Cleaning '{input_file_path}' and converting to pipe-delimited '{output_file_path}'")

try:
    with open(input_file_path, 'r', newline='') as infile, \
         open(output_file_path, 'w', newline='') as outfile:
        
        # Reader for the messy, comma-delimited source file
        reader = csv.reader(infile, delimiter=',')
        
        # Writer for the clean, pipe-delimited destination file
        writer = csv.writer(outfile, delimiter='|', quoting=csv.QUOTE_MINIMAL)

        for row in reader:
            # This handles the dbgen artifact where a trailing comma creates an extra empty field.
            # The csv reader correctly handles commas inside quoted fields.
            if len(row) > 16:
                writer.writerow(row[:16])
            else:
                writer.writerow(row)

    print("Conversion successful.")

except FileNotFoundError:
    print(f"Error: Input file not found at {input_file_path}")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    sys.exit(1)