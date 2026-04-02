import sys
import os
import csv
import argparse
from datetime import datetime, timezone

def process_data(input_stream, min_loc=None, max_loc=None, sort_by=None):
    processed = []
    current_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    reader = csv.DictReader(input_stream)
    for row in reader:
        try:
            author = row['author']
            loc = int(row['lines_of_code'])
            
            # Threshold filtering
            if min_loc is not None and loc < min_loc:
                continue
            if max_loc is not None and loc > max_loc:
                continue
                
            processed.append({
                'timestamp': current_time,
                'author': author,
                'lines_of_code': loc
            })
        except (KeyError, ValueError, TypeError):
            continue

    # Sorting logic
    if sort_by == 'loc':
        processed.sort(key=lambda x: x['lines_of_code'], reverse=True)
    elif sort_by == 'author':
        processed.sort(key=lambda x: x['author'])
        
    return processed

def save_to_csv(data, output_file):
    if not data:
        return
    
    file_exists = os.path.isfile(output_file)
    headers = ['timestamp', 'author', 'lines_of_code']

    # Open in append mode 'a' to build time-series history
    with open(output_file, 'a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerows(data)

def main():
    parser = argparse.ArgumentParser(description="OctoTS Data Processor")
    # Optional input file flag. If not provided, script uses stdin (pipe).
    parser.add_argument("--input", help="Source CSV file path")
    parser.add_argument("--output", help="Final database CSV file", required=True)
    parser.add_argument("--min", type=int, help="Filter: minimum lines of code")
    parser.add_argument("--max", type=int, help="Filter: maximum lines of code")
    parser.add_argument("--sort", choices=['loc', 'author'], help="Sort records by field")
    
    args = parser.parse_args()

    # Data Source Selection Logic
    try:
        if args.input:
            # Read from a specific file
            if not os.path.exists(args.input):
                print(f"Error: File {args.input} not found.")
                sys.exit(1)
            with open(args.input, 'r', encoding='utf-8') as f:
                records = process_data(f, args.min, args.max, args.sort)
        else:
            # Read from Pipe (stdin)
            if sys.stdin.isatty():
                print("Error: No input data. Provide --input or use a pipe (|).")
                sys.exit(1)
            records = process_data(sys.stdin, args.min, args.max, args.sort)

        if records:
            save_to_csv(records, args.output)
            print(f"Success: Processed {len(records)} records into {args.output}")
        else:
            print("Done: No records matched the filtering criteria.")

    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
