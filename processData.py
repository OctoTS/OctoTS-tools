import sys
import os
import csv
import argparse
from datetime import datetime, timezone

def process_data(input_stream, min_loc=None, max_loc=None, sort_by=None):
    processed = []
    current_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # Read raw data from the first script via Pipe
    reader = csv.DictReader(input_stream)
    for row in reader:
        try:
            author = row['author']
            loc = int(row['lines_of_code'])
            
            # Filtering by flags
            if min_loc is not None and loc < min_loc:
                continue
            if max_loc is not None and loc > max_loc:
                continue
                
            processed.append({
                'timestamp': current_time,
                'author': author,
                'lines_of_code': loc
            })
        except (KeyError, ValueError):
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

    # 'a' mode means Append (Persistence for Time Series)
    with open(output_file, 'a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerows(data)

def main():
    parser = argparse.ArgumentParser(description="OctoTS Data Processor")
    parser.add_argument("--output", help="Final CSV database file", required=True)
    parser.add_argument("--min", type=int, help="Minimum LOC to keep")
    parser.add_argument("--max", type=int, help="Maximum LOC to keep")
    parser.add_argument("--sort", choices=['loc', 'author'], help="Sort data by field")
    
    args = parser.parse_args()

    if sys.stdin.isatty():
        print("Error: No data detected in Pipe. Use: python3 script1.py | python3 script2.py")
        return

    # Process and Save
    processed_records = process_data(sys.stdin, args.min, args.max, args.sort)
    if processed_records:
        save_to_csv(processed_records, args.output)
        print(f"Success: Added {len(processed_records)} records to {args.output}")
    else:
        print("Done: No records matched the filters.")

if __name__ == "__main__":
    main()
