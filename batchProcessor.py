import sys
import os
import pandas as pd
from datetime import datetime, timezone

def load_input(source):
    """Autodetect format based on extension or stdin."""
    if source == 'stdin':
        return pd.read_csv(sys.stdin)
    
    if not os.path.exists(source):
        raise FileNotFoundError(f"Source file not found: {source}")
        
    ext = os.path.splitext(source)[1].lower()
    if ext == '.json': return pd.read_json(source)
    elif ext in ['.xls', '.xlsx']: return pd.read_excel(source)
    elif ext == '.parquet': return pd.read_parquet(source)
    elif ext in ['.pkl', '.pickle']: return pd.read_pickle(source)
    else: return pd.read_csv(source)

def save_output(df, path, storage_type):
    """Handles appending to different formats."""
    file_exists = os.path.isfile(path)
    
    # CASE: CSV - fast append
    if storage_type == 'csv':
        df.to_csv(path, mode='a', index=False, header=not file_exists)
        return

    # CASE: Binary formats - Read-Modify-Write
    if file_exists:
        existing_df = load_input(path)
        combined = pd.concat([existing_df, df], ignore_index=True)
    else:
        combined = df

    if storage_type == 'json': combined.to_json(path, orient='records', indent=4)
    elif storage_type == 'parquet': combined.to_parquet(path, index=False)
    elif storage_type in ['excel', 'xlsx']: combined.to_excel(path, index=False)
    elif storage_type == 'pickle': combined.to_pickle(path)

def main():
    if len(sys.argv) < 5:
        print("Usage: python3 batchProcessor.py append <output_type> <input_path|stdin> <output_path>")
        print("Example: python3 batchProcessor.py append parquet new_data.csv database.parquet")
        sys.exit(1)

    command = sys.argv[1].lower()
    storage_type = sys.argv[2].lower() # This defines the output format engine
    in_path = sys.argv[3]
    out_path = sys.argv[4]

    if command != 'append':
        print(f"Error: Unknown command '{command}'. Currently only 'append' is supported.")
        sys.exit(1)

    try:
        # Load data
        new_df = load_input(in_path)
        
        # Add ISO 8601 Timestamp at the FIRST position
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        if 'timestamp' not in new_df.columns:
            new_df.insert(0, 'timestamp', timestamp)
        else:
            new_df['timestamp'] = timestamp
            cols = ['timestamp'] + [c for c in new_df.columns if c != 'timestamp']
            new_df = new_df[cols]
        
        # Persist using the specified output_type engine
        save_output(new_df, out_path, storage_type)
        print(f"SUCCESS: {len(new_df)} records appended to {storage_type.upper()} storage.")

    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()