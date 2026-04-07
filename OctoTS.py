import cmd
import os
import pandas as pd

class OctoTS(cmd.Cmd):
    intro = 'Welcome to the OctoTS CLI. Type "help" or "?" to list commands.\n'
    prompt = '(OctoTS) '
    
    def __init__(self):
        super().__init__()
        self.dataFile = None

    def do_import(self, filepath):
        """
        Import a text file containing time-series data. 
        Auto-detects JSON or CSV formats.
        Usage: import <filepath>
        """
        if not filepath:
            print("Error: Please provide a file path. Example: import data.csv")
            return
        
        filepath = filepath.strip("\"'")
        
        if not os.path.exists(filepath):
            print(f"Error: File '{filepath}' not found.")
            return

        print(f"Attempting to load and auto-detect format for '{filepath}'...")
        
        try:
            try:
                self.dataFile = pd.read_json(filepath)
                print("Success: Detected and loaded as JSON.")
            except ValueError:
                self.dataFile = pd.read_csv(filepath, sep=None, engine='python')
                print("Success: Detected and loaded as CSV.")
                
        except Exception as e:
            print(f"Failed to load file. Ensure it is a valid file. Error: {e}")
            self.dataFile = None

    def do_columns(self, arg):
        """
        Show the tags/columns and types available in the loaded dataset.
        Usage: columns
        """
        if self.dataFile is None:
            print("Error: No data loaded. Please 'import <filepath>' first.")
            return
        
        print("\nAvailable Columns/Tags:")
        print("-" * 23)
        for col in self.dataFile.columns:
            dtype = self.dataFile[col].dtype
            print(f" * {col} (Type: {dtype})")
        print("-" * 23)
        print(f"Total columns: {len(self.dataFile.columns)}\n")

    def do_timecol(self, arg):
        """
        Convert a specific column to mathematical datetime objects.
        Do this before sorting by time to ensure accurate chronological sorting.
        Usage: timecol <column_name>
        Example: timecol my_timestamp_col
        """
        if self.dataFile is None:
            print("Error: No data loaded. Please 'import <filepath>' first.")
            return
            
        col_name = arg.strip()
        if not col_name:
            print("Error: Please provide a column name. Usage: timecol <column_name>")
            return
            
        if col_name not in self.dataFile.columns:
            print(f"Error: Column '{col_name}' not found. Use 'columns' to see available columns.")
            return
            
        try:
            print(f"Converting '{col_name}' to datetime objects...")
            # pd.to_datetime automatically parses ISO 8601 and other standard formats
            self.dataFile[col_name] = pd.to_datetime(self.dataFile[col_name])
            print(f"Success: '{col_name}' is now a datetime type. You can verify with 'columns'.")
        except Exception as e:
            print(f"Error converting column to datetime. Are you sure it contains valid dates? Error: {e}")

    def do_sort(self, arg):
        """
        Sort the dataset by a specific column.
        Usage: sort <column_name> [asc|desc]
        Example: sort timestamp desc
        """
        if self.dataFile is None:
            print("Error: No data loaded. Please 'import <filepath>' first.")
            return
            
        args = arg.split()
        if not args:
            print("Error: Please provide a column name. Usage: sort <column_name> [asc|desc]")
            return
            
        ascending = True
        if len(args) > 1 and args[-1].lower() in ['asc', 'desc']:
            order = args.pop().lower()
            ascending = (order == 'asc')
            
        col_name = " ".join(args)
        
        if col_name not in self.dataFile.columns:
            print(f"Error: Column '{col_name}' not found. Use 'columns' to see available columns.")
            return
            
        self.dataFile.sort_values(by=col_name, ascending=ascending, inplace=True)
        order_str = 'ascending' if ascending else 'descending'
        print(f"Success: Data sorted by '{col_name}' in {order_str} order.")

    def do_save(self, filepath):
        """
        Save the current dataset to a file (CSV or JSON).
        Usage: save <filepath>
        Example: save output.json
        """
        if self.dataFile is None:
            print("Error: No data loaded. Nothing to save.")
            return
            
        if not filepath:
            print("Error: Please provide a filename. Example: save output.csv")
            return
            
        filepath = filepath.strip("\"'")
        
        try:
            if filepath.lower().endswith('.json'):
                self.dataFile.to_json(filepath, orient='records', date_format='iso')
                print(f"Success: Data saved to JSON at '{filepath}'")
            else:
                self.dataFile.to_csv(filepath, index=False)
                print(f"Success: Data saved to CSV at '{filepath}'")
        except Exception as e:
            print(f"Error saving file: {e}")

    def do_exit(self, arg):
        """
        Exit the interactive shell.
        Usage: exit (or quit)
        """
        print("Closing the time series shell. Goodbye!")
        return True

    do_quit = do_exit
    do_EOF = do_exit

if __name__ == '__main__':
    try:
        OctoTS().cmdloop()
    except KeyboardInterrupt:
        print("\nExiting...")