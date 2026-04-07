import cmd
import os
import pandas as pd

class OctoTS(cmd.Cmd):
    intro = 'Welcome to the OctoTS CLI. Type "help" or "?" to list commands.\n'
    prompt = '(OctoTS) '
    
    def __init__(self):
        super().__init__()
        self.df = None

    def do_import(self, filepath):
        """
        Import a text file containing time-series data. 
        Auto-detects formats.
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
                self.df = pd.read_json(filepath)
                print("Success: Detected and loaded as JSON.")
            except ValueError:
                self.df = pd.read_csv(filepath, sep=None, engine='python')
                print("Success: Detected and loaded as CSV.")
                
        except Exception as e:
            print(f"Failed to load file. Ensure it is valid file. Error: {e}")
            self.df = None

    def do_columns(self, arg):
        """
        Show the tags/columns and types available in the loaded dataset.
        Usage: columns
        """
        if self.df is None:
            print("Error: No data loaded. Please 'import <filepath>' first.")
            return
        
        print("\nAvailable Columns/Tags:")
        print("-" * 23)
        for col in self.df.columns:
            dtype = self.df[col].dtype
            print(f" * {col} (Type: {dtype})")
        print("-" * 23)
        print(f"Total columns: {len(self.df.columns)}\n")

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