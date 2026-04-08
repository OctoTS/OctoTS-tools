import cmd
import os
import pandas as pd

class OctoTS(cmd.Cmd):
    intro = 'Welcome to the OctoTS CLI. Type "help" or "?" to list commands.\n'
    prompt = '(OctoTS) '
    
    def __init__(self):
        super().__init__()
        self.dataFile = None

    def _auto_detect_timecol(self):
        """
        Internal method to scan columns for date-like strings and prompt the user.
        """
        if self.dataFile is None or self.dataFile.empty:
            return
            
        print("\nScanning for timestamp columns...")
        found_potential = False
        
        for col in self.dataFile.columns:
            if pd.api.types.is_datetime64_any_dtype(self.dataFile[col]):
                print(f" -> Success: '{col}' is already recognized as a datetime format.")
                found_potential = True
                continue
                
            if self.dataFile[col].dtype == 'object':
                sample_series = self.dataFile[col].dropna()
                if sample_series.empty:
                    continue
                    
                sample_val = str(sample_series.iloc[0])
                
                if any(char in sample_val for char in ['-', '/', ':']):
                    try:
                        pd.to_datetime(sample_val)
                        found_potential = True
                        
                        ans = input(f" -> Detected potential timestamp in column '{col}' (e.g., {sample_val}). Convert it? [Y/n]: ").strip().lower()
                        
                        if ans in ['', 'y', 'yes']:
                            print(f"    Converting '{col}' to datetime...")
                            self.dataFile[col] = pd.to_datetime(self.dataFile[col])
                            print("    Success.")
                            return 
                        else:
                            print(f"    Skipped '{col}'.")
                            
                    except (ValueError, TypeError, pd.errors.ParserError):
                        pass 
        
        if not found_potential:
            print(" -> Result: No obvious timestamp columns were detected automatically.")
            print(" -> ACTION REQUIRED: Please use the 'columns' command to check your data,")
            print(" -> then use 'timecol <column_name>' to manually set your time column.")
            
        print("Scan complete.\n")

    def do_import(self, filepath):
        """
        Import a file containing time-series data.
        Supported formats: CSV, JSON, Excel (.xlsx/.xls), Parquet, Pickle.
        Usage: import <filepath>
        """
        if not filepath:
            print("Error: Please provide a file path. Example: import data.csv")
            return
        
        filepath = filepath.strip("\"'")
        
        if not os.path.exists(filepath):
            print(f"Error: File '{filepath}' not found.")
            return

        print(f"Attempting to load '{filepath}'...")
        ext = os.path.splitext(filepath)[1].lower()
        
        try:
            if ext == '.json':
                self.dataFile = pd.read_json(filepath)
                print("Success: Loaded as JSON.")
            elif ext in ['.xls', '.xlsx']:
                self.dataFile = pd.read_excel(filepath)
                print(f"Success: Loaded as Excel ({ext}).")
            elif ext == '.parquet':
                self.dataFile = pd.read_parquet(filepath)
                print("Success: Loaded as Parquet.")
            elif ext in ['.pkl', '.pickle']:
                self.dataFile = pd.read_pickle(filepath)
                print("Success: Loaded as Pickle.")
            else:
                try:
                    self.dataFile = pd.read_csv(filepath, sep=None, engine='python')
                    print(f"Success: Detected and loaded as delimited text/CSV.")
                except ValueError:
                    self.dataFile = pd.read_json(filepath)
                    print("Success: Detected and loaded as JSON.")
            
            self._auto_detect_timecol()
                
        except ImportError as ie:
            print(f"Missing dependency for this file type: {ie}")
            self.dataFile = None
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
        Manually convert a specific column to mathematical datetime objects.
        Usage: timecol <column_name>
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

    def do_trim(self, arg):
        """
        Clean or reduce the dataset. 
        Usage: 
          trim missing                           - Removes any rows containing missing (NaN/Null) values.
          trim spaces                            - Removes leading/trailing whitespace from string columns.
          trim <number>                          - Keeps only the top <number> of rows (e.g., 'trim 100').
          trim date <col> <before|after> <date>  - REMOVES rows before or after an ISO timestamp.
          trim date <col> between <d1> <d2>      - REMOVES rows between two ISO timestamps.
                                                   Example: trim date timestamp between 2026-04-06T00:00:00Z 2026-04-06T01:19:19Z
        """
        if self.dataFile is None:
            print("Error: No data loaded. Please 'import <filepath>' first.")
            return
            
        args = arg.strip().split()
        if not args:
            print("Error: Please specify what to trim. Options: 'missing', 'spaces', a number, or 'date'.")
            return
            
        initial_rows = len(self.dataFile)
        cmd_type = args[0].lower()
            
        if cmd_type == 'missing':
            self.dataFile.dropna(inplace=True)
            new_rows = len(self.dataFile)
            print(f"Success: Removed {initial_rows - new_rows} rows with missing values.")
            
        elif cmd_type == 'spaces':
            str_cols = self.dataFile.select_dtypes(include=['object', 'str']).columns
            if len(str_cols) == 0:
                print("Notice: No text columns found to trim spaces from.")
                return
                
            for col in str_cols:
                self.dataFile[col] = self.dataFile[col].apply(
                    lambda x: x.strip() if isinstance(x, str) else x
                )
            print(f"Success: Trimmed leading and trailing spaces from {len(str_cols)} text columns.")
            
        elif cmd_type.isdigit():
            n = int(cmd_type)
            self.dataFile = self.dataFile.head(n)
            new_rows = len(self.dataFile)
            print(f"Success: Trimmed dataset to the top {new_rows} rows. (Removed {initial_rows - new_rows} rows).")
            
        elif cmd_type == 'date':
            if len(args) < 4:
                print("Error: Missing arguments for date trimming.")
                print("Usage: trim date <column_name> <before|after|between> <ISO_timestamp(s)>")
                return
                
            col_name = args[1]
            operator = args[2].lower()
            
            if col_name not in self.dataFile.columns:
                print(f"Error: Column '{col_name}' not found. Use 'columns' to see available columns.")
                return
                
            if not pd.api.types.is_datetime64_any_dtype(self.dataFile[col_name]):
                print(f"Error: Column '{col_name}' is not recognized as a datetime type.")
                print(f"Please run 'timecol {col_name}' to convert it before trimming by date.")
                return
                
            if operator not in ['before', 'after', 'between']:
                print("Error: Operator must be 'before', 'after', or 'between'.")
                return
                
            if operator == 'between' and len(args) < 5:
                print("Error: Missing the second date. Usage: trim date <col> between <date1> <date2>")
                return

            try:
                date_str1 = args[3]
                target_date1 = pd.to_datetime(date_str1)
                
                if operator == 'after':
                    self.dataFile = self.dataFile[self.dataFile[col_name] <= target_date1]
                    action_str = f"removed data after {date_str1}"
                    
                elif operator == 'before':
                    self.dataFile = self.dataFile[self.dataFile[col_name] >= target_date1]
                    action_str = f"removed data before {date_str1}"
                    
                elif operator == 'between':
                    date_str2 = args[4]
                    target_date2 = pd.to_datetime(date_str2)
                    self.dataFile = self.dataFile[
                        (self.dataFile[col_name] < target_date1) | 
                        (self.dataFile[col_name] > target_date2)
                    ]
                    action_str = f"removed data between {date_str1} and {date_str2}"
                    
                new_rows = len(self.dataFile)
                print(f"Success: {action_str}. Kept {new_rows} rows. (Removed {initial_rows - new_rows} rows).")
                
            except Exception as e:
                print(f"Error parsing dates. Please ensure they are in ISO format (e.g., 2026-04-06T01:19:19Z). Error: {e}")

        else:
            print("Error: Unknown trim argument. Use 'help trim' to see available options.")

    def do_save(self, filepath):
        """
        Save the current dataset to a file, strictly formatting dates to ISO 8601.
        Supported formats: CSV, JSON, Excel (.xlsx), Parquet, Pickle.
        Usage: save <filepath>
        Example: save output.csv
        """
        if self.dataFile is None:
            print("Error: No data loaded. Nothing to save.")
            return
            
        if not filepath:
            print("Error: Please provide a filename. Example: save output.csv")
            return
            
        filepath = filepath.strip("\"'")
        ext = os.path.splitext(filepath)[1].lower()
        
        df_to_save = self.dataFile.copy()
        
        for col in df_to_save.columns:
            if pd.api.types.is_datetime64_any_dtype(df_to_save[col]):
                df_to_save[col] = df_to_save[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        try:
            if ext == '.json':
                df_to_save.to_json(filepath, orient='records')
                print(f"Success: Data saved to JSON at '{filepath}'")
            elif ext in ['.xls', '.xlsx']:
                df_to_save.to_excel(filepath, index=False)
                print(f"Success: Data saved to Excel at '{filepath}'")
            elif ext == '.parquet':
                df_to_save.to_parquet(filepath, index=False)
                print(f"Success: Data saved to Parquet at '{filepath}'")
            elif ext in ['.pkl', '.pickle']:
                df_to_save.to_pickle(filepath)
                print(f"Success: Data saved to Pickle at '{filepath}'")
            else:
                df_to_save.to_csv(filepath, index=False)
                print(f"Success: Data saved to CSV at '{filepath}'")
        except ImportError as ie:
            print(f"Error: Missing a library required to save this format: {ie}")
        except Exception as e:
            print(f"Error saving file: {e}")

    def do_exit(self, arg):
        """
        Exit the interactive shell.
        Usage: exit (or quit)
        """
        print("Closing the OctoTS shell. Goodbye!")
        return True

    do_quit = do_exit
    do_EOF = do_exit

if __name__ == '__main__':
    try:
        OctoTS().cmdloop()
    except KeyboardInterrupt:
        print("\nExiting...")