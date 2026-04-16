import cmd
import os
import pandas as pd
import io 
import re
from urllib.parse import urlparse


class OctoTS(cmd.Cmd):
    intro = 'Welcome to the OctoTS CLI. Type "help" or "?" to list commands.\n'
    prompt = '(OctoTS) '
    
    def __init__(self):
        super().__init__()
        self.dataFile = None
        self.history = [] 
        self.custom_roles = {}

    def _save_history(self):
        """
        Saves a copy of the current dataset to the history stack before modifying it.
        Keeps the last 5 states to save memory.
        """
        if self.dataFile is not None:
            self.history.append(self.dataFile.copy())
            if len(self.history) > 5:
                self.history.pop(0)

    def _auto_detect_timecol(self):
        """
        Internal method to scan columns for date-like strings.
        Auto-converts unambiguous formats (YYYY-MM-DD or obvious column names).
        Prompts for other potential, ambiguous date formats.
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
                
            if self.dataFile[col].dtype == 'object' or self.dataFile[col].dtype == 'string':
                sample_series = self.dataFile[col].dropna().head(5)
                if sample_series.empty:
                    continue
                    
                sample_val = str(sample_series.iloc[0]).strip()
                
                has_date_chars = any(char in sample_val for char in ['-', '/', ':'])
                if not has_date_chars:
                    continue
                
                try:
                    pd.to_datetime(sample_series)
                    found_potential = True
                    
                    is_unambiguous_format = bool(re.match(r'^\d{4}-\d{2}-\d{2}', sample_val)) or ('T' in sample_val)
                    
                    col_lower = str(col).lower()
                    is_obvious_name = col_lower in ['timestamp', 'time', 'date', 'datetime', 'tstamp']
                    
                    if is_unambiguous_format or is_obvious_name:
                        self._save_history()
                        print(f" -> Auto-detected safe timestamp in '{col}'. Converting automatically...")
                        self.dataFile[col] = pd.to_datetime(self.dataFile[col])
                        print("    Success.")
                        return 
                    else:
                        ans = input(f" -> Detected potential timestamp in '{col}' (e.g., {sample_val}). Convert it? [Y/n]: ").strip().lower()
                        
                        if ans in ['', 'y', 'yes']:
                            self._save_history() 
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
            print(" -> ACTION REQUIRED: Please use 'show columns' to check your data,")
            print(" -> then use 'timecol <column_name>' to manually set your time column.")
            
        print("Scan complete.\n")

    # -------------------------------------------------------------------------
    # Internal helpers for lesser-known formats
    # -------------------------------------------------------------------------

    def _read_jsonl(self, filepath):
        """Read JSON Lines / NDJSON format (one JSON object per line)."""
        import json
        records = []
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"  Warning: Skipping malformed line {line_num}: {e}")
        return pd.DataFrame(records)

    def _write_jsonl(self, df, filepath):
        """Write DataFrame to JSON Lines / NDJSON format."""
        df.to_json(filepath, orient='records', lines=True)

    def _read_orc(self, filepath):
        """Read Apache ORC format."""
        try:
            import pyarrow.orc as orc
            table = orc.read_table(filepath)
            return table.to_pandas()
        except ImportError:
            raise ImportError("pyarrow is required for ORC support. Run: pip install pyarrow")

    def _write_orc(self, df, filepath):
        """Write DataFrame to Apache ORC format."""
        try:
            import pyarrow as pa
            import pyarrow.orc as orc
            table = pa.Table.from_pandas(df)
            orc.write_table(table, filepath)
        except ImportError:
            raise ImportError("pyarrow is required for ORC export. Run: pip install pyarrow")

    def _read_netcdf(self, filepath):
        """Read NetCDF format (popular in meteorology, oceanography, climatology)."""
        try:
            import xarray as xr
            ds = xr.open_dataset(filepath)
            return ds.to_dataframe().reset_index()
        except ImportError:
            raise ImportError("xarray and netCDF4 are required for NetCDF support. Run: pip install xarray netCDF4")

    def _write_netcdf(self, df, filepath):
        """Write DataFrame to NetCDF format."""
        try:
            import xarray as xr
            ds = xr.Dataset.from_dataframe(df)
            ds.to_netcdf(filepath)
        except ImportError:
            raise ImportError("xarray and netCDF4 are required for NetCDF export. Run: pip install xarray netCDF4")

    def _read_msgpack(self, filepath):
        """Read MessagePack binary format."""
        try:
            import msgpack
        except ImportError:
            raise ImportError("msgpack is required for MessagePack support. Run: pip install msgpack")
        with open(filepath, 'rb') as f:
            data = msgpack.unpack(f, raw=False)
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        else:
            raise ValueError("MessagePack file must contain a list of records or a single record dict.")

    def _write_msgpack(self, df, filepath):
        """Write DataFrame to MessagePack binary format."""
        try:
            import msgpack
        except ImportError:
            raise ImportError("msgpack is required for MessagePack export. Run: pip install msgpack")
        records = df.to_dict(orient='records')
        import datetime
        def _coerce(v):
            if isinstance(v, (pd.Timestamp, datetime.datetime, datetime.date)):
                return str(v)
            return v
        records = [{k: _coerce(v) for k, v in row.items()} for row in records]
        with open(filepath, 'wb') as f:
            msgpack.pack(records, f, use_bin_type=True)

    def _read_cbor(self, filepath):
        """Read CBOR (Concise Binary Object Representation) format — IETF RFC 8949."""
        try:
            import cbor2
        except ImportError:
            raise ImportError("cbor2 is required for CBOR support. Run: pip install cbor2")
        with open(filepath, 'rb') as f:
            data = cbor2.load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        else:
            raise ValueError("CBOR file must contain a list of records or a single record dict.")

    def _write_cbor(self, df, filepath):
        """Write DataFrame to CBOR format."""
        try:
            import cbor2
        except ImportError:
            raise ImportError("cbor2 is required for CBOR export. Run: pip install cbor2")
        import datetime
        records = df.to_dict(orient='records')
        def _coerce(v):
            if isinstance(v, pd.Timestamp):
                return v.to_pydatetime()
            return v
        records = [{k: _coerce(v) for k, v in row.items()} for row in records]
        with open(filepath, 'wb') as f:
            cbor2.dump(records, f)

    def _read_protobuf(self, filepath):
        """
        Read a Protobuf binary file.
        Requires a companion .proto schema file with the same base name, OR a
        pre-compiled _pb2.py module on PYTHONPATH.
        Falls back to raw byte inspection and raises a descriptive error.
        """
        import importlib, sys
        stem = os.path.splitext(os.path.basename(filepath))[0]
        pb2_name = stem + '_pb2'
        parent_dir = os.path.dirname(os.path.abspath(filepath))
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        try:
            pb2 = importlib.import_module(pb2_name)
        except ModuleNotFoundError:
            raise ImportError(
                f"Could not find a compiled Protobuf module '{pb2_name}.py'.\n"
                "Protobuf requires a pre-compiled _pb2 module generated from a .proto schema.\n"
                "Steps:\n"
                "  1. Ensure you have a .proto schema file.\n"
                "  2. Run: protoc --python_out=. your_schema.proto\n"
                "  3. Place the generated *_pb2.py file next to your .pb file.\n"
                "  4. Re-run the import command.\n"
                "Tip: Run 'pip install protobuf grpcio-tools' to install the compiler."
            )
        from google.protobuf import descriptor as _descriptor
        from google.protobuf import message as _message
        msg_class = None
        for name in dir(pb2):
            obj = getattr(pb2, name)
            try:
                if isinstance(obj, type) and issubclass(obj, _message.Message) and obj is not _message.Message:
                    msg_class = obj
                    break
            except TypeError:
                continue
        if msg_class is None:
            raise ValueError(f"No Protobuf Message class found in '{pb2_name}'.")
        records = []
        with open(filepath, 'rb') as f:
            data = f.read()
        from google.protobuf.internal.decoder import _DecodeVarint
        pos = 0
        while pos < len(data):
            try:
                msg_len, new_pos = _DecodeVarint(data, pos)
                msg_buf = data[new_pos:new_pos + msg_len]
                record = msg_class()
                record.ParseFromString(msg_buf)
                records.append({f.name: getattr(record, f.name) for f in record.DESCRIPTOR.fields})
                pos = new_pos + msg_len
            except Exception:
                record = msg_class()
                record.ParseFromString(data)
                records = [{f.name: getattr(record, f.name) for f in record.DESCRIPTOR.fields}]
                break
        return pd.DataFrame(records)

    def _read_flatbuffers(self, filepath):
        """
        Read a FlatBuffers binary file.
        Like Protobuf, requires a pre-generated Python binding (*_generated.py).
        Raises a descriptive error when the binding is absent.
        """
        import importlib, sys
        stem = os.path.splitext(os.path.basename(filepath))[0]
        gen_name = stem + '_generated'
        parent_dir = os.path.dirname(os.path.abspath(filepath))
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        try:
            importlib.import_module(gen_name)
        except ModuleNotFoundError:
            raise ImportError(
                f"Could not find a generated FlatBuffers binding '{gen_name}.py'.\n"
                "FlatBuffers requires pre-generated Python code from a .fbs schema.\n"
                "Steps:\n"
                "  1. Write a .fbs schema for your data.\n"
                "  2. Run: flatc --python your_schema.fbs\n"
                "  3. Place the generated *_generated.py files next to your binary file.\n"
                "  4. Re-run the import command.\n"
                "Tip: Run 'pip install flatbuffers' and download flatc from https://github.com/google/flatbuffers/releases"
            )
        raise NotImplementedError(
            "FlatBuffers bindings found, but generic DataFrame conversion requires\n"
            "knowledge of your specific root table type. Please write a custom loader\n"
            "using your generated binding and load the result with pandas."
        )

    def _read_yaml(self, filepath):
        """Read YAML format."""
        try:
            import yaml
        except ImportError:
            raise ImportError("PyYAML is required for YAML support. Run: pip install PyYAML")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        else:
            raise ValueError("YAML file must contain a list of records or a single record dict.")

    def _write_yaml(self, df, filepath):
        """Write DataFrame to YAML format."""
        try:
            import yaml
        except ImportError:
            raise ImportError("PyYAML is required for YAML export. Run: pip install PyYAML")
            
        import datetime
        records = df.to_dict(orient='records')
        
        def _coerce(v):
            if pd.isna(v):  
                return None
            if isinstance(v, pd.Timestamp):
                return v.to_pydatetime()
            if hasattr(v, 'item'):
                return v.item()
            return v
            
        records = [{k: _coerce(v) for k, v in row.items()} for row in records]
        
        with open(filepath, 'w', encoding='utf-8') as f:
            yaml.dump(records, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    def do_import(self, filepath):
        """
        Import a file containing time-series data from a local path or URL.

        Supported formats:
          Text/Tabular : CSV, TSV, JSON, JSON Lines (JSONL/NDJSON), XML, HTML, YAML
          Spreadsheet  : Excel (.xls, .xlsx)
          Binary       : Parquet, ORC, Feather, HDF5, Pickle, NetCDF (.nc/.nc4/.cdf)
          Serialized   : MessagePack (.msgpack/.mpack), CBOR (.cbor)
          Schema-based : Protobuf (.pb/.proto) *, FlatBuffers (.bin/.fbs) *
                         (* requires pre-compiled Python bindings — see help)

        Usage: import <filepath_or_url>
        Examples:
          import data.csv
          import data.jsonl
          import data.nc
          import https://raw.githubusercontent.com/user/repo/main/data.parquet
        """
        if not filepath:
            print("Error: Please provide a file path or URL. Example: import data.csv")
            return
        
        filepath = filepath.strip("\"'")
        is_url = filepath.startswith('http://') or filepath.startswith('https://')
        
        if not is_url and not os.path.exists(filepath):
            print(f"Error: File '{filepath}' not found on local disk.")
            return

        source_type = "URL" if is_url else "file"
        print(f"Attempting to load {source_type} '{filepath}'...")
        
        if is_url:
            parsed_url = urlparse(filepath)
            ext = os.path.splitext(parsed_url.path)[1].lower()
        else:
            ext = os.path.splitext(filepath)[1].lower()
        
        try:
            if ext == '.json':
                self.dataFile = pd.read_json(filepath)
                print(f"Success: Loaded JSON from {source_type}.")

            elif ext in ['.yaml', '.yml']:
                if is_url:
                    import urllib.request
                    import tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                        urllib.request.urlretrieve(filepath, tmp.name)
                        self.dataFile = self._read_yaml(tmp.name)
                    os.unlink(tmp.name)
                else:
                    self.dataFile = self._read_yaml(filepath)
                print(f"Success: Loaded YAML from {source_type}.")

            elif ext in ['.jsonl', '.ndjson']:
                if is_url:
                    import urllib.request
                    import tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                        urllib.request.urlretrieve(filepath, tmp.name)
                        self.dataFile = self._read_jsonl(tmp.name)
                    os.unlink(tmp.name)
                else:
                    self.dataFile = self._read_jsonl(filepath)
                print(f"Success: Loaded JSON Lines (JSONL/NDJSON) from {source_type}.")

            elif ext in ['.xls', '.xlsx']:
                self.dataFile = pd.read_excel(filepath)
                print(f"Success: Loaded Excel ({ext}) from {source_type}.")

            elif ext == '.parquet':
                self.dataFile = pd.read_parquet(filepath)
                print(f"Success: Loaded Parquet from {source_type}.")

            elif ext == '.orc':
                if is_url:
                    import urllib.request, tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.orc') as tmp:
                        urllib.request.urlretrieve(filepath, tmp.name)
                        self.dataFile = self._read_orc(tmp.name)
                    os.unlink(tmp.name)
                else:
                    self.dataFile = self._read_orc(filepath)
                print(f"Success: Loaded Apache ORC from {source_type}.")

            elif ext in ['.pkl', '.pickle']:
                self.dataFile = pd.read_pickle(filepath)
                print(f"Success: Loaded Pickle from {source_type}.")

            elif ext == '.xml':
                self.dataFile = pd.read_xml(filepath)
                print(f"Success: Loaded XML from {source_type}.")

            elif ext in ['.feather', '.ftr']:
                self.dataFile = pd.read_feather(filepath)
                print(f"Success: Loaded Feather from {source_type}.")

            elif ext in ['.h5', '.hdf5']:
                self.dataFile = pd.read_hdf(filepath)
                print(f"Success: Loaded HDF5 from {source_type}.")

            elif ext in ['.html', '.htm']:
                dfs = pd.read_html(filepath)
                self.dataFile = dfs[0] 
                print(f"Success: Loaded HTML table (extracted table 1 of {len(dfs)}) from {source_type}.")

            elif ext in ['.nc', '.nc4', '.cdf']:
                if is_url:
                    import urllib.request, tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                        urllib.request.urlretrieve(filepath, tmp.name)
                        self.dataFile = self._read_netcdf(tmp.name)
                    os.unlink(tmp.name)
                else:
                    self.dataFile = self._read_netcdf(filepath)
                print(f"Success: Loaded NetCDF from {source_type}.")

            elif ext in ['.msgpack', '.mpack']:
                if is_url:
                    import urllib.request, tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                        urllib.request.urlretrieve(filepath, tmp.name)
                        self.dataFile = self._read_msgpack(tmp.name)
                    os.unlink(tmp.name)
                else:
                    self.dataFile = self._read_msgpack(filepath)
                print(f"Success: Loaded MessagePack from {source_type}.")

            elif ext == '.cbor':
                if is_url:
                    import urllib.request, tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.cbor') as tmp:
                        urllib.request.urlretrieve(filepath, tmp.name)
                        self.dataFile = self._read_cbor(tmp.name)
                    os.unlink(tmp.name)
                else:
                    self.dataFile = self._read_cbor(filepath)
                print(f"Success: Loaded CBOR from {source_type}.")

            elif ext in ['.pb', '.proto']:
                if is_url:
                    import urllib.request, tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                        urllib.request.urlretrieve(filepath, tmp.name)
                        self.dataFile = self._read_protobuf(tmp.name)
                    os.unlink(tmp.name)
                else:
                    self.dataFile = self._read_protobuf(filepath)
                print(f"Success: Loaded Protobuf from {source_type}.")

            elif ext in ['.fbs', '.flatbuffers']:
                if is_url:
                    import urllib.request, tempfile
                    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                        urllib.request.urlretrieve(filepath, tmp.name)
                        self.dataFile = self._read_flatbuffers(tmp.name)
                    os.unlink(tmp.name)
                else:
                    self.dataFile = self._read_flatbuffers(filepath)
                print(f"Success: Loaded FlatBuffers from {source_type}.")

            else:
                try:
                    self.dataFile = pd.read_csv(filepath, sep=None, engine='python')
                    print(f"Success: Detected and loaded as delimited text/CSV from {source_type}.")
                except ValueError:
                    self.dataFile = pd.read_json(filepath)
                    print(f"Success: Detected and loaded as JSON from {source_type}.")
            
            self.history = []
            self.custom_roles = {}
            self._auto_detect_timecol()
            self.do_show('info')
            self.do_show('roles')
                
        except ImportError as ie:
            print(f"\nMissing dependency to read this file type: {ie}")
            print("Tip: Run 'pip install -r requirements.txt' to enable all formats.")
            self.dataFile = None
        except Exception as e:
            print(f"Failed to load {source_type}. Ensure the path/URL is correct and supported. Error: {e}")
            self.dataFile = None


    def do_show(self, arg):
        """
        Display information about the loaded dataset.
        Usage:
          show columns     - Show available columns and their data types.
          show rows        - Show the total number of rows in the dataset.
          show head [n]    - Show the first 5 (or n) rows of the dataset.
          show tail [n]    - Show the last 5 (or n) rows of the dataset.
          show info        - Show a general summary of the dataset.
          show roles       - Auto-detect which columns are Time, Labels, or Values.
        """
        if self.dataFile is None:
            print("Error: No data loaded. Please 'import <filepath>' first.")
            return

        args = arg.strip().lower().split()
        if not args:
            print("Error: Please specify what to show. Options: columns, rows, head, tail, info, roles.")
            return

        subcmd = args[0]
        if subcmd == 'total' and len(args) > 1 and args[1] == 'rows':
            subcmd = 'rows'

        if subcmd == 'columns':
            print("\nAvailable Columns/Tags:")
            print("-" * 30)
            for col in self.dataFile.columns:
                dtype = self.dataFile[col].dtype
                print(f" * {col} (Type: {dtype})")
            print("-" * 30)
            print(f"Total columns: {len(self.dataFile.columns)}\n")

        elif subcmd == 'rows':
            print(f"\nTotal rows in dataset: {len(self.dataFile)}\n")

        elif subcmd == 'head':
            n = 5
            if len(args) > 1 and args[-1].isdigit():
                n = int(args[-1])
            print(f"\n--- First {n} Rows ---")
            print(self.dataFile.head(n).to_string())
            print("-" * 20 + "\n")

        elif subcmd == 'tail':
            n = 5
            if len(args) > 1 and args[-1].isdigit():
                n = int(args[-1])
            print(f"\n--- Last {n} Rows ---")
            print(self.dataFile.tail(n).to_string())
            print("-" * 20 + "\n")

        elif subcmd == 'info':
            print("\n--- Dataset Summary ---")
            print(f"Rows: {len(self.dataFile)}")
            print(f"Columns: {len(self.dataFile.columns)}")
            print("\nMemory Usage:")
            import io
            buffer = io.StringIO()
            self.dataFile.info(buf=buffer, memory_usage='deep', verbose=False)
            print(buffer.getvalue())
            print("-" * 23 + "\n")
            
        elif subcmd in ['roles', 'profile']:
            print("\n--- Column Roles (Auto-Detected & Manual) ---")
            time_cols = []
            label_cols = []
            value_cols = []
            
            for col in self.dataFile.columns:
                if col in self.custom_roles:
                    role = self.custom_roles[col]
                    display_name = f"{col} (manual override)"
                    if role == 'time': time_cols.append(display_name)
                    elif role == 'label': label_cols.append(display_name)
                    elif role == 'value': value_cols.append(display_name)
                    continue
                
                if pd.api.types.is_datetime64_any_dtype(self.dataFile[col]):
                    time_cols.append(col)
                elif self.dataFile[col].dtype == 'object' or self.dataFile[col].dtype == 'bool' or self.dataFile[col].dtype.name == 'category':
                    label_cols.append(col)
                elif pd.api.types.is_numeric_dtype(self.dataFile[col]):
                    unique_count = self.dataFile[col].nunique()
                    
                    if pd.api.types.is_float_dtype(self.dataFile[col]):
                        value_cols.append(col)
                    elif unique_count <= 5: 
                        label_cols.append(f"{col} (numeric label, {unique_count} unique)")
                    else:
                        value_cols.append(col)
                else:
                    label_cols.append(col)
            
            print(f"\n🕒 Time/Index Columns ({len(time_cols)}):")
            for c in time_cols: print(f"  * {c}")
            
            print(f"\n🏷️  Labels/Dimensions ({len(label_cols)}):")
            for c in label_cols: print(f"  * {c}")
            
            print(f"\n📈 Values/Metrics ({len(value_cols)}):")
            for c in value_cols: print(f"  * {c}")
            print("-" * 34 + "\n")

        else:
            print(f"Error: Unknown show argument '{subcmd}'. Use 'help show' to see available options.")

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
            self._save_history()
            print(f"Converting '{col_name}' to datetime objects...")
            self.dataFile[col_name] = pd.to_datetime(self.dataFile[col_name])
            print(f"Success: '{col_name}' is now a datetime type. You can verify with 'columns'.")
        except Exception as e:
            self.history.pop()
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
            
        self._save_history()
        self.dataFile.sort_values(by=col_name, ascending=ascending, inplace=True)
        order_str = 'ascending' if ascending else 'descending'
        print(f"Success: Data sorted by '{col_name}' in {order_str} order.")

    def do_trim(self, arg):
        """
        Clean or reduce the dataset. 
        Usage: 
          trim missing                           - Removes any rows containing missing (NaN/Null) values.
          trim spaces                            - Removes leading/trailing whitespace from string columns.
          trim <head|tail> <number>              - Keeps only the <top|bottom> <number> of rows.
          trim date <col> <before|after> <date>  - REMOVES rows before or after an ISO timestamp.
          trim date <col> between <d1> <d2>      - REMOVES rows between two ISO timestamps.
        """
        if self.dataFile is None:
            print("Error: No data loaded. Please 'import <filepath>' first.")
            return
            
        args = arg.strip().split()
        if not args:
            print("Error: Please specify what to trim. Options: 'missing', 'spaces', 'head <n>', 'tail <n>' or 'date'.")
            return
            
        initial_rows = len(self.dataFile)
        cmd_type = args[0].lower()
            
        if cmd_type == 'missing':
            self._save_history()
            self.dataFile.dropna(inplace=True)
            new_rows = len(self.dataFile)
            print(f"Success: Removed {initial_rows - new_rows} rows with missing values.")
            
        elif cmd_type == 'spaces':
            str_cols = self.dataFile.select_dtypes(include=['object', 'str']).columns
            if len(str_cols) == 0:
                print("Notice: No text columns found to trim spaces from.")
                return
                
            self._save_history()
            for col in str_cols:
                self.dataFile[col] = self.dataFile[col].apply(
                    lambda x: x.strip() if isinstance(x, str) else x
                )
            print(f"Success: Trimmed leading and trailing spaces from {len(str_cols)} text columns.")
            
        elif cmd_type in ['head', 'tail']:
            if len(args) < 2 or not args[1].isdigit():
                print(f"Error: Missing or invalid number. Usage: trim {cmd_type} <number>")
                return
                
            self._save_history()
            n = int(args[1])
            
            if cmd_type == 'head':
                self.dataFile = self.dataFile.head(n)
            else:
                self.dataFile = self.dataFile.tail(n)
                
            new_rows = len(self.dataFile)
            print(f"Success: Trimmed dataset to the {cmd_type} {new_rows} rows. (Removed {initial_rows - new_rows} rows).")
            
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
                
                self._save_history() 
                
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
                self.history.pop() 
                print(f"Error parsing dates. Please ensure they are in ISO format (e.g., 2026-04-06T01:19:19Z). Error: {e}")

        else:
            print("Error: Unknown trim argument. Use 'help trim' to see available options.")


    def do_undo(self, arg):
        """
        Undo the last modification (trim, sort, timecol).
        Usage: undo
        """
        if not self.history:
            print("Error: Nothing to undo. Either no changes have been made or you reached the undo limit.")
            return
            
        self.dataFile = self.history.pop()
        print(f"Success: Reverted to the previous dataset state. (Current rows: {len(self.dataFile)})")

    def do_export(self, filepath):
        """
        Export/Save the current dataset to a file.

        Supported formats:
          Text/Tabular : CSV, TSV (.tsv), JSON, JSON Lines (.jsonl/.ndjson), XML, HTML, YAML
          Spreadsheet  : Excel (.xlsx)
          Binary       : Parquet, ORC, Feather, HDF5, Pickle, NetCDF (.nc)
          Serialized   : MessagePack (.msgpack), CBOR (.cbor)

        Usage: export <filepath>  (alias: save <filepath>)
        Examples:
          export processed_data.parquet
          export output.jsonl
          export results.msgpack
        """
        if self.dataFile is None:
            print("Error: No data to export. Please 'import' a file first.")
            return

        if not filepath:
            print("Error: Please provide a destination filepath. Example: export output.csv")
            return

        filepath = filepath.strip("\"'")
        ext = os.path.splitext(filepath)[1].lower()

        print(f"Attempting to export data to '{filepath}'...")

        df_to_save = self.dataFile.copy()
        
        if ext in ['.csv', '.tsv', '.json', '.jsonl', '.ndjson', '.xml', '.html', '.htm', '']:
            for col in df_to_save.columns:
                if pd.api.types.is_datetime64_any_dtype(df_to_save[col]):
                    df_to_save[col] = df_to_save[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        elif ext in ['.yaml', '.yml']:
                self._write_yaml(df_to_save, filepath)
                print("Success: Dataset exported to YAML format.")
        
        elif ext in ['.xls', '.xlsx']:
            for col in df_to_save.columns:
                if pd.api.types.is_datetime64_any_dtype(df_to_save[col]):
                    if df_to_save[col].dt.tz is not None:
                        df_to_save[col] = df_to_save[col].dt.tz_localize(None)

        try:
            if ext == '.csv':
                df_to_save.to_csv(filepath, index=False)
                print("Success: Dataset exported to CSV format.")

            elif ext == '.tsv':
                df_to_save.to_csv(filepath, index=False, sep='\t')
                print("Success: Dataset exported to TSV format.")

            elif ext == '.json':
                df_to_save.to_json(filepath, orient='records', indent=4)
                print("Success: Dataset exported to JSON format.")

            elif ext in ['.jsonl', '.ndjson']:
                self._write_jsonl(df_to_save, filepath)
                print("Success: Dataset exported to JSON Lines (JSONL/NDJSON) format.")

            elif ext in ['.xls', '.xlsx']:
                if ext == '.xls':
                    print("Notice: Legacy '.xls' format is deprecated. Auto-correcting to '.xlsx'...")
                    filepath = filepath[:-4] + '.xlsx'
                df_to_save.to_excel(filepath, index=False, engine='openpyxl')
                print("Success: Dataset exported to Excel (.xlsx) format.")

            elif ext == '.parquet':
                df_to_save.to_parquet(filepath)
                print("Success: Dataset exported to Parquet format.")

            elif ext == '.orc':
                self._write_orc(df_to_save, filepath)
                print("Success: Dataset exported to Apache ORC format.")

            elif ext in ['.pkl', '.pickle']:
                df_to_save.to_pickle(filepath)
                print("Success: Dataset exported to Pickle format.")

            elif ext == '.xml':
                df_to_save.to_xml(filepath, index=False)
                print("Success: Dataset exported to XML format.")

            elif ext in ['.feather', '.ftr']:
                df_to_save.to_feather(filepath)
                print("Success: Dataset exported to Feather format.")

            elif ext in ['.h5', '.hdf5']:
                df_to_save.to_hdf(filepath, key='data', mode='w')
                print("Success: Dataset exported to HDF5 format.")

            elif ext in ['.html', '.htm']:
                df_to_save.to_html(filepath, index=False)
                print("Success: Dataset exported to HTML format.")

            elif ext in ['.nc', '.nc4', '.cdf']:
                self._write_netcdf(df_to_save, filepath)
                print("Success: Dataset exported to NetCDF format.")

            elif ext in ['.msgpack', '.mpack']:
                self._write_msgpack(df_to_save, filepath)
                print("Success: Dataset exported to MessagePack format.")

            elif ext == '.cbor':
                self._write_cbor(df_to_save, filepath)
                print("Success: Dataset exported to CBOR format.")

            else:
                print(f"Unknown extension '{ext}'. Defaulting to CSV export.")
                new_path = filepath + ".csv" if not ext else filepath.replace(ext, ".csv")
                df_to_save.to_csv(new_path, index=False)
                print(f"Saved as: {new_path}")

        except ImportError as ie:
            print(f"\nMissing dependency to export to this format: {ie}")
            print("Tip: Run 'pip install -r requirements.txt' to enable all formats.")
        except Exception as e:
            print(f"Failed to export file. Error: {e}")

    do_save = do_export

    def do_setrole(self, arg):
        """
        Manually assign a role to a column. 
        Overrides auto-detection in 'show roles'.
        Usage: setrole <column_name> <time|label|value>
        Example: setrole City ID label
        """
        if self.dataFile is None:
            print("Error: No data loaded.")
            return

        args = arg.split()
        if len(args) < 2:
            print("Error: Invalid arguments. Usage: setrole <column_name> <time|label|value>")
            return

        role = args[-1].lower()
        col_name = " ".join(args[:-1])

        if col_name not in self.dataFile.columns:
            print(f"Error: Column '{col_name}' not found in dataset.")
            return

        if role not in ['time', 'label', 'value']:
            print("Error: Role must be exactly 'time', 'label', or 'value'.")
            return

        self.custom_roles[col_name] = role
        print(f"Success: Column '{col_name}' manually forced to role: '{role.upper()}'.")
        
        if role == 'time' and not pd.api.types.is_datetime64_any_dtype(self.dataFile[col_name]):
            print(f"Notice: You marked this as Time, but it's not a datetime object.")
            print(f"Consider running: timecol {col_name}")

    def emptyline(self):
        """
        Nadpisuje domyślne zachowanie modułu cmd, 
        aby zapobiec powtarzaniu ostatniej komendy po wciśnięciu Enter.
        """
        pass

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
    app = OctoTS()
    while True:
        try:
            app.cmdloop()
            break  
        except KeyboardInterrupt:
            print("^C\n(Use the 'exit' or 'quit' command to close OctoTS)")
            app.intro = ''