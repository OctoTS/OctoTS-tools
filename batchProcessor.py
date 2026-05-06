import typer
import sys
import os
import pandas as pd
from datetime import datetime, timezone
from typing import Callable
from rich.console import Console
import sqlite3

app = typer.Typer(
    help="OctoTS Batch Processor",
    no_args_is_help=True
)
console = Console()
HDF5_DEFAULT_KEY = 'data'
LARGE_FILE_THRESHOLD_MB = 500  # Warning threshold: noticeable lag on typical machines
CRITICAL_FILE_THRESHOLD_MB = 1500  # Critical threshold: high OOM risk on 8-16GB machines

def get_hdf5_keys(path: str) -> list[str]:
    """Get all available keys in a pandas-compatible HDF5 file."""
    with pd.HDFStore(path, mode='r') as store:
        return list(store.keys())


def _read_hdf5(path: str) -> pd.DataFrame:
    try:
        return pd.read_hdf(path, key=HDF5_DEFAULT_KEY)
    except KeyError:
        available_keys = get_hdf5_keys(path)
        if available_keys:
            alt_key = available_keys[0].lstrip('/')
            console.print(f"[yellow]WARNING:[/yellow] Key '{HDF5_DEFAULT_KEY}' not found in HDF5 file. "
                         f"Available keys: {available_keys}. Using first available: '{alt_key}'")
            return pd.read_hdf(path, key=alt_key)
        raise ValueError(f"No datasets found in HDF5 file: {path}")


def _read_pickle(path: str) -> pd.DataFrame:
    console.print("[yellow]WARNING:[/yellow] Loading pickle files executes arbitrary code. Only load trusted sources.")
    return pd.read_pickle(path)


def _read_sql(path: str) -> pd.DataFrame:
    conn = sqlite3.connect(path)
    df = pd.read_sql('SELECT * FROM metrics', conn)
    conn.close()
    return df


def _write_sql(df: pd.DataFrame, path: str, file_exists: bool):
    conn = sqlite3.connect(path)
    df.to_sql('metrics', conn, if_exists='replace', index=False)
    conn.close()


STORAGE_FORMATS = {
    'csv': {
        'extensions': ['.csv', '.txt'],
        'reader': lambda path: pd.read_csv(path),
        'writer': lambda df, path, file_exists: df.to_csv(path, mode='a', index=False, header=not file_exists),
    },
    'tsv': {
        'extensions': ['.tsv'],
        'reader': lambda path: pd.read_csv(path, sep='\t'),
        'writer': lambda df, path, file_exists: df.to_csv(path, mode='a', index=False, sep='\t', header=not file_exists),
    },
    'json': {
        'extensions': ['.json'],
        'reader': pd.read_json,
        'writer': lambda df, path, file_exists: df.to_json(path, orient='records', indent=4),
    },
    'jsonl': {
        'extensions': ['.jsonl', '.ndjson'],
        'reader': lambda path: pd.read_json(path, lines=True),
        'writer': lambda df, path, file_exists: df.to_json(path, mode='a', orient='records', lines=True),
    },
    'excel': {
        'extensions': ['.xls', '.xlsx'],
        'reader': pd.read_excel,
        'writer': lambda df, path, file_exists: df.to_excel(path, index=False),
    },
    'xlsx': {
        'extensions': ['.xlsx'],
        'reader': pd.read_excel,
        'writer': lambda df, path, file_exists: df.to_excel(path, index=False),
    },
    'parquet': {
        'extensions': ['.parquet'],
        'reader': pd.read_parquet,
        'writer': lambda df, path, file_exists: df.to_parquet(path, index=False),
    },
    'feather': {
        'extensions': ['.feather', '.ftr'],
        'reader': pd.read_feather,
        'writer': lambda df, path, file_exists: df.to_feather(path),
    },
    'hdf5': {
        'extensions': ['.h5', '.hdf5'],
        'reader': _read_hdf5,
        'writer': lambda df, path, file_exists: df.to_hdf(path, key=HDF5_DEFAULT_KEY, mode='w'),
    },
    'xml': {
        'extensions': ['.xml'],
        'reader': pd.read_xml,
        'writer': lambda df, path, file_exists: df.to_xml(path, index=False),
    },
    'html': {
        'extensions': ['.html', '.htm'],
        'reader': None,
        'writer': lambda df, path, file_exists: df.to_html(path, index=False),
    },
    'md': {
        'extensions': ['.md', '.markdown'],
        'reader': None,
        'writer': lambda df, path, file_exists: df.to_markdown(path, index=False),
    },
    'latex': {
        'extensions': ['.tex'],
        'reader': None,
        'writer': lambda df, path, file_exists: df.to_latex(path, index=False),
    },
    'sql': {
        'extensions': ['.sql', '.db', '.sqlite'],
        'reader': _read_sql,
        'writer': _write_sql,
    },
    'pickle': {
        'extensions': ['.pkl', '.pickle'],
        'reader': _read_pickle,
        'writer': lambda df, path, file_exists: df.to_pickle(path),
    },
}

EXTENSION_FORMAT_MAP = {
    ext: fmt for fmt, fmt_spec in STORAGE_FORMATS.items() for ext in fmt_spec['extensions']
}

def _needs_newline(path: str) -> bool:
    if os.path.getsize(path) == 0:
        return False
    with open(path, 'rb') as f:
        f.seek(-1, os.SEEK_END)
        return f.read(1) != b'\n'

def validate_storage_extension(path: str, storage_type: str):
    """Strictly validates if the file extension matches the storage engine."""
    ext = os.path.splitext(path)[1].lower()
    format_spec = STORAGE_FORMATS.get(storage_type, {})
    expected = format_spec.get('extensions', [])

    if ext and ext not in expected:
        console.print(
            f"[bold red]ERROR:[/bold red] Extension mismatch! Storage is '{storage_type}' but file is '{ext}'.")
        console.print("To change formats, use the 'convert' command.")
        raise typer.Exit(code=1)


def load_input(source: str):
    """Autodetect format and load into a DataFrame."""
    if source == 'stdin':
        return pd.read_csv(sys.stdin)

    if not os.path.exists(source):
        raise FileNotFoundError(f"Source file not found: {source}")

    ext = os.path.splitext(source)[1].lower()
    format_key = EXTENSION_FORMAT_MAP.get(ext)
    if format_key is None:
        return pd.read_csv(source)

    reader = STORAGE_FORMATS[format_key]['reader']
    if reader is None:
        raise ValueError(f"Reading {ext} files is not supported.")
    return reader(source)


def normalize_timestamp(df: pd.DataFrame):
    """Ensures strict ISO 8601 UTC format as the first column (Index 0)."""
    if df.empty:
        return df

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        else:
            df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
        df['timestamp'] = df['timestamp'].fillna(datetime.now(timezone.utc))
    else:
        df.insert(0, 'timestamp', datetime.now(timezone.utc))

    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    cols = ['timestamp'] + [c for c in df.columns if c != 'timestamp']
    return df[cols]


def save_output(df: pd.DataFrame, path: str, storage_type: str):
    """Persistence layer using direct append or Read-Modify-Write logic."""
    file_exists = os.path.isfile(path)

    format_spec = STORAGE_FORMATS.get(storage_type)
    if format_spec is None or format_spec['writer'] is None:
        raise ValueError(f"Unsupported storage engine: {storage_type}")

    # Native append for text-based time series
    if storage_type in {'csv', 'tsv', 'jsonl'}:
        if file_exists and _needs_newline(path):
            with open(path, 'ab') as f:
                f.write(b'\n')
        if storage_type == 'csv':
            df.to_csv(path, mode='a', index=False, header=not file_exists)
            return
        if storage_type == 'tsv':
            df.to_csv(path, mode='a', index=False, sep='\t', header=not file_exists)
            return
        if storage_type == 'jsonl':
            df.to_json(path, mode='a', orient='records', lines=True)
            return
    
    # Large file warning for binary/structured formats
    if file_exists:
        file_size_mb = os.path.getsize(path) / (1024 * 1024)
        if file_size_mb > CRITICAL_FILE_THRESHOLD_MB:
            console.print(f"[bold red]CRITICAL:[/bold red] File {storage_type.upper()} ({file_size_mb:.1f} MB) is extremely large. "
                         f"Read-Modify-Write will likely cause Out-Of-Memory (OOM) errors. "
                         f"STRONGLY recommend using CSV/TSV/JSONL or a database backend instead.")
        elif file_size_mb > LARGE_FILE_THRESHOLD_MB:
            console.print(f"[yellow]WARNING:[/yellow] Appending to large {storage_type.upper()} file ({file_size_mb:.1f} MB). "
                         f"Read-Modify-Write is expensive for big datasets. Consider using CSV/TSV/JSONL for better performance.")

    # Read-Modify-Write for binary/structured formats
    if file_exists:
        try:
            existing_df = load_input(path)
            df = pd.concat([existing_df, df], ignore_index=True)
        except Exception as e:
            console.print(f"[bold red]CRITICAL ERROR:[/bold red] Database corrupted: {e}")
            raise typer.Exit(code=1)

    writer = format_spec['writer']
    writer(df, path, file_exists)

@app.command()
def append(
        storage_type: str = typer.Argument(..., help="Engine: csv, tsv, json, jsonl, excel, parquet, etc."),
        input_path: str = typer.Argument(..., help="Input source (file path or 'stdin')"),
        output_path: str = typer.Argument(..., help="Destination database file path"),
):
    """Process incoming metrics and append them to a persistent time-series storage."""
    if os.path.exists(input_path) and os.path.abspath(input_path) == os.path.abspath(output_path):
        console.print("[bold red]ERROR:[/bold red] Cannot append a file to itself.")
        raise typer.Exit(code=1)

    validate_storage_extension(output_path, storage_type.lower())

    try:
        new_df = load_input(input_path)
        if new_df.empty:
            console.print("[yellow]NOTICE:[/yellow] Input empty. Skipping.")
            return

        new_df = normalize_timestamp(new_df)
        save_output(new_df, output_path, storage_type.lower())
        console.print(f"[bold green]SUCCESS:[/bold green] Added to {storage_type.upper()} storage.")
    except Exception as e:
        console.print(f"[bold red]INTERNAL ERROR:[/bold red] {e}")
        raise typer.Exit(code=1)


@app.command()
def convert(
        storage_type: str = typer.Argument(..., help="Target storage format"),
        input_path: str = typer.Argument(..., help="Source database to convert"),
        output_path: str = typer.Argument(..., help="Path for the new database"),
):
    """Convert an existing database to a new format while preserving timestamp integrity."""
    validate_storage_extension(output_path, storage_type.lower())
    try:
        df = load_input(input_path)
        if df.empty: raise ValueError("Source is empty.")
        df = normalize_timestamp(df)
        save_output(df, output_path, storage_type.lower())
        console.print(f"[bold green]SUCCESS:[/bold green] Converted to {storage_type.upper()}.")
    except Exception as e:
        console.print(f"[bold red]CONVERSION ERROR:[/bold red] {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()