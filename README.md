# OctoTS CI/CD tools

## Generating sample data with file  `generateSampleData.py`

Use this file to generate sample data about your Github repository (author of code, line of code)

### flags:
- `--output <file>` this flag is optional by default the script going to output data on console output. 

### Usage:


> **Warning**  
>  You have to use it on git repository directory

`python3 generateSampleData.py <dir> [flags]`

# OctoTS Batch ⚙️

The **OctoTS Batch Processor** (`batchProcessor.py`) is the non-interactive version of the engine, specifically designed for automated environments and CI/CD pipelines (like GitHub Actions). It handles the heavy lifting of data ingestion without requiring user input.

### Batch Features
*   **Automated Ingestion:** Perfect for cron jobs; it processes data from start to finish and exits automatically.
*   **Pipe Support (stdin):** Seamlessly integrates with other tools. You can pipe raw generator output directly into the processor.
*   **Instant Timestamping:** Automatically injects a strict ISO 8601 UTC timestamp as the **first column** of every new record.
*   **Format Bridging:** Supports reading from one format (e.g., CSV) and appending to another (e.g., Parquet) in a single command.

## Requirements

Ensure you have Python 3 installed. You can install all necessary dependencies for **both the Batch Processor and OctoTS** using the provided requirements file:

```bash
pip install -r requirements.txt
```
*(Note: Ensure your requirements.txt contains pandas, and optionally openpyxl for Excel support and pyarrow for Parquet support).*

### Batch Command Reference

Run the script using positional arguments:
`python3 batchProcessor.py append <output_type> <input_path|stdin> <output_path>`

#### Examples:

**1. Basic CSV Append**
Appends new local data to a CSV database:
```bash
python3 batchProcessor.py append csv new_data.csv database.csv
```

**2. Pipeline Mode (CI/CD Ready)**
Receives data from another script and saves it to a Parquet store:
```bash
python3 data_generator.py | python3 batchProcessor.py append parquet stdin database.parquet
```

**3. JSON to Excel Conversion**
Takes a JSON file, adds timestamps, and merges it into an Excel report:
```bash
python3 batchProcessor.py append excel updates.json report.xlsx
```

# OctoTS 🐙

OctoTS is a lightweight, interactive Command-Line Interface (CLI) tool designed for quick and easy time-series data manipulation. Built on top of Python's `cmd` module and `pandas`, it allows you to import datasets, auto-detect and convert timestamps, filter data by specific date ranges, and export your cleaned time-series data with strict ISO 8601 formatting.

## Features

* **Multi-Format Support:** Import and save data as CSV, JSON, Excel (`.xlsx`), Parquet, or Pickle.
* **Smart Timestamp Detection:** Automatically scans your dataset on import to find and convert date-like strings into usable datetime objects.
* **Flexible Trimming:** Remove missing values, strip whitespace, limit row counts, or filter data before, after, or between specific dates.
* **Undo System:** Made a mistake? Use the `undo` command to revert to previous states (keeps up to the last 5 states in memory).
* **ISO 8601 Export:** Ensures all your datetime columns are strictly formatted to ISO standards (`YYYY-MM-DDThh:mm:ssZ`) when saving.


## Quick Start
1. Clone or download the script to your local machine.
2. Run the script via your terminal:

```Bash
python octots.py
```
3. You will be greeted by the (OctoTS) prompt. Type help or ? to see available commands.

## Command Reference

### File Operations

- `import <filepath>`  
    
    Loads a time-series dataset into the CLI. Automatically attempts to detect and convert time columns.
    
    Example: `import data.csv`

- `save <filepath>`
    
    Saves the current state of your dataset to a file. Datetimes are automatically converted to strict ISO 8601 strings.


    Example: `save cleaned_data.json`

### Data Inspection

- `columns`
    
    Displays a list of all tags/columns currently loaded in the dataset and their respective data types.

### Data Manipulation

- `timecol <column_name>`
    
    Manually converts a specified column into mathematical datetime objects if the automatic detection missed it.
    Example: timecol timestamp

- `sort <column_name> [asc|desc]`
    
    Sorts the dataset by a specific column in ascending (default) or descending order.
    Example: sort temperature desc

### Trimming & Cleaning

The trim command is highly versatile and allows you to clean or slice your dataset in multiple ways:

- `trim missing`

    Removes any rows containing missing (NaN/Null) values.

- `trim spaces`

    Removes leading and trailing whitespace from all string/text columns.

- `trim <number>`
    
    Keeps only the top <number> of rows in the dataset.
    
    Example: `trim 100 (keeps the first 100 rows).`

- `trim date <col> <before|after> <date>`
    
    Removes rows occurring strictly before or after an ISO timestamp.
    
    Example: `trim date timestamp after 2026-04-06T00:00:00Z`

- `trim date <col> between <date1> <date2>`
    
    Removes rows falling between two ISO timestamps.
    
    Example: `trim date timestamp between 2026-04-06T00:00:00Z 2026-04-06T01:19:19Z`

### Utility

- `undo`
    
    Reverts the dataset to its state before the last `trim`, `sort`, or `timecol` command.

- `exit` or `quit`

    Closes the OctoTS shell.
