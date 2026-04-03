# OctoTS CI/CD tools

## Generating sample data with file  `generateSampleData.py`

Use this file to generate sample data about your Github repository (author of code, line of code)

### flags:
- `--output <file>` this flag is optional by default the script going to output data on console output. 

### Usage:


> **Warning**  
>  You have to use it on git repository directory

`python3 generateSampleData.py <dir> [flags]`

## Data Processing
The `processData.py` script acts as a data manager. It takes raw CSV data from the standard input, adds ISO 8601 timestamps, filters the records, and saves them to a persistent database file.

### Usage

#### 1. Pipeline Mode (Recommended)

This method pipes raw data directly into the processor for immediate standardization and storage.

```python3 generateSampleData.py [dir] | python3 processData.py --output [file.csv] --min [value] --sort [loc/author] ```

#### 2. File-based Mode (CI/CD Artifacts)

Use this mode when you need to store raw metrics as artifacts before processing them.



```python3 processData.py --input [file.csv] --output [file.csv] --min 10 ```


### Processor Flags Reference

 
| Flag | Description  |
|--|--|
| --output | Required. Path to the final CSV database file. |  
| --input | Optional. Path to a source CSV file (if not using pipes). |
| --min | Optional. Filters out records with LOC lower than the value. |
| --max | Optional. Filters out records with LOC higher than the value. |
| --sort | Optional. Sorts by 'loc' (descending) or 'author' (alphabetical). |
