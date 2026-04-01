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
Combine it with the generator using a pipe:
```bash
python3 generateSampleData.py [dir] | python3 processData.py --output [file.csv] --min [value] --sort [loc/author]
