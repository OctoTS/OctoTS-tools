import os
import datetime
import argparse


def count_lines_in_file(file_path):
    """Counts lines in a single file, handling potential encoding issues."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return sum(1 for line in f)
    except Exception:
        # Skip files that cannot be read (e.g. binary files)
        return 0


def collect_metrics(target_dir):
    """Scans the directory and calculates total Lines of Code"""
    total_loc = 0

    # Directories to be ignored to ensure accurate metrics and performance
    excluded_dirs = {
        '.git', 'node_modules', 'venv', '.venv', '__pycache__',
        'dist', 'build', '.next', 'out'
    }

    supported_extensions = {
        '.py', '.js', '.ts', '.tsx', '.jsx', '.go', '.c', '.cpp',
        '.h', '.java', '.cs', '.html', '.css', '.yaml', '.yml'
    }

    for root, dirs, files in os.walk(target_dir):
        # Modify dirs in-place to skip excluded directories during recursion
        dirs[:] = [d for d in dirs if d not in excluded_dirs]

        for file in files:
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext in supported_extensions:
                file_path = os.path.join(root, file)
                total_loc += count_lines_in_file(file_path)

    return total_loc


def main():
    # Setup command line arguments for flexibility in CI/CD
    parser = argparse.ArgumentParser(description="OctoTS LOC Collector")
    parser.add_argument("target_dir", help="Directory to scan for code")
    parser.add_argument("--output", help="CSV file to append data to", default=None)

    args = parser.parse_args()

    # Run the collection logic
    loc_count = collect_metrics(args.target_dir)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    csv_line = f"{timestamp},{loc_count}"

    if args.output:
        # Append to the specified CSV file
        file_exists = os.path.isfile(args.output)
        with open(args.output, 'a', encoding='utf-8') as f:
            if not file_exists:
                f.write("timestamp,lines_of_code\n")
            f.write(csv_line + "\n")
        print(f"Metrics saved to {args.output}: {csv_line}")
    else:
        # Default output to stdout for GitHub Actions logs
        print(csv_line)


if __name__ == "__main__":
    main()