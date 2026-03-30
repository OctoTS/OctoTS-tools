import subprocess
import argparse
from collections import defaultdict

def get_merge_stats(target_dir):
    """Pobiera autorów i dodane linie ze wszystkich commitów z ostatniego merge'a."""
    authors_stats = defaultdict(int)
    
    try:
        cmd = ['git', 'log', 'HEAD^1..HEAD', '--no-merges', '--format=AUTHOR:%aN', '--numstat']
        
        result = subprocess.run(
            cmd,
            cwd=target_dir,
            capture_output=True,
            text=True,
            check=True
        )
        
        lines = result.stdout.strip().splitlines()
        if not lines:
            return None
            
        current_author = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if line.startswith('AUTHOR:'):
                current_author = line[7:]
                continue
                
            if current_author:
                parts = line.split('\t')
                if len(parts) >= 3:
                    added = parts[0]
                    
                    if added == '-':
                        continue
                        
                    authors_stats[current_author] += int(added)
                    
        return dict(authors_stats)

    except subprocess.CalledProcessError:
        return None
    except Exception:
        return None

def main():
    parser = argparse.ArgumentParser(description="Git Merge LOC Collector")
    parser.add_argument("target_dir", help="Directory to scan for code")
    parser.add_argument("--output", help="Path to the temporary CSV file", default=None)
    args = parser.parse_args()

    stats = get_merge_stats(args.target_dir)

    if stats:
        csv_lines = []
        for author, added_lines in stats.items():
            if added_lines > 0:
                csv_lines.append(f"{author},{added_lines}")
                
        if not csv_lines:
            return

        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write("author,lines_of_code\n")
                for line in csv_lines:
                    f.write(line + "\n")
            print(f"Wygenerowano nowy plik tymczasowy: {args.output}")
        else:
            print("author,lines_of_code")
            for line in csv_lines:
                print(line)

if __name__ == "__main__":
    main()