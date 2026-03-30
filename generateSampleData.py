import subprocess
import argparse
from collections import defaultdict

def get_merge_stats(target_dir):
    """Pobiera autorów i dodane linie ze wszystkich commitów wchodzących w skład ostatniego merge'a."""
    
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
    args = parser.parse_args()

    stats = get_merge_stats(args.target_dir)

    if stats:
        for author, added_lines in stats.items():
            if added_lines > 0:
                print(f"{author},{added_lines}")

if __name__ == "__main__":
    main()