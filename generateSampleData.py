import subprocess
import argparse
import sys
from collections import defaultdict

def get_merge_stats(target_dir):
    print("--- ROZPOCZYNAM DZIAŁANIE SKRYPTU ---")
    authors_stats = defaultdict(int)
    
    try:
        cmd = ['git', 'log', 'HEAD^1..HEAD', '--no-merges', '--format=AUTHOR:%aN', '--numstat']
        print(f"Odpalam komendę: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            cwd=target_dir,
            capture_output=True,
            text=True,
            check=True
        )
        
        print(f"Surowy wynik z gita:\n{result.stdout}")
        
        lines = result.stdout.strip().splitlines()
        if not lines:
            print("UWAGA: Git nie zwrócił żadnych danych (pusty wynik)!")
            return None
            
        current_author = None
        for line in lines:
            line = line.strip()
            if not line: continue
            if line.startswith('AUTHOR:'):
                current_author = line[7:]
                continue
            if current_author:
                parts = line.split('\t')
                if len(parts) >= 3:
                    added = parts[0]
                    if added == '-': continue
                    authors_stats[current_author] += int(added)
                    
        return dict(authors_stats)

    except subprocess.CalledProcessError as e:
        print(f"KRYTYCZNY BŁĄD GITA! Kod powrotu: {e.returncode}")
        print(f"Treść błędu z konsoli serwera: {e.stderr}")
        return None
    except Exception as e:
        print(f"INNY BŁĄD PYTHONA: {e}")
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("target_dir")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    stats = get_merge_stats(args.target_dir)
    
    if not stats:
        print("ZAKOŃCZONO: Brak statystyk, nie tworzę pliku.")
        sys.exit(0)

    csv_lines = []
    for author, added_lines in stats.items():
        if added_lines > 0:
            csv_lines.append(f"{author},{added_lines}")

    if not csv_lines:
        print("ZAKOŃCZONO: Zmiany były, ale 0 dodanych linii (np. usunięcia lub pliki binarne).")
        sys.exit(0)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write("author,lines_of_code\n")
            for line in csv_lines:
                f.write(line + "\n")
        print(f"SUKCES: Wygenerowano plik {args.output}")
    else:
        print("author,lines_of_code")
        for line in csv_lines:
            print(line)

if __name__ == "__main__":
    main()