# In this mini-project, you will:
# Use text processing techniques in Python to make sense of logs
# Learn where logs are located in Airflow
# Learn how to monitor automated Airflow DAGs to ensure they are working properly

import sys
from pathlib import Path
import re

# Can detect "error" or "ERROR"
ERROR_PATTERN = re.compile(r'\bERROR\b|\"level\"\s*:\s*\"error\"', re.IGNORECASE)

def analyze_file(file):
    """
    Parse one log file and return:
    - number of error entries
    - list of error lines
    """
    count = 0
    error_list = []
    with open(file, "r", errors="ignore") as f:
        for line in f:
            if ERROR_PATTERN.search(line):
                count += 1
                error_list.append(line.strip())
    return count, error_list

if __name__ == "__main__":
    # Step 2.1: Get the root log directory
    if len(sys.argv) > 1:
        log_dir = sys.argv[1]
    else:
        # default to ~/airflow/logs if not provided
        log_dir = str(Path.home() / "airflow" / "logs")

    # Collect all log files
    file_list = Path(log_dir).rglob("*.log")

    total_count = 0
    all_errors = []

    # Step 2.2: Parse each file
    for file in file_list:
        count, cur_list = analyze_file(file)
        total_count += count
        all_errors.extend(cur_list)

    # Step 2.3: Print cumulative info
    print(f"Total number of errors: {total_count}\n")
    if total_count > 0:
        print("Here are all the errors:\n")
        for err in all_errors:
            print(err + "\n") 
