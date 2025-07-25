import os

import duckdb
import pandas as pd

from config import RAW_DATA_DIR

CHECKPOINT_FILE = 'checkpoint.txt'


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return set(f.read().splitlines())
    return set()


def update_checkpoint(processed_files):
    with open(CHECKPOINT_FILE, 'a') as f:
        for file in processed_files:
            f.write(file + '\n')


def ingest_files():
    processed = load_checkpoint()
    files = sorted(f for f in os.listdir(RAW_DATA_DIR) if f.endswith('.parquet') and f not in processed)

    all_data = []
    stats = []
    failed_files = []

    for file in files:
        path = os.path.join(RAW_DATA_DIR, file)
        try:
            con = duckdb.connect()
            # Schema inspection
            schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchdf()
            print(f"[SCHEMA] {file}:\n{schema}\n")

            # Read data
            df = con.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
            df['source_file'] = file

            # Validation queries
            missing_count = con.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{path}')
                WHERE sensor_id IS NULL OR reading_type IS NULL OR value IS NULL
            """).fetchone()[0]

            if missing_count > 0:
                print(f"[WARNING] File {file} has {missing_count} rows with missing values")

            # Aggregation summary
            summary = con.execute(f"""
                SELECT reading_type, COUNT(*) as count, AVG(value) as avg_value
                FROM read_parquet('{path}')
                GROUP BY reading_type
            """).fetchdf()
            print(f"[SUMMARY] {file}:\n{summary}\n")

            all_data.append(df)
            stats.append((file, len(df), missing_count, 0))  # 0 = success

        except Exception as e:
            print(f"[ERROR] Failed to process {file}: {e}")
            stats.append((file, 0, 0, 1))  # 1 = failed
            failed_files.append(file)

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        update_checkpoint([s[0] for s in stats if s[3] == 0])  # Only successful files
        return combined, stats
    else:
        return pd.DataFrame(), stats
