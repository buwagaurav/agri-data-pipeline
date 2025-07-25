import os
import sys

from ingestion import ingest_files
from loader import save
from transformation import clean_and_transform
from validation import validate

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if __name__ == "__main__":
    print("[INFO] Starting ingestion...")
    df, stats = ingest_files()
    print(f"[INFO] Ingestion stats: {stats}")

    if df.empty:
        print("[WARN] No data ingested.")
        exit(0)

    print("[INFO] Transforming data...")
    df = clean_and_transform(df)

    print("[INFO] Validating data quality...")
    report = validate(df)
    report_path = os.path.join("data", "data_quality_report.csv")
    report.to_csv(report_path, index=False)
    print(f"[INFO] Validation report saved to {report_path}")

    print("[INFO] Saving processed data...")
    save(df)

    print("[INFO] Pipeline complete ✅")
