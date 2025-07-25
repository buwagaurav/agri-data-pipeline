## Agri Data Pipeline

A modular Python pipeline for ingesting, cleaning, validating, and storing sensor data from agricultural fields.

## Features

- Ingest `.parquet` sensor data
- Clean & calibrate readings
- Detect anomalies and outliers
- Validate data quality (missing values, gaps, type checks)
- Save results in optimized Parquet format
- Includes unit tests (pytest) for core logic

## Setup & Run

```bash
# Clone repo
git clone https://github.com/buwagaurav/agri-data-pipeline.git
cd agri-data-pipeline

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run pipeline
PYTHONPATH=. python src/main.py
