import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.transformation import clean_and_transform
from src.validation import validate
from src.config import EXPECTED_RANGES


@pytest.fixture
def sample_df():
    """Create a small test dataset."""
    return pd.DataFrame({
        "sensor_id": ["s1", "s1", "s2", "s2"],
        "timestamp": [
            "2023-06-01T01:00:00Z",
            "2023-06-01T02:00:00Z",
            "2023-06-01T03:00:00Z",
            "2023-06-01T04:00:00Z",
        ],
        "reading_type": ["temperature", "temperature", "humidity", "humidity"],
        "value": [25.0, 1000.0, 50.0, np.nan],  # 1000 is an outlier, NaN included
        "battery_level": [3.8, 3.9, 4.0, 3.7],
    })


def test_clean_and_transform_removes_outliers():
    data = {
        "sensor_id": ["s1"] * 11,
        "timestamp": ["2025-01-01T00:00:00+0530"] * 11,
        "reading_type": ["temperature"] * 11,
        "value": [25.0] * 10 + [500.0],  # One clear outlier
        "battery_level": [3.7] * 11,
    }
    df = pd.DataFrame(data)
    df_transformed = clean_and_transform(df)

    # Check if outlier was corrected (Z-score > 3 would be replaced with mean)
    assert df_transformed['value'].max() < 600, "Outlier not handled properly"
    assert df_transformed['value'].iloc[-1] != 500.0, "Outlier correction did not apply"



def test_anomaly_detection(sample_df):
    """Check anomaly flag logic against EXPECTED_RANGES."""
    df_transformed = clean_and_transform(sample_df)

    assert 'anomalous_reading' in df_transformed.columns

    for _, row in df_transformed.iterrows():
        low, high = EXPECTED_RANGES[row['reading_type']]
        assert isinstance(row['anomalous_reading'], bool)
        if not (low <= row['value'] <= high):
            assert row['anomalous_reading'], f"{row['value']} not flagged as anomaly"
        else:
            assert not row['anomalous_reading'], f"{row['value']} incorrectly flagged"


def test_daily_and_rolling_avg(sample_df):
    """Check daily and 7-day rolling average creation."""
    df_transformed = clean_and_transform(sample_df)
    assert 'daily_avg' in df_transformed.columns
    assert 'rolling_7day_avg' in df_transformed.columns


def test_validate_report(sample_df):
    """Validate data quality report structure."""
    df_transformed = clean_and_transform(sample_df)
    report = validate(df_transformed)

    assert set(['reading_type', 'total', 'pct_missing', 'pct_anomalous']).issubset(report.columns)
    assert (report['pct_missing'] >= 0).all()
    assert (report['pct_anomalous'] >= 0).all()
