import pandas as pd

from config import CALIBRATION, EXPECTED_RANGES


def clean_and_transform(df):
    df = df.drop_duplicates()
    df.dropna(inplace=True)

    # Convert timestamp to ISO format & adjust to UTC+5:30
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert('Asia/Kolkata')
    # df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')

    # Z-score calculation (but not used for dropping)
    df['mean'] = df.groupby('reading_type')['value'].transform('mean')
    df['std'] = df.groupby('reading_type')['value'].transform('std')
    df['z_score'] = (df['value'] - df['mean']) / df['std']

    # Outlier correction: replace outlier with group mean
    def correct_outlier(row):
        if abs(row['z_score']) > 3:
            return row['mean']
        return row['value']

    df.loc[:, 'value'] = df.apply(correct_outlier, axis=1)

    # Calibration
    def normalize(row):
        m, b = CALIBRATION.get(row['reading_type'], (1.0, 0.0))
        return row['value'] * m + b

    df.loc[:, 'value'] = df.apply(normalize, axis=1)

    # Anomaly detection
    df['anomalous_reading'] = df.apply(
        lambda row: not (
                    EXPECTED_RANGES[row['reading_type']][0] <= row['value'] <= EXPECTED_RANGES[row['reading_type']][1]),
        axis=1
    )

    # Daily average per sensor & reading_type
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    daily_avg = df.groupby(['sensor_id', 'reading_type', 'date'])['value'].mean().reset_index(name='daily_avg')
    df = df.merge(daily_avg, on=['sensor_id', 'reading_type', 'date'])

    # 7-day rolling average
    df['rolling_7day_avg'] = df.groupby(['sensor_id', 'reading_type'])['daily_avg'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )

    # Drop helper columns
    df.drop(columns=['mean', 'std', 'z_score'], inplace=True)

    return df
