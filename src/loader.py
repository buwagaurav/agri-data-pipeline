import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import PROCESSED_DATA_DIR


def save(df):
    df['date'] = pd.to_datetime(df['timestamp']).dt.date.astype(str)

    for (date, sensor_id), group in df.groupby(['date', 'sensor_id']):
        partition_dir = os.path.join(PROCESSED_DATA_DIR, f"date={date}", f"sensor_id={sensor_id}")
        os.makedirs(partition_dir, exist_ok=True)
        output_path = os.path.join(partition_dir, "part.parquet")
        table = pa.Table.from_pandas(group)
        pq.write_table(table, output_path, compression='snappy')
