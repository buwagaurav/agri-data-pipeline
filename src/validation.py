import duckdb


def validate(df):
    con = duckdb.connect()
    con.register("df", df)

    # Summary of missing and anomalous values
    report = con.execute("""
        SELECT
            reading_type,
            COUNT(*) AS total,
            SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_missing,
            SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_anomalous
        FROM df
        GROUP BY reading_type
    """).fetchdf()

    # Flatten generate_series to avoid TIMESTAMP[] issue
    hourly_gaps = con.execute("""
        WITH bounds AS (
            SELECT sensor_id, reading_type,
                   MIN(timestamp) AS min_ts,
                   MAX(timestamp) AS max_ts
            FROM df
            GROUP BY sensor_id, reading_type
        ),
        expected_times AS (
            SELECT 
                b.sensor_id,
                b.reading_type,
                unnest(generate_series(b.min_ts, b.max_ts, INTERVAL 1 HOUR)) AS expected_ts
            FROM bounds b
        ),
        actual_times AS (
            SELECT sensor_id, reading_type, timestamp AS actual_ts
            FROM df
        ),
        missing AS (
            SELECT e.sensor_id, e.reading_type, e.expected_ts
            FROM expected_times e
            LEFT JOIN actual_times a
            ON e.sensor_id = a.sensor_id
            AND e.reading_type = a.reading_type
            AND e.expected_ts = a.actual_ts
            WHERE a.actual_ts IS NULL
        )
        SELECT sensor_id, reading_type, COUNT(*) AS missing_hours
        FROM missing
        GROUP BY sensor_id, reading_type
    """).fetchdf()

    # Merge with profile summary
    final_report = report.merge(hourly_gaps, on='reading_type', how='left')
    final_report['missing_hours'] = final_report['missing_hours'].fillna(0).astype(int)
    return final_report
