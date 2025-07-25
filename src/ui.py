import streamlit as st
import pandas as pd
import duckdb
import os

from src.transformation import clean_and_transform
from src.validation import validate
from src.config import EXPECTED_RANGES, PROCESSED_DATA_DIR

st.set_page_config(page_title="Agri Sensor Dashboard", layout="wide")
st.title(" Agricultural Sensor Data Dashboard")

st.sidebar.header("📂 Upload Raw Data")
uploaded_file = st.sidebar.file_uploader("Upload a raw Parquet file", type=["parquet"])

if uploaded_file:
    df = pd.read_parquet(uploaded_file)
    st.subheader("Raw Data Preview")
    st.dataframe(df.head())

    st.markdown("---")
    st.subheader(" Reading Type Distribution")
    st.bar_chart(df['reading_type'].value_counts())

    if st.button(" Run Transformation & Validation"):
        df_transformed = clean_and_transform(df)
        report = validate(df_transformed)

        st.success("Pipeline complete ")

        st.subheader(" Data Quality Report")
        st.dataframe(report)

        st.subheader(" Transformed Sample")
        st.dataframe(df_transformed.head())

        # Save to processed directory
        date = pd.to_datetime(df_transformed['timestamp']).dt.date.min()
        out_path = os.path.join(PROCESSED_DATA_DIR, f"{date}.parquet")
        df_transformed.to_parquet(out_path, index=False)
        st.info(f"Saved processed file to: {out_path}")

st.sidebar.markdown("---")
st.sidebar.subheader(" Browse Processed Files")

if os.path.exists(PROCESSED_DATA_DIR):
    files = [f for f in os.listdir(PROCESSED_DATA_DIR) if f.endswith(".parquet")]
    selected_file = st.sidebar.selectbox("Select processed file", files)

    if selected_file:
        df_proc = pd.read_parquet(os.path.join(PROCESSED_DATA_DIR, selected_file))
        st.subheader(f" {selected_file} - Processed Data")
        st.dataframe(df_proc.head(50))

        if st.checkbox("Run DuckDB Query"):
            query = st.text_area("Enter SQL query (DuckDB syntax)", "SELECT reading_type, AVG(value) as avg_val FROM df_proc GROUP BY reading_type")
            try:
                result = duckdb.query(query).to_df()
                st.dataframe(result)
            except Exception as e:
                st.error(f"Query failed: {e}")
