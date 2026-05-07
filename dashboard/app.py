import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="TXN Intelligence Platform",
    page_icon="",
    layout="wide"
)

st.title("Transaction Intelligence Platform")
st.subheader("Financial transaction monitoring, fraud risk scoring, and pipeline observability")

st.markdown("""
### What this platform does
This pipeline processes 50,000+ synthetic credit card transactions through a
Bronze/Silver/Gold medallion architecture on GCP, orchestrated by Apache Airflow.

### Pages
- **Fraud Analytics** -- fraud risk scores by merchant category, top high-risk transactions
- **Spend Trends** -- daily spend patterns, anomaly detection
- **Pipeline Timeline** -- Airflow run history, task durations, observability metrics
- **ML Predictions** -- GradientBoosting vs rule-based scoring comparison, feature importance

### Stack
`Apache Airflow` `Google Cloud Storage` `BigQuery` `Python` `Streamlit` `Scikit-learn`
""")

st.divider()
col1, col2, col3 = st.columns(3)
col1.info("Navigate using the sidebar pages")
col2.info("Data refreshes on each page load from BigQuery")
col3.info("Pipeline runs daily via Airflow scheduler")

st.caption(f"Last refreshed: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
