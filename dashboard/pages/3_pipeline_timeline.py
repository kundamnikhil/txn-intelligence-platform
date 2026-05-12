import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
sys.path.insert(0, '/home/kundamnikhil/amex-finops-pipeline/dashboard')
from bq_client import pipeline_metrics

st.set_page_config(page_title="Pipeline Timeline", layout="wide")
st.title("Pipeline Observability")
st.caption("Health and performance of every pipeline run")

with st.spinner("Loading pipeline metrics from BigQuery..."):
    df = pipeline_metrics()

df["started_at"] = pd.to_datetime(df["started_at"])
df["ended_at"] = pd.to_datetime(df["ended_at"])

# KPI row
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Runs", len(df))
col2.metric("Successful Runs", len(df[df["status"] == "success"]))
col3.metric("Avg Duration", f"{df['duration_ms'].mean()/1000:.1f}s")
col4.metric("Total Records Processed", f"{df['records_in'].sum():,}")

st.divider()

# Gantt chart
st.subheader("Pipeline Run Timeline")
df["status_color"] = df["status"].map({"success": "green", "failed": "red"})
fig = px.timeline(
    df,
    x_start="started_at",
    x_end="ended_at",
    y="run_id",
    color="status",
    color_discrete_map={"success": "#00CC96", "failed": "#EF553B"},
    hover_data=["duration_ms", "records_in", "null_rate"],
    labels={"run_id": "Run ID", "status": "Status"},
)
fig.update_layout(height=max(300, len(df) * 60))
fig.update_yaxes(autorange="reversed")
st.plotly_chart(fig, use_container_width=True)

# Duration trend
st.subheader("Run Duration Over Time")
fig2 = px.line(
    df.sort_values("started_at"),
    x="started_at",
    y="duration_ms",
    markers=True,
    labels={"duration_ms": "Duration (ms)", "started_at": "Run Time"},
)
fig2.update_layout(height=300)
st.plotly_chart(fig2, use_container_width=True)

# Metrics table
st.subheader("All Pipeline Runs")
st.dataframe(
    df[["run_id", "task_name", "started_at", "duration_ms", "records_in", "null_rate", "status"]],
    use_container_width=True
)

if st.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()
