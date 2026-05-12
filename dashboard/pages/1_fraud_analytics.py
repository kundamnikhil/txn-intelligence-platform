import streamlit as st
import plotly.express as px
import sys
sys.path.insert(0, '/home/kundamnikhil/amex-finops-pipeline/dashboard')
from bq_client import fraud_summary, top_high_risk_transactions

st.set_page_config(page_title="Fraud Analytics", layout="wide")
st.title("Fraud Risk Analytics")
st.caption("Real-time fraud scoring across transaction categories")

with st.spinner("Loading fraud data from BigQuery..."):
    summary_df = fraud_summary()
    high_risk_df = top_high_risk_transactions()

# KPI row
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Categories", len(summary_df))
col2.metric("Avg Fraud Score", f"{summary_df['avg_fraud_score'].mean():.1f}")
col3.metric("High Risk Transactions", int(summary_df['high_risk_count'].sum()))
col4.metric("Overall Fraud Rate", f"{summary_df['high_risk_count'].sum() / summary_df['total_transactions'].sum() * 100:.2f}%")

st.divider()

# Bar chart -- fraud rate by category
st.subheader("Fraud Rate by Merchant Category")
fig1 = px.bar(
    summary_df.sort_values("fraud_rate_pct", ascending=True),
    x="fraud_rate_pct",
    y="mcc_category",
    orientation="h",
    color="fraud_rate_pct",
    color_continuous_scale="Reds",
    labels={"fraud_rate_pct": "Fraud Rate %", "mcc_category": "Category"},
)
fig1.update_layout(height=400, showlegend=False)
st.plotly_chart(fig1, use_container_width=True)

# Scatter -- avg fraud score vs total transactions
st.subheader("Fraud Score vs Transaction Volume")
fig2 = px.scatter(
    summary_df,
    x="total_transactions",
    y="avg_fraud_score",
    size="high_risk_count",
    color="mcc_category",
    hover_name="mcc_category",
    labels={
        "total_transactions": "Total Transactions",
        "avg_fraud_score": "Avg Fraud Score",
        "high_risk_count": "High Risk Count"
    },
)
fig2.update_layout(height=400)
st.plotly_chart(fig2, use_container_width=True)

# Top high risk transactions table
st.subheader("Top 50 High Risk Transactions")
st.dataframe(
    high_risk_df,
    use_container_width=True,
    height=400
)

if st.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()
