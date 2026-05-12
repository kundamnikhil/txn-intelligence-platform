import streamlit as st
import plotly.express as px
import pandas as pd
import sys
sys.path.insert(0, '/home/kundamnikhil/amex-finops-pipeline/dashboard')
from bq_client import spend_trends

st.set_page_config(page_title="Spend Trends", layout="wide")
st.title("Spend Trends")
st.caption("Daily transaction spend by merchant category")

with st.spinner("Loading spend data from BigQuery..."):
    df = spend_trends()

df["date"] = pd.to_datetime(df["date"])

# KPI row
col1, col2, col3 = st.columns(3)
col1.metric("Total Spend", f"${df['total_spend'].sum():,.0f}")
col2.metric("Total Transactions", f"{df['transaction_count'].sum():,}")
col3.metric("Avg Daily Spend", f"${df.groupby('date')['total_spend'].sum().mean():,.0f}")

st.divider()

# Top 5 categories by spend
top_categories = df.groupby("mcc_category")["total_spend"].sum().nlargest(5).index.tolist()
filtered_df = df[df["mcc_category"].isin(top_categories)]

st.subheader("Daily Spend by Top 5 Categories")
fig1 = px.line(
    filtered_df,
    x="date",
    y="total_spend",
    color="mcc_category",
    labels={"total_spend": "Total Spend ($)", "date": "Date", "mcc_category": "Category"},
)
fig1.update_layout(height=400)
st.plotly_chart(fig1, use_container_width=True)

# Anomaly detection -- days where spend > mean + 2std
st.subheader("Daily Spend Anomalies")
daily_total = df.groupby("date")["total_spend"].sum().reset_index()
mean_spend = daily_total["total_spend"].mean()
std_spend = daily_total["total_spend"].std()
daily_total["is_anomaly"] = daily_total["total_spend"] > mean_spend + 2 * std_spend
daily_total["color"] = daily_total["is_anomaly"].map({True: "Anomaly", False: "Normal"})

fig2 = px.bar(
    daily_total,
    x="date",
    y="total_spend",
    color="color",
    color_discrete_map={"Anomaly": "#EF553B", "Normal": "#636EFA"},
    labels={"total_spend": "Total Spend ($)", "date": "Date"},
)
fig2.add_hline(y=mean_spend + 2 * std_spend, line_dash="dash", line_color="red", annotation_text="Anomaly threshold")
fig2.update_layout(height=400)
st.plotly_chart(fig2, use_container_width=True)

# Category breakdown table
st.subheader("Spend by Category")
category_summary = df.groupby("mcc_category").agg(
    total_spend=("total_spend", "sum"),
    total_transactions=("transaction_count", "sum"),
    avg_daily_spend=("total_spend", "mean")
).round(2).sort_values("total_spend", ascending=False).reset_index()
st.dataframe(category_summary, use_container_width=True)

if st.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()
