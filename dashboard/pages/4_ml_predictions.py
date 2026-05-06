import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
sys.path.insert(0, '/home/kundamnikhil/amex-finops-pipeline/dashboard')
from bq_client import ml_predictions

st.set_page_config(page_title="ML Fraud Predictions", layout="wide")
st.title("ML Fraud Predictions")
st.caption("GradientBoostingClassifier vs rule-based scoring -- where they agree and disagree")

with st.spinner("Loading ML predictions from BigQuery..."):
    df = ml_predictions()

# KPI row
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Scored", f"{len(df):,}")
col2.metric("ML Flagged", f"{df['ml_predicted_fraud'].sum():,}")
col3.metric("Model Caught, Rules Missed", f"{df['model_caught_rules_missed'].sum():,}")
col4.metric("Rules Caught, Model Missed", f"{df['rules_caught_model_missed'].sum():,}")

st.divider()

# Rule vs ML scatter
st.subheader("Rule-Based Score vs ML Probability")
st.caption("Top-right quadrant: both systems agree on high risk. Bottom-left: both agree on low risk. Off-diagonal: where they disagree.")

fig1 = px.scatter(
    df.sample(min(5000, len(df))),
    x="composite_fraud_score",
    y="ml_fraud_probability",
    color=df.sample(min(5000, len(df))).apply(
        lambda r: "Both flag" if r["ml_predicted_fraud"] and r["rule_predicted_fraud"]
        else "ML only" if r["model_caught_rules_missed"]
        else "Rules only" if r["rules_caught_model_missed"]
        else "Neither", axis=1
    ),
    color_discrete_map={
        "Both flag": "#EF553B",
        "ML only": "#FFA15A",
        "Rules only": "#636EFA",
        "Neither": "#CCCCCC"
    },
    opacity=0.5,
    labels={
        "composite_fraud_score": "Rule-Based Composite Score (0-100)",
        "ml_fraud_probability": "ML Fraud Probability (0-1)"
    },
)
fig1.add_hline(y=0.5, line_dash="dash", line_color="gray", annotation_text="ML threshold 0.5")
fig1.add_vline(x=60, line_dash="dash", line_color="gray", annotation_text="Rule threshold 60")
fig1.update_layout(height=500)
st.plotly_chart(fig1, use_container_width=True)

st.divider()

# Feature importance bar chart
st.subheader("Model Feature Importance")
st.caption("Which features drove fraud predictions most. Amount and behavioral deviation dominate.")

features = ["amount", "amount_zscore", "is_international", "velocity_score",
            "composite_fraud_score", "amount_anomaly_score", "international_score"]
importances = [0.7589, 0.2167, 0.0147, 0.0054, 0.0043, 0.0000, 0.0000]

feat_df = pd.DataFrame({"feature": features, "importance": importances})
feat_df = feat_df.sort_values("importance", ascending=True)

fig2 = px.bar(
    feat_df,
    x="importance",
    y="feature",
    orientation="h",
    color="importance",
    color_continuous_scale="Blues",
    labels={"importance": "Feature Importance", "feature": "Feature"},
)
fig2.update_layout(height=350, showlegend=False)
st.plotly_chart(fig2, use_container_width=True)

st.divider()

# Incremental value table
st.subheader("Transactions Model Caught, Rules Missed")
st.caption("These are the transactions the rule system would have let through. This is the business case for ML on top of rules.")

model_only = df[df["model_caught_rules_missed"] == True].sort_values(
    "ml_fraud_probability", ascending=False
).head(50)

if len(model_only) > 0:
    st.dataframe(
        model_only[["transaction_id", "card_id", "amount",
                    "composite_fraud_score", "ml_fraud_probability"]],
        use_container_width=True,
        height=400
    )
else:
    st.info("No transactions where model caught what rules missed in this sample.")

st.divider()

# ML probability distribution
st.subheader("ML Fraud Probability Distribution")
fig3 = px.histogram(
    df,
    x="ml_fraud_probability",
    nbins=50,
    color_discrete_sequence=["#2E75B6"],
    labels={"ml_fraud_probability": "ML Fraud Probability"},
)
fig3.add_vline(x=0.5, line_dash="dash", line_color="red", annotation_text="Decision threshold")
fig3.update_layout(height=300)
st.plotly_chart(fig3, use_container_width=True)
