import os
import streamlit as st
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "txn-intelligence-platform")
DATASET = os.environ.get("BQ_DATASET", "txn_intelligence")

def get_client():
    try:
        if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"],
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    except Exception:
        pass
    return bigquery.Client(project=PROJECT_ID)

def run_query(sql):
    client = get_client()
    return client.query(sql).to_dataframe()

@st.cache_data(ttl=3600)
def fraud_summary():
    return run_query(f"""
        SELECT
            mcc_category,
            COUNT(*) as total_transactions,
            SUM(CASE WHEN is_high_risk THEN 1 ELSE 0 END) as high_risk_count,
            ROUND(AVG(composite_fraud_score), 2) as avg_fraud_score,
            ROUND(SUM(CASE WHEN is_high_risk THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as fraud_rate_pct
        FROM `{PROJECT_ID}.{DATASET}.raw_transactions` r
        JOIN `{PROJECT_ID}.{DATASET}.fraud_risk_scores` f
            USING (transaction_id)
        GROUP BY mcc_category
        ORDER BY avg_fraud_score DESC
    """)

@st.cache_data(ttl=3600)
def spend_trends():
    return run_query(f"""
        SELECT
            DATE(timestamp) as date,
            mcc_category,
            ROUND(SUM(amount), 2) as total_spend,
            COUNT(*) as transaction_count
        FROM `{PROJECT_ID}.{DATASET}.raw_transactions`
        GROUP BY DATE(timestamp), mcc_category
        ORDER BY date DESC
    """)

@st.cache_data(ttl=3600)
def top_high_risk_transactions():
    return run_query(f"""
        SELECT
            r.transaction_id,
            r.card_id,
            r.merchant_name,
            r.mcc_category,
            r.amount,
            r.country,
            r.is_international,
            f.composite_fraud_score,
            f.velocity_score,
            f.amount_anomaly_score,
            f.international_score
        FROM `{PROJECT_ID}.{DATASET}.raw_transactions` r
        JOIN `{PROJECT_ID}.{DATASET}.fraud_risk_scores` f
            USING (transaction_id)
        WHERE f.is_high_risk = TRUE
        ORDER BY f.composite_fraud_score DESC
        LIMIT 50
    """)

@st.cache_data(ttl=300)
def pipeline_metrics():
    return run_query(f"""
        SELECT
            run_id,
            task_name,
            started_at,
            ended_at,
            duration_ms,
            records_in,
            records_out,
            null_rate,
            status
        FROM `{PROJECT_ID}.{DATASET}.pipeline_metrics`
        ORDER BY started_at DESC
    """)

@st.cache_data(ttl=3600)
def ml_predictions():
    return run_query(f"""
        SELECT
            transaction_id,
            card_id,
            amount,
            composite_fraud_score,
            ml_fraud_probability,
            ml_predicted_fraud,
            rule_predicted_fraud,
            model_caught_rules_missed,
            rules_caught_model_missed,
            predicted_at
        FROM `{PROJECT_ID}.{DATASET}.ml_fraud_predictions`
        ORDER BY ml_fraud_probability DESC
        LIMIT 5000
    """)
