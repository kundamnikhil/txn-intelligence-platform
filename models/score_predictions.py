import os
import sys
import pickle
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dashboard'))
from bq_client import run_query

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "txn-intelligence-platform")
DATASET = os.environ.get("BQ_DATASET", "txn_intelligence")

def load_model():
    model_path = os.path.join(os.path.dirname(__file__), "fraud_model.pkl")
    with open(model_path, "rb") as f:
        return pickle.load(f)

def score_and_write():
    print("Loading model...")
    artifact = load_model()
    model = artifact["model"]
    scaler = artifact["scaler"]
    features = artifact["features"]

    print("Loading features from BigQuery...")
    df = run_query(f"""
        SELECT
            r.transaction_id,
            r.card_id,
            r.amount,
            r.is_international,
            f.velocity_score,
            f.amount_zscore,
            f.amount_anomaly_score,
            f.international_score,
            f.composite_fraud_score,
            f.is_high_risk
        FROM `{PROJECT_ID}.{DATASET}.raw_transactions` r
        JOIN `{PROJECT_ID}.{DATASET}.fraud_risk_scores` f
            USING (transaction_id)
    """)

    print(f"Scoring {len(df)} transactions...")
    X = df[features].copy()
    X["is_international"] = X["is_international"].astype(int)
    X_scaled = scaler.transform(X)

    df["ml_fraud_probability"] = model.predict_proba(X_scaled)[:, 1].round(4)
    df["ml_predicted_fraud"] = df["ml_fraud_probability"] >= 0.5
    df["rule_predicted_fraud"] = df["is_high_risk"].astype(bool)
    df["model_caught_rules_missed"] = df["ml_predicted_fraud"] & ~df["rule_predicted_fraud"]
    df["rules_caught_model_missed"] = df["rule_predicted_fraud"] & ~df["ml_predicted_fraud"]
    df["predicted_at"] = pd.Timestamp.utcnow()

    output_cols = [
        "transaction_id", "card_id", "amount", "composite_fraud_score",
        "ml_fraud_probability", "ml_predicted_fraud", "rule_predicted_fraud",
        "model_caught_rules_missed", "rules_caught_model_missed", "predicted_at"
    ]

    out_df = df[output_cols].copy()
    out_df["ml_predicted_fraud"] = out_df["ml_predicted_fraud"].astype(bool)
    out_df["rule_predicted_fraud"] = out_df["rule_predicted_fraud"].astype(bool)
    out_df["model_caught_rules_missed"] = out_df["model_caught_rules_missed"].astype(bool)
    out_df["rules_caught_model_missed"] = out_df["rules_caught_model_missed"].astype(bool)

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.ml_fraud_predictions"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        schema=[
            bigquery.SchemaField("transaction_id", "STRING"),
            bigquery.SchemaField("card_id", "STRING"),
            bigquery.SchemaField("amount", "FLOAT64"),
            bigquery.SchemaField("composite_fraud_score", "INT64"),
            bigquery.SchemaField("ml_fraud_probability", "FLOAT64"),
            bigquery.SchemaField("ml_predicted_fraud", "BOOL"),
            bigquery.SchemaField("rule_predicted_fraud", "BOOL"),
            bigquery.SchemaField("model_caught_rules_missed", "BOOL"),
            bigquery.SchemaField("rules_caught_model_missed", "BOOL"),
            bigquery.SchemaField("predicted_at", "TIMESTAMP"),
        ]
    )
    client.load_table_from_dataframe(out_df, table_id, job_config=job_config).result()

    model_caught = out_df["model_caught_rules_missed"].sum()
    rules_caught = out_df["rules_caught_model_missed"].sum()
    print(f"Written {len(out_df)} predictions to {table_id}")
    print(f"Model caught, rules missed: {model_caught}")
    print(f"Rules caught, model missed: {rules_caught}")

if __name__ == "__main__":
    score_and_write()
