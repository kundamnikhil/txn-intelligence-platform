import os
import sys
import pandas as pd
from google.cloud import bigquery, storage
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from fraud_score import score_transactions
from validate import validate

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "txn-intelligence-platform")
DATASET = os.environ.get("BQ_DATASET", "txn_intelligence")
BUCKET = os.environ.get("GCS_BUCKET_NAME", "txn-intelligence-raw")

def load_to_bigquery(csv_path):
    client = bigquery.Client(project=PROJECT_ID)
    gcs_client = storage.Client(project=PROJECT_ID)
    started_at = datetime.utcnow()

    # read + validate
    df = pd.read_csv(csv_path, dtype={"mcc_code": str})
    validation = validate(df)
    if not validation["passed"]:
        raise ValueError(f"Validation failed: {validation}")

    # cast types
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["ingested_at"] = pd.Timestamp.utcnow()
    df["is_international"] = df["is_international"].astype(bool)
    df["is_fraud"] = df["is_fraud"].astype(bool)
    df["amount"] = df["amount"].astype(float)

    # load raw_transactions
    raw_table = f"{PROJECT_ID}.{DATASET}.raw_transactions"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=False,
        schema=[
            bigquery.SchemaField("transaction_id", "STRING"),
            bigquery.SchemaField("card_id", "STRING"),
            bigquery.SchemaField("merchant_id", "STRING"),
            bigquery.SchemaField("merchant_name", "STRING"),
            bigquery.SchemaField("mcc_code", "STRING"),
            bigquery.SchemaField("mcc_category", "STRING"),
            bigquery.SchemaField("amount", "FLOAT64"),
            bigquery.SchemaField("is_international", "BOOL"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("is_fraud", "BOOL"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ]
    )
    job = client.load_table_from_dataframe(df, raw_table, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows to {raw_table}")

    # score transactions
    scored_df = score_transactions(df)
    scored_df["scored_at"] = pd.Timestamp.utcnow()
    score_cols = [
        "transaction_id", "card_id", "velocity_score",
        "amount_zscore", "amount_anomaly_score",
        "international_score", "composite_fraud_score",
        "is_high_risk", "scored_at"
    ]
    scored_df["velocity_score"] = scored_df["velocity_score"].astype(int)
    scored_df["amount_anomaly_score"] = scored_df["amount_anomaly_score"].astype(int)
    scored_df["international_score"] = scored_df["international_score"].astype(int)
    scored_df["composite_fraud_score"] = scored_df["composite_fraud_score"].astype(int)
    scored_df["amount_zscore"] = scored_df["amount_zscore"].astype(float)
    scored_df["is_high_risk"] = scored_df["is_high_risk"].astype(bool)

    fraud_table = f"{PROJECT_ID}.{DATASET}.fraud_risk_scores"
    fraud_job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=False,
        schema=[
            bigquery.SchemaField("transaction_id", "STRING"),
            bigquery.SchemaField("card_id", "STRING"),
            bigquery.SchemaField("velocity_score", "INT64"),
            bigquery.SchemaField("amount_zscore", "FLOAT64"),
            bigquery.SchemaField("amount_anomaly_score", "INT64"),
            bigquery.SchemaField("international_score", "INT64"),
            bigquery.SchemaField("composite_fraud_score", "INT64"),
            bigquery.SchemaField("is_high_risk", "BOOL"),
            bigquery.SchemaField("scored_at", "TIMESTAMP"),
        ]
    )
    job2 = client.load_table_from_dataframe(
        scored_df[score_cols], fraud_table, job_config=fraud_job_config
    )
    job2.result()
    print(f"Loaded {len(scored_df)} rows to {fraud_table}")

    # log pipeline metrics
    ended_at = datetime.utcnow()
    duration_ms = int((ended_at - started_at).total_seconds() * 1000)
    metrics_df = pd.DataFrame([{
        "run_id": f"manual_{started_at.strftime('%Y%m%d_%H%M%S')}",
        "task_name": "bq_load",
        "started_at": pd.Timestamp(started_at),
        "ended_at": pd.Timestamp(ended_at),
        "duration_ms": duration_ms,
        "records_in": len(df),
        "records_out": len(df),
        "null_rate": float(validation["null_rate"]),
        "status": "success"
    }])
    metrics_table = f"{PROJECT_ID}.{DATASET}.pipeline_metrics"
    metrics_job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=False,
        schema=[
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("task_name", "STRING"),
            bigquery.SchemaField("started_at", "TIMESTAMP"),
            bigquery.SchemaField("ended_at", "TIMESTAMP"),
            bigquery.SchemaField("duration_ms", "INT64"),
            bigquery.SchemaField("records_in", "INT64"),
            bigquery.SchemaField("records_out", "INT64"),
            bigquery.SchemaField("null_rate", "FLOAT64"),
            bigquery.SchemaField("status", "STRING"),
        ]
    )
    job3 = client.load_table_from_dataframe(
        metrics_df, metrics_table, job_config=metrics_job_config
    )
    job3.result()
    print(f"Pipeline metrics logged. Duration: {duration_ms}ms")

if __name__ == "__main__":
    date_str = datetime.utcnow().strftime("%Y_%m_%d")
    load_to_bigquery(f"/tmp/transactions_{date_str}.csv")
