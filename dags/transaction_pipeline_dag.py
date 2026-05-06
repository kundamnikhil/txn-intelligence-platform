import os
import sys
import requests
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, '/opt/airflow/transforms')
sys.path.insert(0, '/opt/airflow/ingestion')

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "txn-intelligence-platform")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "txn-intelligence-raw")
BQ_DATASET = os.environ.get("BQ_DATASET", "txn_intelligence")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

def slack_alert(context):
    if not SLACK_WEBHOOK_URL or "placeholder" in SLACK_WEBHOOK_URL:
        print(f"ALERT: Task {context['task_instance'].task_id} failed in DAG {context['dag'].dag_id}")
        return
    message = {
        "text": f":red_circle: *Pipeline Failed*\n"
                f"*DAG*: {context['dag'].dag_id}\n"
                f"*Task*: {context['task_instance'].task_id}\n"
                f"*Run*: {context['ds']}\n"
                f"*Error*: {context.get('exception', 'Unknown')}"
    }
    requests.post(SLACK_WEBHOOK_URL, json=message)

def log_success_metrics(context):
    print(f"SUCCESS: DAG {context['dag'].dag_id} run {context['ds']} completed")

def ingest_raw(**context):
    from google.cloud import storage
    ds = context['ds'].replace('-', '_')
    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"raw/transactions_{ds}.csv")
    local_path = f"/tmp/transactions_{ds}.csv"
    if not blob.exists():
        print(f"File not in GCS for {ds}, generating fresh data")
        from generate_transactions import generate_transactions
        generate_transactions()
        bucket.blob(f"raw/transactions_{ds}.csv").upload_from_filename(local_path)
    else:
        blob.download_to_filename(local_path)
    print(f"Ingested transactions_{ds}.csv -> {local_path}")
    return local_path

def validate_data(**context):
    from validate import validate
    ds = context['ds'].replace('-', '_')
    local_path = f"/tmp/transactions_{ds}.csv"
    df = pd.read_csv(local_path, dtype={"mcc_code": str})
    report = validate(df)
    print(f"Validation report: {report}")
    if not report["passed"]:
        raise ValueError(f"Data quality check failed: {report}")
    context['ti'].xcom_push(key='null_rate', value=report["null_rate"])
    context['ti'].xcom_push(key='record_count', value=report["total_records"])
    return report

def transform_data(**context):
    from fraud_score import score_transactions
    ds = context['ds'].replace('-', '_')
    local_path = f"/tmp/transactions_{ds}.csv"
    df = pd.read_csv(local_path, dtype={"mcc_code": str})
    scored_df = score_transactions(df)
    output_path = f"/tmp/scored_{ds}.csv"
    scored_df.to_csv(output_path, index=False)
    print(f"Transformed and scored {len(scored_df)} transactions -> {output_path}")
    context['ti'].xcom_push(key='high_risk_count', value=int(scored_df['is_high_risk'].sum()))
    return output_path

def load_to_bq(**context):
    from google.cloud import bigquery
    import time
    ds = context['ds'].replace('-', '_')
    local_path = f"/tmp/transactions_{ds}.csv"
    scored_path = f"/tmp/scored_{ds}.csv"
    started_at = datetime.utcnow()

    client = bigquery.Client(project=GCP_PROJECT_ID)

    # load raw
    df = pd.read_csv(local_path, dtype={"mcc_code": str})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["ingested_at"] = pd.Timestamp.utcnow()
    df["is_international"] = df["is_international"].astype(bool)
    df["is_fraud"] = df["is_fraud"].astype(bool)
    df["amount"] = df["amount"].astype(float)

    raw_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.raw_transactions"
    raw_config = bigquery.LoadJobConfig(
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
    client.load_table_from_dataframe(df, raw_table, job_config=raw_config).result()
    print(f"Loaded {len(df)} rows to {raw_table}")

    # load fraud scores
    scored_df = pd.read_csv(scored_path, dtype={"mcc_code": str})
    scored_df["scored_at"] = pd.Timestamp.utcnow()
    scored_df["velocity_score"] = scored_df["velocity_score"].astype(int)
    scored_df["amount_anomaly_score"] = scored_df["amount_anomaly_score"].astype(int)
    scored_df["international_score"] = scored_df["international_score"].astype(int)
    scored_df["composite_fraud_score"] = scored_df["composite_fraud_score"].astype(int)
    scored_df["amount_zscore"] = scored_df["amount_zscore"].astype(float)
    scored_df["is_high_risk"] = scored_df["is_high_risk"].astype(bool)

    score_cols = [
        "transaction_id", "card_id", "velocity_score",
        "amount_zscore", "amount_anomaly_score",
        "international_score", "composite_fraud_score",
        "is_high_risk", "scored_at"
    ]
    fraud_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.fraud_risk_scores"
    fraud_config = bigquery.LoadJobConfig(
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
    client.load_table_from_dataframe(
        scored_df[score_cols], fraud_table, job_config=fraud_config
    ).result()
    print(f"Loaded {len(scored_df)} rows to {fraud_table}")

    # log pipeline metrics
    ended_at = datetime.utcnow()
    duration_ms = int((ended_at - started_at).total_seconds() * 1000)
    null_rate = context['ti'].xcom_pull(key='null_rate', task_ids='validate_data') or 0.0
    record_count = context['ti'].xcom_pull(key='record_count', task_ids='validate_data') or 0
    high_risk_count = context['ti'].xcom_pull(key='high_risk_count', task_ids='transform_data') or 0

    metrics_df = pd.DataFrame([{
        "run_id": context['run_id'],
        "task_name": "full_pipeline",
        "started_at": pd.Timestamp(started_at),
        "ended_at": pd.Timestamp(ended_at),
        "duration_ms": duration_ms,
        "records_in": record_count,
        "records_out": record_count,
        "null_rate": float(null_rate),
        "status": "success"
    }])
    metrics_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.pipeline_metrics"
    metrics_config = bigquery.LoadJobConfig(
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
    client.load_table_from_dataframe(
        metrics_df, metrics_table, job_config=metrics_config
    ).result()
    print(f"Pipeline metrics logged. Duration: {duration_ms}ms, High risk: {high_risk_count}")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": slack_alert,
}

with DAG(
    dag_id="transaction_pipeline",
    default_args=default_args,
    description="AmEx-style transaction quality monitoring and fraud scoring pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    on_success_callback=log_success_metrics,
    tags=["finops", "fraud", "transactions"],
) as dag:

    t1 = PythonOperator(
        task_id="ingest_raw",
        python_callable=ingest_raw,
    )

    t2 = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    t3 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    t4 = PythonOperator(
        task_id="load_to_bq",
        python_callable=load_to_bq,
    )

    t1 >> t2 >> t3 >> t4
