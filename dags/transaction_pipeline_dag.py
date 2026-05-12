import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, '/opt/airflow/transforms')
sys.path.insert(0, '/opt/airflow/ingestion')
sys.path.insert(0, '/opt/airflow/models')

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "txn-intelligence-platform")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "txn-intelligence-raw")
BQ_DATASET = os.environ.get("BQ_DATASET", "txn_intelligence")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

def validate_env():
    missing = [v for v in ["GCP_PROJECT_ID", "GCS_BUCKET_NAME", "BQ_DATASET"]
               if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {missing}")

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

def check_idempotency(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT_ID)
    ds = context['ds']
    query = f"""
        SELECT COUNT(*) as cnt
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_transactions`
        WHERE DATE(ingested_at) = '{ds}'
    """
    result = client.query(query).result()
    count = list(result)[0].cnt
    if count > 0:
        print(f"Idempotency check: {count} rows already exist for {ds}. Skipping load.")
        context['ti'].xcom_push(key='skip_load', value=True)
    else:
        print(f"Idempotency check: no rows for {ds}. Proceeding with load.")
        context['ti'].xcom_push(key='skip_load', value=False)

def ingest_raw(**context):
    from google.cloud import storage
    from generate_transactions import generate_transactions
    from upload_to_gcs import upload_to_gcs

    skip = context['ti'].xcom_pull(key='skip_load', task_ids='check_idempotency')
    if skip:
        print("Skipping ingest -- data already loaded for today")
        return None

    ds = context['ds'].replace('-', '_')
    local_path = f"/tmp/transactions_{ds}.csv"

    print(f"Generating fresh transactions for {ds}")
    generate_transactions()

    gcs_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = gcs_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"raw/transactions_{ds}.csv")
    blob.upload_from_filename(local_path)
    print(f"Uploaded to gs://{GCS_BUCKET_NAME}/raw/transactions_{ds}.csv")
    return local_path

def validate_data(**context):
    from validate import validate

    skip = context['ti'].xcom_pull(key='skip_load', task_ids='check_idempotency')
    if skip:
        print("Skipping validation -- data already loaded for today")
        return None

    ds = context['ds'].replace('-', '_')
    local_path = f"/tmp/transactions_{ds}.csv"
    df = pd.read_csv(local_path, dtype={"mcc_code": str})
    report = validate(df)
    print(f"Validation report: {report}")

    if not report["passed"]:
        # write failed records to dead letter table
        bad_rows = df[df["card_id"].isnull() | (df["amount"] <= 0)]
        if len(bad_rows) > 0:
            from google.cloud import bigquery
            client = bigquery.Client(project=GCP_PROJECT_ID)
            bad_rows["failed_at"] = pd.Timestamp.utcnow()
            bad_rows["failure_reason"] = "validation_failed"
            bad_rows["run_id"] = context['run_id']
            dead_letter_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.dead_letter_queue"
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=True
            )
            client.load_table_from_dataframe(
                bad_rows, dead_letter_table, job_config=job_config
            ).result()
            print(f"Written {len(bad_rows)} failed records to dead_letter_queue")
        raise ValueError(f"Data quality check failed: {report}")

    context['ti'].xcom_push(key='null_rate', value=report["null_rate"])
    context['ti'].xcom_push(key='record_count', value=report["total_records"])
    return report

def transform_data(**context):
    from fraud_score import score_transactions

    skip = context['ti'].xcom_pull(key='skip_load', task_ids='check_idempotency')
    if skip:
        print("Skipping transform -- data already loaded for today")
        return None

    ds = context['ds'].replace('-', '_')
    local_path = f"/tmp/transactions_{ds}.csv"
    df = pd.read_csv(local_path, dtype={"mcc_code": str})
    scored_df = score_transactions(df)
    output_path = f"/tmp/scored_{ds}.csv"
    scored_df.to_csv(output_path, index=False)
    print(f"Transformed {len(scored_df)} transactions -> {output_path}")
    context['ti'].xcom_push(key='high_risk_count', value=int(scored_df['is_high_risk'].sum()))
    return output_path

def load_to_bq(**context):
    from google.cloud import bigquery

    skip = context['ti'].xcom_pull(key='skip_load', task_ids='check_idempotency')
    if skip:
        print("Skipping BQ load -- data already loaded for today")
        return

    ds = context['ds'].replace('-', '_')
    local_path = f"/tmp/transactions_{ds}.csv"
    scored_path = f"/tmp/scored_{ds}.csv"
    started_at = datetime.utcnow()

    client = bigquery.Client(project=GCP_PROJECT_ID)

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
    job = client.load_table_from_dataframe(df, raw_table, job_config=raw_config)
    job.result()
    bytes_processed = job.output_bytes if hasattr(job, 'output_bytes') else 0
    print(f"Loaded {len(df)} rows to {raw_table}")

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

def ml_score(**context):
    import pickle
    from google.cloud import bigquery

    skip = context['ti'].xcom_pull(key='skip_load', task_ids='check_idempotency')
    if skip:
        print("Skipping ML scoring -- data already loaded for today")
        return

    model_path = "/opt/airflow/models/fraud_model.pkl"
    if not os.path.exists(model_path):
        print("Model artifact not found at /opt/airflow/models/fraud_model.pkl -- skipping ML score")
        return

    with open(model_path, "rb") as f:
        artifact = pickle.load(f)
    model = artifact["model"]
    scaler = artifact["scaler"]
    features = artifact["features"]

    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT
            r.transaction_id, r.card_id, r.amount, r.is_international,
            f.velocity_score, f.amount_zscore, f.amount_anomaly_score,
            f.international_score, f.composite_fraud_score, f.is_high_risk
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_transactions` r
        JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.fraud_risk_scores` f
            USING (transaction_id)
        WHERE DATE(r.ingested_at) = '{context['ds']}'
    """
    df = client.query(query).to_dataframe()
    if df.empty:
        print("No new records to score today")
        return

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
    for col in ["ml_predicted_fraud", "rule_predicted_fraud",
                "model_caught_rules_missed", "rules_caught_model_missed"]:
        out_df[col] = out_df[col].astype(bool)

    ml_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.ml_fraud_predictions"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
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
    client.load_table_from_dataframe(out_df, ml_table, job_config=job_config).result()
    print(f"ML scored {len(out_df)} transactions for {context['ds']}")
    print(f"Model caught rules missed: {out_df['model_caught_rules_missed'].sum()}")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": slack_alert,
}

validate_env()

with DAG(
    dag_id="transaction_pipeline",
    default_args=default_args,
    description="Financial transaction quality monitoring and fraud scoring pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    on_success_callback=log_success_metrics,
    tags=["finops", "fraud", "transactions"],
) as dag:

    t0 = PythonOperator(
        task_id="check_idempotency",
        python_callable=check_idempotency,
    )

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

    t5 = PythonOperator(
        task_id="ml_score",
        python_callable=ml_score,
    )

    t0 >> t1 >> t2 >> t3 >> t4 >> t5
