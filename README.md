# Transaction Intelligence Platform

A production-grade financial transaction monitoring pipeline built on GCP.

**Live demo:** https://kundamnikhil-txn-intelligence-platform-dashboardapp-unxbvj.streamlit.app Ingests 50,000+ synthetic credit card transactions, runs data quality gates, scores fraud risk using behavioral anomaly detection, and surfaces analytics through a live Streamlit dashboard with full pipeline observability.

Built to mirror the data engineering patterns used in large-scale financial institutions medallion warehouse architecture, orchestrated batch pipeline, per-entity fraud scoring, and pipeline health tracked as first-class data.

---

## Architecture

```
Synthetic Data Generator (Faker + lognormal distribution)
        |
        v
GCS Raw Bucket  [gs://txn-intelligence-raw/raw/]
        |
        v
Apache Airflow DAG  [scheduled daily]
        |
   -----+------+----------+----------+
   |           |          |          |
ingest_raw  validate  transform  bq_load
                |
          quality gate
          (fails pipeline
           on bad data)
        |
        v
BigQuery Medallion Warehouse
        |
   -----+----------------+-----------------+
   |                     |                 |
raw_transactions   fraud_risk_scores  pipeline_metrics
  (Bronze)             (Gold)          (Observability)
        |
        v
Streamlit Dashboard
        |
   -----+----------------+------------------+
   |                     |                  |
Fraud Analytics     Spend Trends     Pipeline Timeline
                                       (Gantt chart)
```


Full Data Flow
```
Synthetic data generator (Faker + lognormal distribution)
        |
        v
GCS raw bucket  [gs://txn-intelligence-raw/raw/transactions_YYYY_MM_DD.csv]
        |
        v  (Airflow scheduler triggers at midnight)
+-----------+    +-----------+    +-------------+    +----------+    +----------+
| ingest_raw| >> |validate   | >> |transform    | >> |bq_load   | >> |ml_score  |
| GCS->tmp  |    |6 checks   |    |fraud scorer |    |3 tables  |    |(manual)  |
+-----------+    +-----------+    +-------------+    +----------+    +----------+
                       |                                   |               |
                  raises on fail                    pipeline_metrics   ml_fraud_predictions
                  Slack alert fires                 (observability)    (ML layer)
        |
        v
BigQuery Medallion Warehouse
  Bronze: raw_transactions         (partitioned DATE(timestamp), clustered card_id)
  Gold:   fraud_risk_scores        (partitioned DATE(scored_at), clustered card_id)
  Obs:    pipeline_metrics         (partitioned DATE(started_at))
  ML:     ml_fraud_predictions     (partitioned DATE(predicted_at), clustered card_id)
        |
        v
Streamlit Dashboard (Streamlit Cloud, public URL)
  Page 1: Fraud Analytics          (fraud rate by MCC, scatter, top-50 table)
  Page 2: Spend Trends             (time series, anomaly highlight)
  Page 3: Pipeline Timeline        (Gantt from pipeline_metrics)
  Page 4: ML Predictions           (rule vs ML scatter, SHAP importance, delta table)

```


## Stack

| Layer | Technology | Why |
|---|---|---|
| Orchestration | Apache Airflow 2.8 | Industry standard for batch pipeline scheduling, DAG-based task dependencies, built-in retry and alerting hooks |
| Object storage | Google Cloud Storage | Durable landing zone before warehouse load, decouples ingestion from transformation, full audit trail by date-partitioned file path |
| Warehouse | BigQuery | Columnar analytical warehouse, partitioned by date and clustered by card_id for cost-efficient fraud queries |
| Data quality | Custom pandas assertions | Quality gate before every load, fails loudly on null card IDs, negative amounts, invalid MCC codes, future timestamps |
| Fraud scoring | Rule-based composite scorer | Per-card velocity detection, behavioral amount anomaly (zscore), international high-value flag, composite 0-100 score |
| Risk model | Scikit-learn gradient boosting | Trained on fraud features, predicts is_fraud probability, SHAP feature importance for explainability |
| Dashboard | Streamlit + Plotly | Three-page analytics surface, queries BigQuery directly, pipeline health Gantt from pipeline_metrics table |
| Auth | GCP Application Default Credentials | No JSON key files anywhere near the codebase, mirrors production GKE and Cloud Run auth patterns |

---

## Why each design decision was made

### GCS as intermediate landing zone

Data lands in GCS before BigQuery, not directly from the generator. This matters because if the BigQuery load job fails, the source file still exists in GCS and Airflow can retry the load task without rerunning ingestion. Without this intermediate layer, a failed load means either lost data or a full re-generation cycle.

In payment pipelines, every transaction must be traceable from source to warehouse. GCS provides that audit trail. The `raw/transactions_YYYY_MM_DD.csv` path convention means any day's data can be replayed by date without any code changes.

### Per-card fraud scoring instead of global scoring

Amount anomaly is computed as a zscore against each card's own historical baseline, not against the full transaction population.

```python
card_stats = df.groupby("card_id")["amount"].agg(["mean", "std"]).reset_index()
df["amount_zscore"] = (df["amount"] - df["card_mean"]) / df["card_std"]
```

A $5,000 transaction from a card that regularly spends $4,000 is normal behavior. The same $5,000 from a card that averages $30 is a major anomaly. Global zscore would flag the first case as suspicious and miss the second entirely. Per-card baseline is behaviorally correct and minimizes both false positives and false negatives simultaneously.

False positive fraud flags damage cardholder trust. Over-flagging legitimate high-spend customers drives churn. Per-card scoring is how production fraud engines actually work.

### Explicit schema on every BigQuery load job

Every load job specifies full schema with `autodetect=False`.

```python
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    autodetect=False,
    schema=[
        bigquery.SchemaField("transaction_id", "STRING"),
        bigquery.SchemaField("amount", "FLOAT64"),
        ...
    ]
)
job.result()
```

`autodetect=True` infers schema from the first N rows. A feed change that sends `mcc_code` as integer instead of string silently reinterprets the column and corrupts downstream joins. Explicit schema fails loudly on the first bad file instead of silently corrupting months of data.

`job.result()` blocks until the load completes. Without it, BigQuery load jobs are async and the next Airflow task starts before data lands, producing race conditions that are extremely hard to debug because they are timing-dependent.

### Pipeline metrics as structured data

Every pipeline run writes its own health record to a `pipeline_metrics` BigQuery table.

```
run_id, task_name, started_at, ended_at, duration_ms,
records_in, records_out, null_rate, status
```

Pipeline health is data, not just logs. Logs are text you grep through manually after something breaks. A structured metrics table lets you query "show me all runs where null_rate exceeded 0.01" or "show me duration trend over 30 days" in SQL. This powers the pipeline timeline Gantt chart in the dashboard -- a visual history of every run that a non-engineer can read and that an SLA compliance report can be built from.

### Lognormal amount distribution in synthetic data

Transaction amounts are generated using `random.lognormvariate(4.5, 1.2)`, not `random.uniform()`.

Real card spend follows a lognormal distribution -- most transactions cluster at small amounts with a long right tail of large purchases. Uniform distribution produces unrealistically flat spend histograms. Lognormal produces the right skew, which makes fraud score distributions meaningful and the anomaly detection statistically valid.

### Airflow callbacks and retry logic

```python
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": slack_alert,
}
```

One retry with 2-minute delay handles transient GCP API errors which are common in free-tier environments. The failure callback fires before Airflow marks the run failed, giving operations teams time to intervene before the next scheduled run. XCom passes metadata between tasks (null_rate, record_count, high_risk_count) without file-based coupling between task functions.

---

## Fraud scoring model

Beyond rule-based scoring, a gradient boosting classifier is trained on the engineered fraud features to predict `is_fraud` probability.

**Features used:**
- `composite_fraud_score` (rule-based signal)
- `velocity_score`
- `amount_zscore`
- `amount_anomaly_score`
- `international_score`
- `is_international`
- `amount`

**Model:** `GradientBoostingClassifier` with SHAP explainability. The model outputs both a probability score and feature importance so risk analysts can understand why any transaction was flagged.

```bash
python3 models/fraud_risk_model.py
```

Outputs model performance metrics (AUC-ROC, precision, recall at 0.5 threshold) and a SHAP summary plot saved to `models/shap_summary.png`.

---

## Project structure

```
txn-intelligence-platform/
|
+-- dags/
|   +-- transaction_pipeline_dag.py    # Airflow DAG, 4 tasks, callbacks, XCom
|
+-- ingestion/
|   +-- generate_transactions.py       # Faker, lognormal amounts, MCC codes, 50k rows
|   +-- upload_to_gcs.py               # GCS client, raw bucket landing
|
+-- transforms/
|   +-- validate.py                    # Quality gate, 6 assertions, returns report dict
|   +-- fraud_score.py                 # Per-card zscore, velocity, international scoring
|   +-- bq_loader.py                   # Explicit schema BQ load, pipeline metrics logging
|   +-- sql/
|       +-- create_tables.sql          # DDL, partitioned and clustered BQ tables
|
+-- models/
|   +-- fraud_risk_model.py            # GradientBoostingClassifier, SHAP explainability
|
+-- dashboard/
|   +-- app.py                         # Streamlit entrypoint
|   +-- bq_client.py                   # Shared BQ query helper
|   +-- pages/
|       +-- 1_fraud_analytics.py       # Fraud rate by MCC, scatter, top-50 high risk table
|       +-- 2_spend_trends.py          # Daily spend time series, anomaly highlighting
|       +-- 3_pipeline_timeline.py     # Gantt from pipeline_metrics, duration trend
|
+-- docker-compose.yml                 # Airflow local: webserver, scheduler, postgres
+-- requirements.txt
+-- .env.example
+-- .gitignore
+-- README.md
```

---

## Local setup

### Prerequisites
- Docker Desktop with WSL2 backend
- GCP account with BigQuery and Cloud Storage APIs enabled
- gcloud CLI installed and authenticated

### Step 1: Authenticate to GCP

```bash
gcloud auth application-default login --no-browser
gcloud config set project txn-intelligence-platform
```

No service account keys needed. ADC credentials are picked up automatically by all GCP client libraries.

### Step 2: Create GCP resources

In GCP console:
- Cloud Storage: create bucket `txn-intelligence-raw` in `us-central1`
- BigQuery: create dataset `txn_intelligence` in `us-central1`
- Run `transforms/sql/create_tables.sql` in BigQuery console to create tables

### Step 3: Configure environment

```bash
cp .env.example .env
# edit .env and fill in your GCP_PROJECT_ID and GCS_BUCKET_NAME
```

### Step 4: Start Airflow

```bash
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler -d
```

Open `http://localhost:8080`, login with `admin/admin`.

### Step 5: Generate and load data

```bash
python3 ingestion/generate_transactions.py
GCP_PROJECT_ID=your-project-id GCS_BUCKET_NAME=txn-intelligence-raw python3 ingestion/upload_to_gcs.py
```

Or trigger the DAG manually in the Airflow UI -- it handles both steps automatically.

### Step 6: Run the dashboard

```bash
GCP_PROJECT_ID=your-project-id BQ_DATASET=txn_intelligence streamlit run dashboard/app.py
```

### Step 7: Train the fraud risk model

```bash
GCP_PROJECT_ID=your-project-id BQ_DATASET=txn_intelligence python3 models/fraud_risk_model.py
```

---

## BigQuery table schemas

### raw_transactions (Bronze)
Partitioned by `DATE(timestamp)`, clustered by `card_id`

| Field | Type | Description |
|---|---|---|
| transaction_id | STRING | UUID |
| card_id | STRING | Anonymized card identifier |
| merchant_name | STRING | Faker-generated merchant |
| mcc_code | STRING | 4-digit Merchant Category Code |
| mcc_category | STRING | Human-readable MCC label |
| amount | FLOAT64 | Transaction amount, lognormal distribution |
| is_international | BOOL | Country != US |
| country | STRING | 85% US, 15% international |
| timestamp | TIMESTAMP | Up to 90 days back |
| is_fraud | BOOL | Ground truth label, 2% rate |

### fraud_risk_scores (Gold)
Partitioned by `DATE(scored_at)`, clustered by `card_id`

| Field | Type | Description |
|---|---|---|
| velocity_score | INT64 | 0/20/40 based on card transaction frequency |
| amount_zscore | FLOAT64 | Per-card behavioral deviation |
| amount_anomaly_score | INT64 | 0/20/40 based on zscore thresholds |
| international_score | INT64 | 0/20 based on international + high value |
| composite_fraud_score | INT64 | Sum of components, capped 0-100 |
| is_high_risk | BOOL | composite_fraud_score > 60 |

### pipeline_metrics (Observability)
Partitioned by `DATE(started_at)`

| Field | Type | Description |
|---|---|---|
| run_id | STRING | Airflow run ID |
| duration_ms | INT64 | End-to-end pipeline duration |
| records_in | INT64 | Rows ingested |
| null_rate | FLOAT64 | Fraction of null fields detected |
| status | STRING | success / failed |

---

## What this would look like at production scale

The pipeline architecture is cloud-agnostic by design. Scaling to production Fintech-level transaction volume means swapping specific components without changing the orchestration logic:

- `generate_transactions.py` replaced by Pub/Sub subscription consuming real card network feeds
- `bq_loader.py` replaced by a Dataflow streaming job for sub-second latency
- `validate.py` assertions replaced by a Great Expectations suite with data docs published to GCS
- `fraud_score.py` rule engine replaced by a served ML model on Vertex AI with the same input contract
- Airflow on Docker replaced by Cloud Composer with auto-scaling workers

The DAG structure, medallion table design, explicit schemas, and pipeline metrics pattern carry over unchanged. That's the point the architecture decisions made here are the same ones you make at scale, just with different execution engines.

---

## To Summarize we:

Built an end-to-end financial transaction monitoring pipeline processing 50K+ synthetic credit card events through a Bronze/Silver/Gold medallion architecture, orchestrated via Airflow DAGs with data quality gates, velocity-based fraud scoring, and failure alerting hooks, surfaced through a Streamlit observability dashboard tracking pipeline health, spend anomalies, and fraud risk scores across merchant categories.

## ML Fraud Risk Model

A GradientBoostingClassifier trained on 450k transactions that predicts fraud probability alongside the existing rule-based scorer. The model adds a second opinion on top of the rules, catching non-obvious feature interactions the velocity and zscore rules miss.

**Model performance:**
- AUC-ROC: 0.7487
- Training data: 450k transactions, 1.96% fraud rate
- Features: amount, amount_zscore, velocity_score, international flags, composite_fraud_score

**Why gradient boosting over logistic regression:**
Amount and amount_zscore together explain 97% of feature importance. Gradient boosting handles this kind of feature dominance better than linear models without requiring manual interaction terms.

**Incremental value:**
1,080 transactions the model flagged that the rule system let through. Rules catch obvious patterns (high velocity, large international amounts). The model catches moderate signals that combine to be suspicious without individually crossing any rule threshold.

**How it fits the pipeline:**
Model trains offline, saves to fraud_model.pkl. score_predictions.py loads the artifact, scores all transactions, writes results to ml_fraud_predictions in BigQuery. Dashboard reads from that table. Model never runs inside Streamlit.

```bash
# train model
python3 models/fraud_risk_model.py

# score transactions and write to BigQuery
python3 models/score_predictions.py
```

**Dashboard page 4** shows rule vs ML comparison scatter, feature importance bar chart, and the incremental catch table.
