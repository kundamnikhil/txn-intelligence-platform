CREATE TABLE IF NOT EXISTS `txn-intelligence-platform.txn_intelligence.raw_transactions` (
  transaction_id STRING NOT NULL,
  card_id STRING NOT NULL,
  merchant_id STRING,
  merchant_name STRING,
  mcc_code STRING,
  mcc_category STRING,
  amount FLOAT64,
  is_international BOOL,
  country STRING,
  timestamp TIMESTAMP,
  is_fraud BOOL,
  ingested_at TIMESTAMP
)
PARTITION BY DATE(timestamp)
CLUSTER BY card_id;

CREATE TABLE IF NOT EXISTS `txn-intelligence-platform.txn_intelligence.fraud_risk_scores` (
  transaction_id STRING NOT NULL,
  card_id STRING NOT NULL,
  velocity_score INT64,
  amount_zscore FLOAT64,
  amount_anomaly_score INT64,
  international_score INT64,
  composite_fraud_score INT64,
  is_high_risk BOOL,
  scored_at TIMESTAMP
)
PARTITION BY DATE(scored_at)
CLUSTER BY card_id;

CREATE TABLE IF NOT EXISTS `txn-intelligence-platform.txn_intelligence.pipeline_metrics` (
  run_id STRING,
  task_name STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_ms INT64,
  records_in INT64,
  records_out INT64,
  null_rate FLOAT64,
  status STRING
)
PARTITION BY DATE(started_at);
