import os
import sys
import pickle
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dashboard'))
from bq_client import run_query

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "txn-intelligence-platform")
DATASET = os.environ.get("BQ_DATASET", "txn_intelligence")

FEATURES = [
    "amount",
    "is_international",
    "velocity_score",
    "amount_zscore",
    "amount_anomaly_score",
    "international_score",
    "composite_fraud_score"
]

def load_training_data():
    return run_query(f"""
        SELECT
            r.amount,
            r.is_international,
            f.velocity_score,
            f.amount_zscore,
            f.amount_anomaly_score,
            f.international_score,
            f.composite_fraud_score,
            r.is_fraud
        FROM `{PROJECT_ID}.{DATASET}.raw_transactions` r
        JOIN `{PROJECT_ID}.{DATASET}.fraud_risk_scores` f
            USING (transaction_id)
    """)

def train_model():
    print("Loading training data from BigQuery...")
    df = load_training_data()
    print(f"Loaded {len(df)} rows. Fraud rate: {df['is_fraud'].mean()*100:.2f}%")

    X = df[FEATURES].copy()
    X["is_international"] = X["is_international"].astype(int)
    y = df["is_fraud"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    print("Training GradientBoostingClassifier...")
    model = GradientBoostingClassifier(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        random_state=42
    )
    model.fit(X_train_scaled, y_train)

    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
    y_pred = (y_pred_proba >= 0.5).astype(int)

    auc = roc_auc_score(y_test, y_pred_proba)
    print(f"\nModel Performance:")
    print(f"AUC-ROC: {auc:.4f}")
    print(classification_report(y_test, y_pred, target_names=["Legit", "Fraud"]))

    print("\nFeature Importance:")
    for feat, imp in sorted(zip(FEATURES, model.feature_importances_), key=lambda x: -x[1]):
        print(f"  {feat}: {imp:.4f}")

    model_dir = os.path.dirname(__file__)
    with open(os.path.join(model_dir, "fraud_model.pkl"), "wb") as f:
        pickle.dump({"model": model, "scaler": scaler, "features": FEATURES}, f)
    print(f"\nModel saved to models/fraud_model.pkl")
    return model, scaler

if __name__ == "__main__":
    train_model()
