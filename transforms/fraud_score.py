import pandas as pd
import numpy as np
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def score_transactions(df):
    if df.empty:
        logger.warning("Empty dataframe passed to score_transactions, returning early")
        return df

    logger.info(f"Scoring {len(df)} transactions")
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # velocity score
    df = df.sort_values(["card_id", "timestamp"])
    df["velocity_count"] = df.groupby("card_id")["timestamp"].transform(
        lambda x: x.expanding().count()
    )
    df["velocity_score"] = pd.cut(
        df["velocity_count"],
        bins=[0, 3, 5, float("inf")],
        labels=[0, 20, 40]
    ).astype(float).fillna(0).astype(int)

    # amount anomaly score -- per card zscore
    card_stats = df.groupby("card_id")["amount"].agg(["mean", "std"]).reset_index()
    card_stats.columns = ["card_id", "card_mean", "card_std"]
    # cards with only one transaction have undefined std -- default to 1
    # so zscore equals raw deviation in dollar terms
    card_stats["card_std"] = card_stats["card_std"].fillna(1)
    df = df.merge(card_stats, on="card_id", how="left")
    df["amount_zscore"] = ((df["amount"] - df["card_mean"]) / df["card_std"]).round(2)
    df["amount_anomaly_score"] = pd.cut(
        df["amount_zscore"],
        bins=[float("-inf"), 2, 3, float("inf")],
        labels=[0, 20, 40]
    ).astype(float).fillna(0).astype(int)

    # international high value flag
    df["international_score"] = (
        (df["is_international"] == True) & (df["amount"] > 500)
    ).astype(int) * 20

    # composite score
    df["composite_fraud_score"] = (
        df["velocity_score"] +
        df["amount_anomaly_score"] +
        df["international_score"]
    ).clip(0, 100)
    df["is_high_risk"] = df["composite_fraud_score"] > 60
    df["scored_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    high_risk_count = df["is_high_risk"].sum()
    avg_score = df["composite_fraud_score"].mean()
    logger.info(f"Scoring complete. High risk: {high_risk_count} ({high_risk_count/len(df)*100:.2f}%), avg score: {avg_score:.2f}")

    return df

if __name__ == "__main__":
    df = pd.read_csv("/tmp/transactions_2026_05_05.csv")
    scored = score_transactions(df)
    print(scored[["transaction_id", "composite_fraud_score", "is_high_risk"]].head(10))
