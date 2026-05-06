import pandas as pd
import numpy as np
from datetime import datetime

def score_transactions(df):
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # velocity score -- same card more than 3 times in 1 hour
    df = df.sort_values(["card_id", "timestamp"])
    df["velocity_count"] = df.groupby("card_id")["timestamp"].transform(
        lambda x: x.expanding().count()
    )
    df["velocity_score"] = pd.cut(
        df["velocity_count"],
        bins=[0, 3, 5, float("inf")],
        labels=[0, 20, 40]
    ).astype(float).fillna(0).astype(int)

    # amount anomaly score -- zscore per card
    card_stats = df.groupby("card_id")["amount"].agg(["mean", "std"]).reset_index()
    card_stats.columns = ["card_id", "card_mean", "card_std"]
    card_stats["card_std"] = card_stats["card_std"].fillna(1)
    df = df.merge(card_stats, on="card_id", how="left")
    df["amount_zscore"] = ((df["amount"] - df["card_mean"]) / df["card_std"]).round(2)
    df["amount_anomaly_score"] = pd.cut(
        df["amount_zscore"],
        bins=[float("-inf"), 2, 3, float("inf")],
        labels=[0, 20, 40]
    ).astype(float).fillna(0).astype(int)

    # international score
    df["international_score"] = ((df["is_international"] == True) & (df["amount"] > 500)).astype(int) * 20

    # composite score
    df["composite_fraud_score"] = (
        df["velocity_score"] +
        df["amount_anomaly_score"] +
        df["international_score"]
    ).clip(0, 100)
    df["is_high_risk"] = df["composite_fraud_score"] > 60
    df["scored_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    print(f"Scored {len(df)} transactions")
    print(f"High risk: {df['is_high_risk'].sum()} ({df['is_high_risk'].mean()*100:.2f}%)")
    print(f"Avg fraud score: {df['composite_fraud_score'].mean():.2f}")
    return df

if __name__ == "__main__":
    df = pd.read_csv("/tmp/transactions_2026_05_05.csv")
    scored = score_transactions(df)
    print(scored[["transaction_id", "composite_fraud_score", "is_high_risk"]].head(10))
