import pandas as pd
from datetime import datetime

def validate(df):
    report = {
        "total_records": len(df),
        "null_card_id": int(df["card_id"].isnull().sum()),
        "null_amount": int(df["amount"].isnull().sum()),
        "negative_amount": int((df["amount"] <= 0).sum()),
        "invalid_mcc": int((~df["mcc_code"].astype(str).str.match(r'^\d{4}$')).sum()),
        "future_timestamps": int((pd.to_datetime(df["timestamp"]) > datetime.utcnow()).sum()),
    }
    report["passed"] = all([
        report["null_card_id"] == 0,
        report["null_amount"] == 0,
        report["negative_amount"] == 0,
        report["invalid_mcc"] == 0,
        report["future_timestamps"] == 0
    ])
    report["null_rate"] = round(
        (report["null_card_id"] + report["null_amount"]) / len(df), 4
    )
    return report

if __name__ == "__main__":
    df = pd.read_csv("/tmp/transactions_2026_05_05.csv")
    report = validate(df)
    print(report)
