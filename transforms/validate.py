import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate(df):
    logger.info(f"Running validation on {len(df)} records")

    report = {
        "total_records": len(df),
        "null_card_id": int(df["card_id"].isnull().sum()),
        "null_amount": int(df["amount"].isnull().sum()),
        "negative_amount": int((df["amount"] <= 0).sum()),
        "invalid_mcc": int((~df["mcc_code"].astype(str).str.match(r'^\d{4}$')).sum()),
        "future_timestamps": int((pd.to_datetime(df["timestamp"]) > datetime.utcnow()).sum()),
        "duplicate_transaction_ids": int(df["transaction_id"].duplicated().sum()),
    }

    report["passed"] = all([
        report["null_card_id"] == 0,
        report["null_amount"] == 0,
        report["negative_amount"] == 0,
        report["invalid_mcc"] == 0,
        report["future_timestamps"] == 0,
        report["duplicate_transaction_ids"] == 0,
    ])

    report["null_rate"] = round(
        (report["null_card_id"] + report["null_amount"]) / len(df), 4
    )

    for check, value in report.items():
        if check not in ["passed", "null_rate", "total_records"]:
            if value > 0:
                logger.warning(f"Validation check failed: {check} = {value}")
            else:
                logger.info(f"Validation check passed: {check}")

    logger.info(f"Validation result: passed={report['passed']}, null_rate={report['null_rate']}")
    return report

if __name__ == "__main__":
    df = pd.read_csv("/tmp/transactions_2026_05_05.csv")
    report = validate(df)
    print(report)
