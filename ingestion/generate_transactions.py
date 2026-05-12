import pandas as pd
import uuid
import random
import argparse
from faker import Faker
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()

MCC_CODES = {
    "5411": "Grocery Stores",
    "5812": "Restaurants",
    "5541": "Gas Stations",
    "5912": "Pharmacies",
    "4111": "Transportation",
    "5732": "Electronics",
    "5941": "Sporting Goods",
    "7011": "Hotels",
    "4511": "Airlines",
    "5999": "Misc Retail"
}

COUNTRIES = ["US"] * 85 + ["UK", "CA", "MX", "IN", "DE", "FR", "AU", "JP", "BR"]

def generate_transactions(n=50000, for_date=None):
    target_date = for_date or datetime.utcnow().date()
    logger.info(f"Generating {n} transactions for {target_date}")

    records = []
    card_ids = [str(uuid.uuid4())[:8] for _ in range(500)]
    merchant_ids = [str(uuid.uuid4())[:8] for _ in range(200)]
    mcc_list = list(MCC_CODES.keys())

    for _ in range(n):
        card_id = random.choice(card_ids)
        mcc_code = random.choice(mcc_list)
        country = random.choice(COUNTRIES)
        is_international = country != "US"
        amount = round(random.lognormvariate(4.5, 1.2), 2)
        amount = max(1.0, min(amount, 15000.0))
        # all transactions timestamped to today with random hour
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        timestamp = datetime.combine(target_date, datetime.min.time()).replace(
            hour=hour, minute=minute
        )
        is_fraud = random.random() < 0.02

        records.append({
            "transaction_id": str(uuid.uuid4()),
            "card_id": card_id,
            "merchant_id": random.choice(merchant_ids),
            "merchant_name": fake.company(),
            "mcc_code": mcc_code,
            "mcc_category": MCC_CODES[mcc_code],
            "amount": amount,
            "is_international": is_international,
            "country": country,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "is_fraud": is_fraud
        })

    df = pd.DataFrame(records)
    date_str = target_date.strftime("%Y_%m_%d")
    filename = f"transactions_{date_str}.csv"
    output_path = f"/tmp/{filename}"
    df.to_csv(output_path, index=False)

    logger.info(f"Generated {n} transactions -> {output_path}")
    logger.info(f"Fraud rate: {df['is_fraud'].mean()*100:.2f}%")
    logger.info(f"International rate: {df['is_international'].mean()*100:.2f}%")
    return filename

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data")
    parser.add_argument("--rows", type=int, default=50000, help="Number of rows to generate")
    parser.add_argument("--date", type=str, default=None, help="Target date YYYY-MM-DD (default: today)")
    args = parser.parse_args()
    target = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None
    generate_transactions(n=args.rows, for_date=target)
