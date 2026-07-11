import json
import random
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def make_late_trxn():
    return {
        "transaction_id": f"txn_{random.randint(100000, 999999)}",
        "user_id": f"user_{random.randint(1, 50)}",
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "currency": "USD",
        "merchant": random.choice(["amazon", "walmart", "uber", "starbucks", "shell","flipkart"]),
        "event_time": (datetime.now(timezone.utc)-timedelta(minutes=5)).isoformat(),
    }

txn_late = make_late_trxn()
producer.send("transactions", value=txn_late)
print(f"sent: {txn_late['transaction_id']} {txn_late['user_id']} ${txn_late['amount']}")

producer.flush()
producer.close()