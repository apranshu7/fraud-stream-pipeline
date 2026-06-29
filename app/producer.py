import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def make_transaction():
    return {
        "transaction_id": f"txn_{random.randint(100000, 999999)}",
        "user_id": f"user_{random.randint(1, 50)}",
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "currency": "USD",
        "merchant": random.choice(["amazon", "walmart", "uber", "starbucks", "shell","flipkart"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }
if __name__ == "__main__":
    print("Producer starting at port kafka:9092, topic 'transactions'")
    try:
        while True:
            txn = make_transaction()
            producer.send("transactions", value=txn)
            print(f"sent: {txn['transaction_id']} {txn['user_id']} ${txn['amount']}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")