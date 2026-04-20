"""
Transaction Producer: Reads PaySim CSV and publishes to Kafka topic 'transactions'.

Key design choices for interview talking points:
- Keyed by nameOrig → same account always goes to same partition → ordering guarantee
- acks='all' → waits for all in-sync replicas → no data loss
- Simulates real-time by controlling send rate (configurable TPS)
"""

import csv
import json
import time
import uuid
import os
import sys
from datetime import datetime, timedelta

from kafka import KafkaProducer
from kafka.errors import KafkaError


# ─── Configuration ────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
CSV_PATH = os.getenv("CSV_PATH", "data/PS_20174392719_1491204439457_log.csv")
TPS = int(os.getenv("TPS", "500"))  # transactions per second
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "0"))  # 0 = unlimited


def create_producer() -> KafkaProducer:
    """Create Kafka producer with production-grade settings."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # Serialization
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Durability: wait for ALL in-sync replicas to acknowledge
        acks="all",
        # Retry on transient failures
        retries=3,
        retry_backoff_ms=100,
        # Batching for throughput (linger 5ms to accumulate batch)
        linger_ms=5,
        batch_size=32 * 1024,  # 32KB batch
        # Compression
        compression_type="snappy",
    )


def on_success(metadata):
    """Callback on successful send."""
    pass  # silent in production; enable for debugging


def on_error(exc):
    """Callback on send failure."""
    print(f"[ERROR] Failed to send message: {exc}", file=sys.stderr)


def simulate_event_time(step: int, base_time: datetime) -> str:
    """Convert PaySim step (1 step = 1 hour) to ISO timestamp."""
    return (base_time + timedelta(hours=step)).isoformat()


def main():
    print(f"[Producer] Connecting to Kafka at {KAFKA_BOOTSTRAP}")
    producer = create_producer()

    # Base time for converting step → timestamp
    base_time = datetime(2026, 4, 1, 0, 0, 0)

    if not os.path.exists(CSV_PATH):
        print(f"[ERROR] CSV not found at {CSV_PATH}")
        print("Download from: https://www.kaggle.com/datasets/ealaxi/paysim1")
        sys.exit(1)

    print(f"[Producer] Reading from {CSV_PATH}")
    print(f"[Producer] Target TPS: {TPS}")

    sent = 0
    batch_start = time.time()

    with open(CSV_PATH, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Build transaction event
            txn = {
                "transaction_id": str(uuid.uuid4()),
                "step": int(row["step"]),
                "type": row["type"],
                "amount": float(row["amount"]),
                "nameOrig": row["nameOrig"],
                "oldbalanceOrg": float(row["oldbalanceOrg"]),
                "newbalanceOrig": float(row["newbalanceOrig"]),
                "nameDest": row["nameDest"],
                "oldbalanceDest": float(row["oldbalanceDest"]),
                "newbalanceDest": float(row["newbalanceDest"]),
                "isFraud": int(row["isFraud"]),
                "isFlaggedFraud": int(row["isFlaggedFraud"]),
                "event_time": simulate_event_time(int(row["step"]), base_time),
            }

            # Send with nameOrig as key → partition by account
            future = producer.send(
                TOPIC,
                key=txn["nameOrig"],
                value=txn,
            )
            future.add_callback(on_success)
            future.add_errback(on_error)

            sent += 1

            # Rate limiting: control TPS
            if sent % TPS == 0:
                elapsed = time.time() - batch_start
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                batch_start = time.time()

                if sent % (TPS * 10) == 0:
                    print(f"[Producer] Sent {sent:,} transactions")

            if MAX_RECORDS > 0 and sent >= MAX_RECORDS:
                break

    producer.flush()
    producer.close()
    print(f"[Producer] Done. Total sent: {sent:,}")


if __name__ == "__main__":
    main()
