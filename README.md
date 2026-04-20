# 🔍 Real-time Credit Card Fraud Detection Pipeline

A production-grade end-to-end fraud detection data pipeline built for PayPay Card Data Engineer interview preparation.

## Architecture

```
┌─────────────┐    ┌─────────┐    ┌───────────────────┐    ┌──────────────┐    ┌────────────┐
│ Transaction  │───▶│  Kafka  │───▶│   Flink (PyFlink) │───▶│  PostgreSQL  │───▶│ Grafana    │
│  Producer    │    │         │    │  Fraud Detection   │    │  (Alerts DB) │    │ Dashboard  │
└─────────────┘    └─────────┘    └───────────────────┘    └──────────────┘    └────────────┘
   (Python)        (3 partitions)   - Rule-based engine
                                    - Sliding Window
                                    - Stateful Processing
```

## Streaming Pipeline

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Data Source | PaySim Dataset (6M+ transactions) | Simulated mobile payment transactions |
| Message Queue | Apache Kafka | High-throughput event streaming |
| Stream Processing | Apache Flink (PyFlink) | Real-time fraud detection with windowing |
| Alert Storage | PostgreSQL | Persist fraud alerts and metrics |
| Monitoring | Grafana | Real-time fraud dashboard |

## Dataset: PaySim

Synthetic mobile money transaction dataset (~6.3M records) simulating:
- **CASH_IN** / **CASH_OUT** / **TRANSFER** / **PAYMENT** / **DEBIT**
- Fields: `step`, `type`, `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`, `nameDest`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- ~500MB disk for PaySim CSV

### 1. Download the dataset

```bash
# Option A: Use Kaggle CLI
pip install kaggle
kaggle datasets download -d ealaxi/paysim1 -p data/ --unzip

# Option B: Manual download from https://www.kaggle.com/datasets/ealaxi/paysim1
# Place CSV in data/PS_20174392719_1491204439457_log.csv
```

### 2. Start infrastructure

```bash
docker compose up -d
```

### 3. Create Kafka topic

```bash
docker exec kafka kafka-topics.sh \
  --create --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 4. Run the producer

```bash
uv run python streaming/producer/transaction_producer.py
```

### 5. Run the Flink fraud detector

```bash
docker exec flink-jobmanager flink run -py /opt/flink/jobs/fraud_detector.py
```

### 6. Open Grafana

Visit http://localhost:3000 (admin/admin) → Import dashboard from `grafana/dashboard.json`

## Fraud Detection Rules

| Rule | Description | Window |
|------|------------|--------|
| High Amount | Single transaction > 200,000 | Instant |
| Rapid Fire | >3 transactions from same account in 1 min | Sliding 60s |
| Drain Account | Total outflow > 80% of balance in 10 min | Sliding 10min |
| Type Pattern | TRANSFER immediately followed by CASH_OUT | Sliding 5min |

## Key Technical Concepts Demonstrated

### Kafka
- **Partitioning**: Keyed by `nameOrig` for account-level ordering
- **Consumer Groups**: Flink consumer group with offset management
- **acks=all**: Producer durability guarantee

### Flink
- **Sliding Windows**: Time-based aggregation for behavioral patterns
- **Stateful Processing**: Per-account state for velocity checks
- **Event Time**: Watermarks for out-of-order event handling
- **Checkpointing**: Fault tolerance with exactly-once semantics

## Project Structure

```
cc-fraud-pipeline/
├── docker-compose.yml
├── pyproject.toml
├── streaming/
│   ├── producer/
│   │   └── transaction_producer.py
│   └── flink/
│       ├── fraud_detector.py
│       └── Dockerfile
├── sql/
│   └── init.sql
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/datasource.yml
│   │   └── dashboards/dashboard.yml
│   └── dashboard.json
├── data/                              # PaySim CSV (gitignored)
└── README.md
```