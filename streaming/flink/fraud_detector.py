"""
Flink Fraud Detection Job (PyFlink Table API + SQL)

Reads transactions from Kafka, applies fraud detection rules, and writes
alerts to both PostgreSQL and a Kafka alerts topic.

Key concepts demonstrated:
- Flink SQL with Kafka connector (event-time processing)
- Tumbling & Sliding windows for behavioral aggregation
- Stateful pattern detection (TRANSFER → CASH_OUT)
- JDBC sink for persistent alert storage
"""

import os
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.window import Slide, Tumble


# ─── Configuration ─────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/fraud_detection")
PG_USER = os.getenv("PG_USER", "fraud_user")
PG_PASS = os.getenv("PG_PASS", "fraud_pass")


def main():
    # ─── Environment Setup ─────────────────────────────────────
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Configure checkpointing for exactly-once semantics
    t_env.get_config().set("execution.checkpointing.interval", "60000")
    t_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")
    t_env.get_config().set("state.backend.type", "hashmap")
    t_env.get_config().set("parallelism.default", "2")

    # ─── Source: Kafka transactions topic ──────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE transactions (
            transaction_id  STRING,
            `step`          INT,
            `type`          STRING,
            amount          DOUBLE,
            nameOrig        STRING,
            oldbalanceOrg   DOUBLE,
            newbalanceOrig  DOUBLE,
            nameDest        STRING,
            oldbalanceDest  DOUBLE,
            newbalanceDest  DOUBLE,
            isFraud         INT,
            isFlaggedFraud  INT,
            event_time      TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transactions',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-fraud-detector',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # ─── Sink: PostgreSQL fraud_alerts table ───────────────────
    t_env.execute_sql(f"""
        CREATE TABLE fraud_alerts_sink (
            transaction_id  STRING,
            account_orig    STRING,
            account_dest    STRING,
            txn_type        STRING,
            amount          DOUBLE,
            rule_triggered  STRING,
            risk_score      DOUBLE,
            event_time      TIMESTAMP(3),
            detected_at     TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'fraud_alerts',
            'username' = '{PG_USER}',
            'password' = '{PG_PASS}',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # ─── Sink: PostgreSQL txn_metrics table ────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE txn_metrics_sink (
            window_start    TIMESTAMP(3),
            window_end      TIMESTAMP(3),
            txn_type        STRING,
            txn_count       BIGINT,
            total_amount    DOUBLE,
            fraud_count     BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'txn_metrics',
            'username' = '{PG_USER}',
            'password' = '{PG_PASS}',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # ─── Sink: Kafka fraud-alerts topic (for downstream) ──────
    t_env.execute_sql(f"""
        CREATE TABLE fraud_alerts_kafka (
            transaction_id  STRING,
            account_orig    STRING,
            account_dest    STRING,
            txn_type        STRING,
            amount          DOUBLE,
            rule_triggered  STRING,
            risk_score      DOUBLE,
            event_time      TIMESTAMP(3),
            detected_at     TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'fraud-alerts',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # ═══════════════════════════════════════════════════════════
    # RULE 1: High Amount Detection (instant, no window)
    # Single transaction > 200,000 is suspicious
    # ═══════════════════════════════════════════════════════════
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW high_amount_alerts AS
        SELECT
            transaction_id,
            nameOrig        AS account_orig,
            nameDest        AS account_dest,
            `type`          AS txn_type,
            amount,
            'HIGH_AMOUNT'   AS rule_triggered,
            LEAST(100.0, amount / 2000.0) AS risk_score,
            event_time,
            CURRENT_TIMESTAMP AS detected_at
        FROM transactions
        WHERE amount > 200000
          AND `type` IN ('TRANSFER', 'CASH_OUT')
    """)

    # ═══════════════════════════════════════════════════════════
    # RULE 2: Rapid Fire Detection (sliding window 60s, slide 10s)
    # >3 transactions from same account in 1 minute
    # ═══════════════════════════════════════════════════════════
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW rapid_fire_alerts AS
        SELECT
            MAX(transaction_id)   AS transaction_id,
            nameOrig              AS account_orig,
            ''                    AS account_dest,
            'MULTI'               AS txn_type,
            SUM(amount)           AS amount,
            'RAPID_FIRE'          AS rule_triggered,
            LEAST(100.0, CAST(COUNT(*) AS DOUBLE) * 20.0) AS risk_score,
            MAX(event_time)       AS event_time,
            CURRENT_TIMESTAMP     AS detected_at
        FROM TABLE(
            HOP(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '10' SECOND, INTERVAL '60' SECOND)
        )
        GROUP BY nameOrig, window_start, window_end
        HAVING COUNT(*) > 3
    """)

    # ═══════════════════════════════════════════════════════════
    # RULE 3: Drain Account Detection (sliding window 10 min)
    # Total outflow > 80% of initial balance in 10 minutes
    # ═══════════════════════════════════════════════════════════
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW drain_account_alerts AS
        SELECT
            MAX(transaction_id)   AS transaction_id,
            nameOrig              AS account_orig,
            ''                    AS account_dest,
            'MULTI'               AS txn_type,
            SUM(amount)           AS amount,
            'DRAIN_ACCOUNT'       AS rule_triggered,
            90.0                  AS risk_score,
            MAX(event_time)       AS event_time,
            CURRENT_TIMESTAMP     AS detected_at
        FROM TABLE(
            HOP(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '10' MINUTE)
        )
        WHERE `type` IN ('TRANSFER', 'CASH_OUT')
        GROUP BY nameOrig, window_start, window_end
        HAVING SUM(amount) > MAX(oldbalanceOrg) * 0.8
           AND MAX(oldbalanceOrg) > 0
    """)

    # ═══════════════════════════════════════════════════════════
    # RULE 4: Type Pattern (TRANSFER → CASH_OUT within 5 min)
    # Classic fraud pattern: transfer to mule, then cash out
    # ═══════════════════════════════════════════════════════════
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW type_pattern_alerts AS
        SELECT
            t2.transaction_id,
            t1.nameOrig       AS account_orig,
            t1.nameDest       AS account_dest,
            'TRANSFER_CASHOUT' AS txn_type,
            t1.amount + t2.amount AS amount,
            'TYPE_PATTERN'    AS rule_triggered,
            85.0              AS risk_score,
            t2.event_time     AS event_time,
            CURRENT_TIMESTAMP AS detected_at
        FROM transactions t1
        JOIN transactions t2
            ON t1.nameDest = t2.nameOrig
            AND t1.`type` = 'TRANSFER'
            AND t2.`type` = 'CASH_OUT'
            AND t2.event_time BETWEEN t1.event_time AND t1.event_time + INTERVAL '5' MINUTE
            AND t2.amount >= t1.amount * 0.9
    """)

    # ═══════════════════════════════════════════════════════════
    # UNION all alerts and write to both sinks
    # ═══════════════════════════════════════════════════════════
    all_alerts = t_env.execute_sql("""
        CREATE TEMPORARY VIEW all_alerts AS
        SELECT * FROM high_amount_alerts
        UNION ALL
        SELECT * FROM rapid_fire_alerts
        UNION ALL
        SELECT * FROM drain_account_alerts
        UNION ALL
        SELECT * FROM type_pattern_alerts
    """)

    # Write alerts to PostgreSQL (StatementSet for multi-sink)
    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql("""
        INSERT INTO fraud_alerts_sink
        SELECT * FROM all_alerts
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO fraud_alerts_kafka
        SELECT * FROM all_alerts
    """)

    # ─── Transaction Metrics: tumbling 1-minute window ─────────
    stmt_set.add_insert_sql("""
        INSERT INTO txn_metrics_sink
        SELECT
            window_start,
            window_end,
            `type`          AS txn_type,
            COUNT(*)        AS txn_count,
            SUM(amount)     AS total_amount,
            SUM(isFraud)    AS fraud_count
        FROM TABLE(
            TUMBLE(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        GROUP BY window_start, window_end, `type`
    """)

    # Execute all inserts
    stmt_set.execute()

    print("[FraudDetector] Job submitted successfully")


if __name__ == "__main__":
    main()
