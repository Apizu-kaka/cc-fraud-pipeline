-- ============================================================
-- Fraud Detection Database Schema
-- ============================================================

-- Fraud alerts table: stores every detected fraud event
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id              BIGSERIAL PRIMARY KEY,
    transaction_id  VARCHAR(64) NOT NULL,
    account_orig    VARCHAR(32) NOT NULL,
    account_dest    VARCHAR(32) NOT NULL,
    txn_type        VARCHAR(16) NOT NULL,
    amount          NUMERIC(15, 2) NOT NULL,
    rule_triggered  VARCHAR(64) NOT NULL,
    risk_score      NUMERIC(5, 2) NOT NULL,
    event_time      TIMESTAMP NOT NULL,
    detected_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    details         JSONB
);

-- Index for Grafana time-series queries
CREATE INDEX idx_fraud_alerts_detected_at ON fraud_alerts (detected_at);
CREATE INDEX idx_fraud_alerts_account ON fraud_alerts (account_orig);
CREATE INDEX idx_fraud_alerts_rule ON fraud_alerts (rule_triggered);

-- Transaction metrics: aggregated stats per window
CREATE TABLE IF NOT EXISTS txn_metrics (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    txn_type        VARCHAR(16) NOT NULL,
    txn_count       BIGINT NOT NULL,
    total_amount    NUMERIC(18, 2) NOT NULL,
    fraud_count     BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_txn_metrics_window ON txn_metrics (window_start, window_end);
