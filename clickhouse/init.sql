CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS analytics;

-- сырые данные, как они пришли из Kafka
CREATE TABLE IF NOT EXISTS raw.transactions
(
    transaction_id String,
    client_id      UInt32,
    merchant       String,
    category       LowCardinality(String),
    amount         Decimal(12, 2),
    currency       LowCardinality(String),
    event_time     DateTime64(3),
    loaded_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, transaction_id);
