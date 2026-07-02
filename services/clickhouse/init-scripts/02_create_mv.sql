-- =============================================================================
-- 02_create_mv.sql
-- Materialized Views: bridge Kafka → raw table + pre-aggregation tables
-- =============================================================================

-- ── MV 1: Kafka Queue → Raw MergeTree ─────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_pos
TO pos_transactions
AS
SELECT
    JSONExtractString(raw, 'payload', 'transaction_id') as transaction_id,
    JSONExtractString(raw, 'payload', 'pos_id') as pos_id,
    JSONExtractString(raw, 'payload', 'product_id') as product_id,
    JSONExtractString(raw, 'payload', 'product_name') as product_name,
    JSONExtractString(raw, 'payload', 'category') as category,
    JSONExtractUInt(raw, 'payload', 'quantity') as quantity,
    JSONExtract(raw, 'payload', 'unit_price', 'Decimal(15, 2)') as unit_price,
    JSONExtract(raw, 'payload', 'total_amount', 'Decimal(15, 2)') as total_amount,
    JSONExtractString(raw, 'payload', 'region') as region,
    JSONExtractString(raw, 'payload', 'store_type') as store_type,
    fromUnixTimestamp64Milli(JSONExtractInt(raw, 'payload', 'timestamp')) as timestamp
FROM pos_kafka_queue;


-- ── Hourly aggregate table (SummingMergeTree auto-merges on ORDER BY keys) ────
CREATE TABLE IF NOT EXISTS pos_hourly_agg
(
    hour        DateTime,
    region      LowCardinality(String),
    category    LowCardinality(String),
    revenue     Decimal(20, 2),
    units_sold  UInt64,
    tx_count    UInt64
)
ENGINE = SummingMergeTree((revenue, units_sold, tx_count))
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, region, category);


-- ── MV 2: Raw → Hourly aggregate ──────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_pos_hourly_agg
TO pos_hourly_agg
AS
SELECT
    toStartOfHour(timestamp) AS hour,
    region,
    category,
    sum(total_amount)        AS revenue,
    sum(quantity)            AS units_sold,
    count()                  AS tx_count
FROM pos_transactions
GROUP BY hour, region, category;


-- ── Product daily aggregate table ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pos_product_agg
(
    date            Date,
    product_id      LowCardinality(String),
    product_name    LowCardinality(String),
    category        LowCardinality(String),
    revenue         Decimal(20, 2),
    units_sold      UInt64,
    tx_count        UInt64
)
ENGINE = SummingMergeTree((revenue, units_sold, tx_count))
PARTITION BY toYYYYMM(date)
ORDER BY (date, category, product_id);


-- ── MV 3: Raw → Product aggregate ─────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_pos_product_agg
TO pos_product_agg
AS
SELECT
    toDate(timestamp)   AS date,
    product_id,
    product_name,
    category,
    sum(total_amount)   AS revenue,
    sum(quantity)       AS units_sold,
    count()             AS tx_count
FROM pos_transactions
GROUP BY date, product_id, product_name, category;
