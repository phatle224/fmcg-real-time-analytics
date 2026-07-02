-- =============================================================================
-- 01_create_tables.sql
-- Hot Path: Kafka Engine queue + MergeTree raw storage
-- =============================================================================

-- 1. Kafka Engine — acts as Kafka Consumer, reads from pos.transactions topic
CREATE TABLE IF NOT EXISTS pos_kafka_queue
(
    transaction_id  String,
    pos_id          String,
    product_id      String,
    product_name    String,
    category        String,
    quantity        UInt8,
    unit_price      Decimal(15, 2),
    total_amount    Decimal(15, 2),
    region          String,
    store_type      String,
    timestamp       DateTime64(3, 'UTC')
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list     = 'kafka:29092',
    kafka_topic_list      = 'pos.transactions',
    kafka_group_name      = 'clickhouse-pos-consumer',
    kafka_format          = 'JSONEachRow',
    kafka_num_consumers   = 4,
    kafka_skip_broken_messages = 100;


-- 2. MergeTree — raw event storage (hot: last 30 days)
--    ORDER BY chosen for typical query patterns: filter by region/product, time range
CREATE TABLE IF NOT EXISTS pos_transactions
(
    transaction_id  String,
    pos_id          String,
    product_id      LowCardinality(String),
    product_name    LowCardinality(String),
    category        LowCardinality(String),
    quantity        UInt8,
    unit_price      Decimal(15, 2),
    total_amount    Decimal(15, 2),
    region          LowCardinality(String),
    store_type      LowCardinality(String),
    timestamp       DateTime64(3, 'UTC'),
    insert_time     DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (region, product_id, toStartOfHour(timestamp))
TTL toDateTime(timestamp) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
