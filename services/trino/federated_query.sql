-- =====================================================================
-- Vinamilk FMCG Analytics Platform - Federated Query Demo
-- joining ClickHouse (Hot Path - Real-time) & Apache Iceberg (Cold Path - Historical)
-- =====================================================================

WITH hot_data AS (
    -- ClickHouse maps String columns as VARBINARY by default in Trino.
    -- We use from_utf8() to decode them into VARCHAR for joins.
    SELECT 
        from_utf8(region) as region, 
        from_utf8(category) as category, 
        SUM(total_amount) as revenue_realtime,
        COUNT(*) as tx_count_realtime
    FROM clickhouse.default.pos_transactions
    GROUP BY from_utf8(region), from_utf8(category)
),
cold_data AS (
    -- Apache Iceberg table stores historical records partitioned by month and region.
    SELECT 
        region, 
        category, 
        SUM(total_amount) as revenue_historical,
        COUNT(*) as tx_count_historical
    FROM iceberg.fmcg.pos_transactions_historical
    GROUP BY region, category
)
SELECT 
    coalesce(h.region, c.region) as region,
    coalesce(h.category, c.category) as category,
    coalesce(h.revenue_realtime, 0.0) as revenue_realtime,
    coalesce(c.revenue_historical, 0.0) as revenue_historical,
    coalesce(h.revenue_realtime, 0.0) + coalesce(c.revenue_historical, 0.0) as total_revenue,
    coalesce(h.tx_count_realtime, 0) + coalesce(c.tx_count_historical, 0) as total_transactions
FROM hot_data h
FULL OUTER JOIN cold_data c 
    ON h.region = c.region 
   AND h.category = c.category
ORDER BY total_revenue DESC;
