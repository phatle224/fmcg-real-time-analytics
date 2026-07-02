import clickhouse_connect
import psycopg2
import trino.dbapi
import time

def run_postgres(cursor, query):
    # Warmup
    cursor.execute(query)
    cursor.fetchall()
    
    # Run 5 times
    durations = []
    for _ in range(5):
        start = time.perf_counter()
        cursor.execute(query)
        cursor.fetchall()
        end = time.perf_counter()
        durations.append((end - start) * 1000)
    return sum(durations) / len(durations)

def run_clickhouse(client, query):
    # Warmup
    client.command(query)
    
    # Run 5 times
    durations = []
    for _ in range(5):
        start = time.perf_counter()
        client.command(query)
        end = time.perf_counter()
        durations.append((end - start) * 1000)
    return sum(durations) / len(durations)

def run_trino(cursor, query):
    # Warmup
    cursor.execute(query)
    cursor.fetchall()
    
    # Run 3 times (federated queries can be slightly heavier)
    durations = []
    for _ in range(3):
        start = time.perf_counter()
        cursor.execute(query)
        cursor.fetchall()
        end = time.perf_counter()
        durations.append((end - start) * 1000)
    return sum(durations) / len(durations)

def main():
    print("=========================================================================")
    print(" FMCG Analytics Platform - Native Performance Benchmark                   ")
    print("=========================================================================")
    
    # Establish connections
    print("Connecting to databases...")
    try:
        ch_client = clickhouse_connect.get_client(host='localhost', port=8123, username='default')
        print("OK: Connected to ClickHouse (localhost:8123)")
    except Exception as e:
        print(f"ERROR: ClickHouse connection failed: {e}")
        return
        
    try:
        pg_conn = psycopg2.connect(host='localhost', port=15433, user='postgres', password='postgrespassword', database='fmcg')
        pg_cursor = pg_conn.cursor()
        print("OK: Connected to PostgreSQL (localhost:15433)")
    except Exception as e:
        print(f"ERROR: PostgreSQL connection failed: {e}")
        return
        
    try:
        trino_conn = trino.dbapi.connect(host='localhost', port=8060, user='admin')
        trino_cursor = trino_conn.cursor()
        print("OK: Connected to Trino (localhost:8060)")
    except Exception as e:
        print(f"ERROR: Trino connection failed: {e}")
        return
        
    queries = {
        "COUNT": {
            "postgres": "SELECT COUNT(*) FROM pos_transactions;",
            "clickhouse_raw": "SELECT COUNT() FROM default.pos_transactions;",
            "clickhouse_mv": "SELECT SUM(tx_count) FROM default.pos_hourly_agg;"
        },
        "REVENUE_BY_REGION": {
            "postgres": "SELECT region, SUM(total_amount) FROM pos_transactions GROUP BY region ORDER BY SUM(total_amount) DESC;",
            "clickhouse_raw": "SELECT region, SUM(total_amount) FROM default.pos_transactions GROUP BY region ORDER BY SUM(total_amount) DESC;",
            "clickhouse_mv": "SELECT region, SUM(revenue) FROM default.pos_hourly_agg GROUP BY region ORDER BY SUM(revenue) DESC;"
        },
        "SALES_BY_CATEGORY": {
            "postgres": "SELECT category, SUM(total_amount), SUM(quantity) FROM pos_transactions GROUP BY category ORDER BY SUM(total_amount) DESC;",
            "clickhouse_raw": "SELECT category, SUM(total_amount), SUM(quantity) FROM default.pos_transactions GROUP BY category ORDER BY SUM(total_amount) DESC;",
            "clickhouse_mv": "SELECT category, SUM(revenue), SUM(units_sold) FROM default.pos_hourly_agg GROUP BY category ORDER BY SUM(revenue) DESC;"
        }
    }
    
    trino_federated_query = """
    WITH hot_data AS (
        SELECT from_utf8(region) as region, from_utf8(category) as category, SUM(total_amount) as revenue_realtime
        FROM clickhouse.default.pos_transactions_trino_view
        GROUP BY from_utf8(region), from_utf8(category)
    ),
    cold_data AS (
        SELECT region, category, SUM(total_amount) as revenue_historical
        FROM iceberg.fmcg.pos_transactions_historical
        GROUP BY region, category
    )
    SELECT 
        coalesce(h.region, c.region) as region,
        coalesce(h.category, c.category) as category,
        coalesce(h.revenue_realtime, 0.0) + coalesce(c.revenue_historical, 0.0) as total_revenue
    FROM hot_data h
    FULL OUTER JOIN cold_data c ON h.region = c.region AND h.category = c.category
    ORDER BY total_revenue DESC
    """
    
    results = []
    
    for q_name, engines in queries.items():
        print(f"\nRunning Benchmark for {q_name}...")
        t_pg = run_postgres(pg_cursor, engines["postgres"])
        t_ch_raw = run_clickhouse(ch_client, engines["clickhouse_raw"])
        t_ch_mv = run_clickhouse(ch_client, engines["clickhouse_mv"])
        
        results.append({
            "Query": q_name,
            "PostgreSQL (ms)": f"{t_pg:.3f}" if t_pg else "Error",
            "ClickHouse Raw (ms)": f"{t_ch_raw:.3f}" if t_ch_raw else "Error",
            "ClickHouse MV (ms)": f"{t_ch_mv:.3f}" if t_ch_mv else "Error",
            "Speedup (PG vs MV)": f"{t_pg / t_ch_mv:.1f}x" if (t_pg and t_ch_mv) else "N/A"
        })
        
    print("\nRunning Federated Query on Trino (ClickHouse + Iceberg)...")
    t_trino = run_trino(trino_cursor, trino_federated_query)
    
    print("\n==========================================================================================")
    print("                                   BENCHMARK RESULTS                                      ")
    print("==========================================================================================")
    headers = ["Query", "PostgreSQL (ms)", "ClickHouse Raw (ms)", "ClickHouse MV (ms)", "Speedup (PG vs MV)"]
    print(f"{headers[0]:<25} | {headers[1]:<18} | {headers[2]:<20} | {headers[3]:<18} | {headers[4]:<18}")
    print("-" * 105)
    for r in results:
        print(f"{r['Query']:<25} | {r['PostgreSQL (ms)']:<18} | {r['ClickHouse Raw (ms)']:<20} | {r['ClickHouse MV (ms)']:<18} | {r['Speedup (PG vs MV)']:<18}")
        
    if t_trino:
        print(f"\nTrino Federated Query (ClickHouse Hot + Iceberg Cold Join): {t_trino:.3f} ms")
    else:
        print("\nTrino Federated Query: Error")
    print("==========================================================================================")
    
    # Close connections
    pg_cursor.close()
    pg_conn.close()
    trino_cursor.close()
    trino_conn.close()

if __name__ == "__main__":
    main()
