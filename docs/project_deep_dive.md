# Giải Thích Chi Tiết Toàn Bộ Hệ Thống FMCG Real-Time Analytics Platform

## Tổng Quan: Hệ Thống Này Giải Quyết Vấn Đề Gì?

Hãy tưởng tượng Vinamilk có **1,000 điểm bán lẻ (POS)** trên toàn quốc. Mỗi giây có hàng chục giao dịch xảy ra đồng thời. Ban quản lý cần biết ngay lập tức:
- Doanh thu theo khu vực đang như thế nào?
- Sản phẩm nào đang bán chạy nhất?
- Khu vực nào đang có vấn đề về tồn kho?

Đồng thời, bộ phận phân tích cũng cần truy vấn **dữ liệu lịch sử 3 năm** để dự đoán xu hướng. Đây là mâu thuẫn cốt lõi: **dữ liệu thời gian thực cần tốc độ cao, dữ liệu lịch sử cần chi phí lưu trữ thấp**.

Hệ thống này giải quyết bài toán đó bằng kiến trúc **Hot/Cold Path**.

---

## Luồng Dữ Liệu Từ Đầu Đến Cuối

```
[POS Terminals / Giả Lập]
        |
        | HTTP POST (JSON)
        v
[FastAPI - Generator Service] <-- Đây là bước 1: thu nhận dữ liệu
        |
        | produce message (Kafka Protocol)
        v
[Apache Kafka - Message Broker] <-- Đây là trung tâm phân phối
        |
        +------ HOT PATH ------+------ COLD PATH ------+
        |                      |
        | consume               | consume
        v                      v
[ClickHouse]          [Kafka Connect S3 Sink]
(phân tích nóng)       (lưu trữ lạnh)
        |                      |
        |               [MinIO S3 Storage]
        |               (file .parquet)
        |                      |
        |               [Hive Metastore]
        |               (quản lý metadata)
        |                      |
        +----------[Trino]-----+
                   (truy vấn liên kết)
                        |
                   [Cube.js]
                   (semantic layer)
                        |
                   [Grafana]
                   (dashboard)
```

---

## Chi Tiết Từng Thành Phần

### Bước 1: Generator (FastAPI + Simulator)
**File:** `services/generator/main.py`, `simulator.py`, `schemas.py`
**Port:** `localhost:8000`

**Simulator làm gì?**
`simulator.py` tạo ra dữ liệu giả lập giao dịch POS thực tế của Vinamilk:
- 12 sản phẩm thật (Vinamilk Tuoi Tiet Trung 1L, Sua Chua Vinamilk, Vfresh...)
- Phân bổ khu vực theo thực tế: HCM 35%, HN 28%, DN 12%, CT 10%...
- 4 loại hình cửa hàng: siêu thị 30%, tiện lợi 35%, chợ truyền thống 25%...
- Giá được làm tròn đến 500 VND

**FastAPI có 4 endpoints chính:**
```
GET  /health                        -- Kiểm tra trạng thái kết nối Kafka
POST /api/v1/simulate?count=14000   -- Gửi N giao dịch vào Kafka (một lần)
POST /api/v1/events                 -- Gửi 1 giao dịch cụ thể (test đơn lẻ)
POST /api/v1/stream/start?tps=1000  -- Bật chế độ phát liên tục 1000 tx/giây
POST /api/v1/stream/stop            -- Tắt chế độ phát liên tục
```

**Tại sao cần wrap trong `schema + payload`?**
Kafka Connect S3 Sink Connector yêu cầu message phải có cấu trúc:
```json
{
  "schema": { "type": "struct", "fields": [...] },
  "payload": { "transaction_id": "...", "region": "HN", ... }
}
```
Điều này cho phép connector tự động hiểu cấu trúc dữ liệu mà không cần cấu hình schema riêng.

---

### Bước 2: Apache Kafka (Message Broker)
**Image:** `confluentinc/cp-kafka:7.6.1`
**Container:** `fmcg-kafka`, `fmcg-zookeeper`
**Port nội bộ:** `kafka:29092`; **Port từ máy host:** `localhost:19092`

**Kafka là gì?**
Kafka là một **hàng đợi tin nhắn phân tán** (distributed message queue). Hãy hình dung như băng chuyền sản xuất: Producer (generator) đặt hàng hóa lên băng, nhiều Consumer (ClickHouse, Kafka Connect) lấy hàng xuống theo nhịp riêng mà không ảnh hưởng nhau.

**Tại sao cần Kafka thay vì ghi trực tiếp vào DB?**
- **Tách biệt tốc độ ghi và đọc:** Generator gửi 1000 tx/giây, ClickHouse có thể xử lý theo batch.
- **Chống mất dữ liệu:** Nếu ClickHouse tạm thời bị chậm, Kafka giữ dữ liệu trong hàng đợi (retention 7 ngày theo cấu hình `KAFKA_LOG_RETENTION_HOURS: 168`).
- **Một Producer, nhiều Consumer:** Cùng một luồng dữ liệu, ClickHouse đọc riêng, Kafka Connect đọc riêng, không cần duplicate code.

**Topic:** `pos.transactions` (6 partitions, auto-created)

**Kafka UI:** `localhost:8080` - Giao diện web để xem topic, message, consumer lag.

**Zookeeper** là service quản lý metadata và election leader cho Kafka (đây là kiến trúc cũ, Kafka mới dùng KRaft thay thế, nhưng version 7.6.1 vẫn dùng Zookeeper).

---

### Bước 3a: ClickHouse - Hot Path (Lưu Trữ Nóng)
**Image:** `clickhouse/clickhouse-server:24.5`
**Container:** `fmcg-clickhouse`
**Port:** `localhost:8123` (HTTP), `localhost:9000` (Native)

**ClickHouse là gì?**
ClickHouse là **cơ sở dữ liệu phân tích cột** (columnar OLAP database). Thay vì lưu dữ liệu theo hàng như PostgreSQL (mỗi hàng = 1 record), ClickHouse lưu theo cột (mỗi cột = tất cả giá trị của 1 trường).

**Ưu điểm cột khi phân tích:**
- Query `SELECT SUM(total_amount) FROM pos_transactions` chỉ cần đọc đúng cột `total_amount`, bỏ qua toàn bộ các cột khác.
- PostgreSQL phải đọc từng hàng, lấy tất cả cột rồi mới tính tổng -> chậm hơn nhiều.

**3 đối tượng chính trong ClickHouse (`default` database):**

**1. Bảng Kafka Engine (`pos_kafka_queue`):**
```sql
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list  = 'pos.transactions',
    kafka_group_name  = 'clickhouse-pos-consumer',
    kafka_format      = 'JSONAsString',
    kafka_num_consumers = 4
```
Đây là một consumer Kafka được implement ngay trong ClickHouse. Nó đọc raw JSON string từ Kafka topic. Bảng này không lưu dữ liệu vĩnh viễn - nó chỉ là "cổng vào".

**2. Bảng MergeTree (`pos_transactions`) - Bảng lưu trữ thực:**
```sql
ENGINE = MergeTree
ORDER BY (region, product_id, toStartOfHour(timestamp))
PARTITION BY toYYYYMM(timestamp)
TTL toDateTime(timestamp) + INTERVAL 90 DAY
```
- `ORDER BY`: Dữ liệu được sắp xếp theo `region, product_id, timestamp` - khớp với pattern truy vấn phổ biến ("lọc theo khu vực, sau đó theo sản phẩm").
- `TTL`: Dữ liệu tự động xóa sau 90 ngày (đây là "hot storage").
- `MergeTree` tự động merge các file nhỏ thành file lớn hơn ở background để tối ưu đọc.

**3. Materialized View (`mv_kafka_to_pos`) - Cầu nối:**
```sql
CREATE MATERIALIZED VIEW mv_kafka_to_pos TO pos_transactions AS
SELECT
    JSONExtractString(raw, 'payload', 'transaction_id') as transaction_id,
    JSONExtractString(raw, 'payload', 'region') as region,
    ...
    fromUnixTimestamp64Milli(JSONExtractInt(raw, 'payload', 'timestamp')) as timestamp
FROM pos_kafka_queue;
```
Materialized View hoạt động như một **trigger tự động**: mỗi khi `pos_kafka_queue` có dữ liệu mới, MV này tự động extract JSON, convert kiểu dữ liệu, và ghi vào `pos_transactions`.

**4. Bảng Pre-aggregation (`pos_hourly_agg`, `pos_product_agg`):**
```sql
ENGINE = SummingMergeTree((revenue, units_sold, tx_count))
ORDER BY (hour, region, category)
```
`SummingMergeTree` là engine đặc biệt: khi có 2 hàng có cùng `ORDER BY key`, nó tự động cộng dồn các cột số. Kết quả: truy vấn "tổng doanh thu theo giờ" chỉ mất 2ms thay vì quét toàn bộ bảng raw.

**Luồng dữ liệu trong ClickHouse:**
```
Kafka topic -> [pos_kafka_queue] -> [mv_kafka_to_pos trigger]
                                          |
                    +---------------------+---------------------+
                    |                                           |
                    v                                           v
           [pos_transactions]                        [mv_pos_hourly_agg trigger]
           (MergeTree raw)                                      |
                                                                v
                                                       [pos_hourly_agg]
                                                       (SummingMergeTree)
```

---

### Bước 3b: Kafka Connect S3 Sink - Cold Path (Lưu Trữ Lạnh)
**Container:** `fmcg-kafka-connect`
**Port:** `localhost:8083` (REST API)

**Kafka Connect là gì?**
Kafka Connect là một **framework tích hợp dữ liệu** của Confluent. Nó cung cấp cơ chế "connector" để đọc/ghi dữ liệu giữa Kafka và các hệ thống khác (database, S3, Elasticsearch...) mà không cần viết code.

**S3 Sink Connector** (`services/kafka-connect/connectors/s3-sink-config.json`):
```json
{
  "connector.class": "io.confluent.connect.s3.S3SinkConnector",
  "topics": "pos.transactions",
  "s3.bucket.name": "fmcg-lakehouse",
  "store.url": "http://minio:9000",
  "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
  "partitioner.class": "TimeBasedPartitioner",
  "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
  "flush.size": "1000",
  "rotate.interval.ms": "60000"
}
```

**Giải thích từng config quan trọng:**
- `format.class = ParquetFormat`: Ghi file theo định dạng **Parquet** (cột nhị phân, nén cao). Không phải JSON, không phải CSV - Parquet là tiêu chuẩn lưu trữ Big Data.
- `path.format`: File được phân thư mục theo thời gian `year=2026/month=07/day=03/hour=15/` - gọi là **partitioning**, giúp query chỉ scan đúng partition cần thiết.
- `flush.size = 1000`: Gom tích đủ 1000 message mới tạo 1 file Parquet.
- `rotate.interval.ms = 60000`: Hoặc sau 60 giây, dù chưa đủ 1000 message, cũng tạo file mới.

**Tại sao dùng Parquet thay vì JSON?**
- Parquet nén dữ liệu tốt hơn 5-10 lần so với JSON.
- Parquet lưu theo cột, khi query chỉ đọc đúng cột cần thiết.
- Parquet hỗ trợ predicate pushdown: đọc xong metadata biết ngay partition nào có dữ liệu cần.

---

### Bước 4: MinIO (Object Storage - S3 Compatible)
**Container:** `fmcg-minio`
**Port:** `localhost:9005` (API S3), `localhost:9006` (Web Console)
**Credentials:** `minioadmin / minioadmin`

**MinIO là gì?**
MinIO là phần mềm open-source cung cấp **Object Storage tương thích 100% với AWS S3**. Hệ thống sử dụng MinIO thay cho AWS S3 thực để chạy trên máy local.

Dữ liệu Parquet từ Kafka Connect được lưu vào bucket `fmcg-lakehouse`:
```
fmcg-lakehouse/
  topics/
    pos.transactions/
      year=2026/
        month=07/
          day=03/
            hour=15/
              pos.transactions+0+0000012345.parquet
              pos.transactions+1+0000009876.parquet
```

**minio-create-bucket**: Một container nhỏ (`fmcg-minio-init`) chạy một lần khi khởi động để tự động tạo bucket `fmcg-lakehouse` nếu chưa tồn tại.

---

### Bước 5: Hive Metastore (Quản Lý Metadata)
**Container:** `fmcg-hive-metastore`
**Port:** `localhost:9083` (Thrift protocol)

**Hive Metastore là gì?**
Hive Metastore là một **catalog trung tâm lưu trữ metadata** của các bảng dữ liệu phân tán. Nó biết:
- Bảng `iceberg.fmcg.pos_transactions_historical` có những file nào trên MinIO?
- Cấu trúc schema của bảng đó là gì?
- Các file đó nằm ở partition nào?

Hive Metastore lưu toàn bộ metadata vào **MySQL** (`fmcg-metastore-db`). Khi Trino cần query bảng Iceberg, nó hỏi Hive Metastore trước để biết file nào cần đọc.

**Apache Iceberg** là format bảng (table format) hiện đại, được xây dựng trên nền tảng Parquet files. Iceberg bổ sung:
- **Time Travel**: `SELECT * FROM table FOR TIMESTAMP AS OF '2026-01-01'`
- **Schema Evolution**: Thêm/đổi tên cột mà không cần rewrite toàn bộ data
- **ACID transactions**: Ghi dữ liệu an toàn, không bị corrupt
- **Partition Pruning thông minh**: Đọc metadata file thay vì liệt kê toàn bộ file S3

---

### Bước 6: Trino (Federated Query Engine)
**Image:** `trinodb/trino:451`
**Container:** `fmcg-trino`
**Port:** `localhost:8060`

**Trino là gì?**
Trino là một **MPP SQL Query Engine** (Massively Parallel Processing). Nó không lưu dữ liệu, chỉ **đọc dữ liệu từ nhiều nguồn khác nhau và thực hiện JOIN ở RAM**.

**Trino kết nối đến 2 catalog:**

**Catalog 1 - ClickHouse** (`services/trino/catalog/clickhouse.properties`):
```ini
connector.name=clickhouse
connection-url=jdbc:clickhouse://clickhouse:8123/default
connection-user=default
```
Trino kết nối đến ClickHouse qua JDBC, giống như client tool. Tuy nhiên, do ClickHouse dùng kiểu `LowCardinality(String)` và `String`, Trino JDBC driver map chúng thành `varbinary` (bytes) thay vì `varchar`.

**Giải pháp:** Dùng hàm `from_utf8(column)` để convert `varbinary` thành `varchar`:
```sql
SELECT from_utf8(region) AS region FROM clickhouse.default.pos_transactions
```

**Catalog 2 - Iceberg** (`services/trino/catalog/iceberg.properties`):
```ini
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
```
Trino kết nối đến Hive Metastore để lấy metadata, sau đó đọc trực tiếp file Parquet trên MinIO.

**Federated Query mẫu:**
```sql
WITH combined AS (
    SELECT from_utf8(region) AS region, from_utf8(category) AS category,
           CAST(total_amount AS DOUBLE) AS total_amount
    FROM clickhouse.default.pos_transactions_trino_view  -- Hot Path
    
    UNION ALL
    
    SELECT region, category, total_amount
    FROM iceberg.fmcg.pos_transactions_historical        -- Cold Path
)
SELECT region, category, SUM(total_amount) AS total_revenue
FROM combined
GROUP BY region, category
ORDER BY total_revenue DESC;
```
Trino lập kế hoạch query, đẩy filter xuống ClickHouse và MinIO song song, lấy kết quả về, JOIN ở bộ nhớ RAM của Trino coordinator, trả kết quả.

---

### Bước 7: Cube.js (Semantic Layer)
**Container:** `fmcg-cubejs`
**Port:** `localhost:4000`

**Cube.js là gì?**
Cube.js là một **Semantic Layer** - tầng định nghĩa ngữ nghĩa nghiệp vụ. Thay vì mỗi team tự viết SQL với cách tính khác nhau, Cube.js định nghĩa tập trung:

```javascript
cube('PosTransactions', {
  measures: {
    revenue: { sql: 'total_amount', type: 'sum' },
    avgBasketSize: { sql: '${revenue} / NULLIF(${count}, 0)', type: 'number' }
  },
  dimensions: {
    region: { sql: 'region', type: 'string' },
    category: { sql: 'category', type: 'string' }
  }
})
```

Khi Grafana hỏi "doanh thu theo khu vực", Cube.js tự động sinh ra SQL Trino chuẩn, trả kết quả, và cache lại. Lần sau hỏi lại: trả cache ngay lập tức (< 10ms).

---

### Bước 8: Monitoring Stack
**Grafana** (`localhost:3000`): Dashboard trực quan hóa dữ liệu. Đã pre-install plugin `grafana-clickhouse-datasource` để kết nối trực tiếp ClickHouse.

**Prometheus** (`localhost:9090`): Thu thập metrics từ các service (CPU, RAM, request count...) theo dạng time series.

**cAdvisor** (`localhost:9091`): Thu thập metrics của Docker containers (CPU/RAM từng container).

---

### Bước 9: PostgreSQL (Benchmark Baseline)
**Container:** `fmcg-postgres`
**Port:** `localhost:15433`

PostgreSQL được dùng **chỉ để so sánh hiệu năng** trong script `benchmark.py`. Nó lưu cùng dữ liệu với ClickHouse để đo xem ClickHouse nhanh hơn bao nhiêu lần.

---

## Tóm Tắt: Tại Sao Mỗi Tool Được Chọn?

| Tool | Lý Do Chọn |
| :--- | :--- |
| **FastAPI** | Framework Python nhanh nhất, hỗ trợ async, tự động gen Swagger docs |
| **Apache Kafka** | Message queue phân tán, đảm bảo không mất dữ liệu, 1 producer cho nhiều consumer |
| **ClickHouse** | OLAP columnar DB nhanh nhất cho aggregation query (100-1000x nhanh hơn PostgreSQL) |
| **Kafka Connect** | Framework tích hợp zero-code, tự động đẩy Kafka sang S3/MinIO |
| **Apache Parquet** | Định dạng cột, nén tốt, tiêu chuẩn Big Data |
| **MinIO** | S3-compatible storage chạy local, thay thế AWS S3 cho dev/test |
| **Hive Metastore** | Catalog metadata trung tâm, cho phép Trino "biết" bảng nào ở đâu |
| **Apache Iceberg** | Table format hiện đại: time travel, ACID, schema evolution |
| **Trino** | Federated SQL engine, JOIN cross-database mà không cần move data |
| **Cube.js** | Semantic layer, tập trung định nghĩa metrics, caching tự động |
| **Grafana** | Dashboard visualization, nhiều plugin data source |
| **Prometheus** | Time-series metrics collection cho monitoring |

---

## Cổng Mạng Tổng Hợp (Quick Reference)

| Service | URL Truy Cập | Mục Đích |
| :--- | :--- | :--- |
| FastAPI Swagger UI | `http://localhost:8000/docs` | Test API, xem endpoints |
| Kafka UI | `http://localhost:8080` | Xem topics, messages, consumer lag |
| Kafka Connect API | `http://localhost:8083/connectors` | Quản lý connector |
| ClickHouse HTTP | `http://localhost:8123` | Query ClickHouse qua HTTP |
| MinIO Console | `http://localhost:9006` | Xem file Parquet trên S3 |
| Trino UI | `http://localhost:8060` | Xem query execution plan |
| Cube.js Playground | `http://localhost:4000` | Test semantic metrics |
| Grafana | `http://localhost:3000` | Dashboard (admin/admin123) |
| Prometheus | `http://localhost:9090` | Xem metrics raw |

---

## Các Vấn Đề Đã Gặp và Cách Giải Quyết

### 1. PowerShell không dùng được `\` để nối dòng
**Bash:** Dùng `\` cuối dòng để nối.
**PowerShell:** Dùng dấu backtick `` ` `` để nối dòng, hoặc dùng `Invoke-RestMethod` thay vì `curl`.

### 2. ClickHouse dùng database `fmcg` hay `default`?
**Thực tế:** Toàn bộ bảng trong project này nằm trong database `default` (theo `.env`: `CLICKHOUSE_DB=default`). Tài liệu `architecture.md` đề cập `fmcg.pos_transactions` là sai - lệnh đúng là `default.pos_transactions`.

### 3. Trino không đọc được String từ ClickHouse (trả về varbinary)
**Nguyên nhân:** ClickHouse JDBC driver map `String` và `LowCardinality(String)` thành `varbinary`.
**Giải pháp:** Dùng hàm `from_utf8(column_name)` trong Trino SQL để convert sang `varchar`.

### 4. `/api/v1/simulate` trả về `Method Not Allowed`
**Nguyên nhân:** Endpoint này được định nghĩa là `@app.post(...)`, không phải `@app.get(...)`.
**Giải pháp:** Dùng `-Method Post` trong PowerShell hoặc `-X POST` trong curl.

### 5. `/api/v1/events` trả về `Not Found`
**Nguyên nhân:** Endpoint này ban đầu không tồn tại trong code.
**Giải pháp:** Đã thêm endpoint `POST /api/v1/events` vào `main.py` và rebuild container.

### 6. Schema validation: `"Dairy"` vs `"dairy"`
**Nguyên nhân:** Pydantic schema dùng `Literal["dairy", "yogurt", ...]` - chỉ chấp nhận chữ thường.
**Giải pháp:** Dùng chữ thường: `"dairy"`, `"yogurt"`, `"convenience"`, `"supermarket"`.
