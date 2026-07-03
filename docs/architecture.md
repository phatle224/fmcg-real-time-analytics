# Tài Liệu Thiết Kế Kiến Trúc Hệ Thống (System Architecture & Design)

Tài liệu này mô tả chi tiết thiết kế kỹ thuật, luồng dữ liệu, cấu trúc bảng, và cơ chế vận hành của hệ thống **FMCG Real-Time Analytics Platform** (Nền tảng phân tích dữ liệu bán lẻ thời gian thực cho ngành hàng tiêu dùng nhanh).

---

## 1. Tổng Quan Hệ Thống (Executive Summary)

Trong kinh doanh FMCG, việc theo dõi doanh thu bán hàng và sản lượng sản phẩm theo thời gian thực tại hàng nghìn điểm bán (POS) đóng vai trò quyết định để quản lý chuỗi cung ứng, tồn kho và các chiến dịch khuyến mãi. Tuy nhiên, lưu trữ toàn bộ dữ liệu lịch sử chi tiết trong cơ sở dữ liệu phân tích thời gian thực (OLAP) rất tốn kém và làm suy giảm hiệu năng truy vấn.

Hệ thống này giải quyết bài toán trên bằng mô hình **Hot/Cold Path** (Kiến trúc đường dẫn kép):
*   **Luồng dữ liệu nóng (Hot Path)**: Lưu trữ dữ liệu giao dịch trong vòng 24 giờ qua vào ClickHouse để phục vụ dashboard vận hành thời gian thực độ trễ thấp (< 5s).
*   **Luồng dữ liệu lạnh (Cold Path)**: Tự động nén và đẩy dữ liệu lịch sử lớn sang MinIO dưới định dạng Apache Iceberg để phân tích dài hạn và tối ưu chi phí lưu trữ.
*   **Tầng liên kết (Federated Layer)**: Sử dụng Trino để truy vấn liên kết, cho phép JOIN dữ liệu ClickHouse và Iceberg mà không cần dịch chuyển dữ liệu vật lý.
*   **Tầng ngữ nghĩa (Semantic Layer)**: Định nghĩa tập trung các chỉ số doanh nghiệp (Revenue, Units Sold, Avg Basket Size) bằng Cube.js nhằm bảo đảm tính nhất quán số liệu và tận dụng cơ chế lưu đệm (caching).

---

## 2. Chi Tiết Kiến Trúc Hai Luồng (Hot/Cold Path Architecture)

```
                       ┌─────────────────────────┐
                       │  POS Transaction Event  │
                       └────────────┬────────────┘
                                    │
                                    ▼
                       ┌─────────────────────────┐
                       │  FastAPI Ingestion API  │
                       └────────────┬────────────┘
                                    │
                                    ▼
                       ┌─────────────────────────┐
                       │  Apache Kafka Broker    │
                       │ Topic: pos.transactions │
                       └──────┬────────────┬─────┘
                              │            │
         ┌────────────────────┘            └────────────────────┐
         │ HOT PATH                                             │ COLD PATH
         ▼                                                      ▼
┌─────────────────────────┐                            ┌─────────────────────────┐
│ ClickHouse Kafka Engine │                            │ Kafka Connect S3 Sink   │
└────────┬────────────────┘                            └────────┬────────────────┘
         │                                                      │
         ▼                                                      ▼
┌─────────────────────────┐                            ┌─────────────────────────┐
│ ClickHouse MergeTree    │                            │ MinIO Object Storage    │
│ (Raw Events Table)      │                            │ (Parquet Partitioned)   │
└────────┬────────────────┘                            └────────┬────────────────┘
         │                                                      │
         ▼                                                      ▼
┌─────────────────────────┐                            ┌─────────────────────────┐
│ Materialized Views      │                            │ Apache Iceberg Table    │
│ (Hourly & Product Aggs) │                            │ (Hive Metastore Catalog)│
└────────┬────────────────┘                            └────────┬────────────────┘
         │                                                      │
         └────────────────────┬─────────────────────────────────┘
                              │
                              ▼
                       ┌─────────────────────────┐
                       │   Trino Query Engine    │
                       └────────────┬────────────┘
                                    │ (PostgreSQL Protocol)
                                    ▼
                       ┌─────────────────────────┐
                       │ Cube.js Semantic Layer  │
                       └────────────┬────────────┘
                                    │
                                    ▼
                       ┌─────────────────────────┐
                       │    Grafana Dashboard    │
                       └─────────────────────────┘
```

### A. Luồng Dữ Liệu Nóng (Hot Path)
1.  **FastAPI Ingest API**: Tiếp nhận các event giao dịch từ bộ giả lập dưới dạng JSON payload qua HTTP POST và đẩy vào Kafka topic `pos.transactions` một cách không đồng bộ (async).
2.  **ClickHouse Kafka Engine**: Một công cụ bảng đặc biệt trong ClickHouse hoạt động như một Kafka Consumer. Nó tự động đọc luồng thông điệp từ Kafka theo cơ chế batching.
3.  **ClickHouse Materialized Views**: Hoạt động như một trigger ghi. Ngay khi dữ liệu được nạp vào bảng Kafka Engine, Materialized View sẽ lọc, biến đổi kiểu dữ liệu và ghi trực tiếp vào bảng đích MergeTree vật lý, đồng thời cập nhật dữ liệu tiền tổng hợp vào các bảng tổng hợp theo giờ/theo sản phẩm.

### B. Luồng Dữ Liệu Lạnh (Cold Path)
1.  **Kafka Connect S3 Sink Connector**: Đọc dữ liệu từ Kafka topic `pos.transactions` và gom cụm theo chu kỳ (ví dụ: mỗi 60 giây hoặc mỗi 10,000 records).
2.  **MinIO Object Storage**: Nhận dữ liệu từ connector dưới định dạng tệp tin cột Parquet hiệu năng cao, tổ chức phân mục (partition) theo thư mục ngày/giờ và vùng địa lý.
3.  **Hive Metastore**: Lưu trữ và quản lý metadata của các tệp tin lưu trên MinIO, đồng bộ hóa cấu trúc bảng (schema) với Apache Iceberg.
4.  **Apache Iceberg**: Cung cấp tầng quản lý bảng hiện đại với các tính năng time-travel (truy vấn theo thời điểm lịch sử) và schema evolution (tiến hóa cấu trúc bảng mà không cần ghi lại tệp dữ liệu thô).

---

## 3. Thiết Kế Cơ Sở Dữ Liệu & Bảng (Database Schema Design)

### A. ClickHouse (Hot Storage)

#### Bảng Lưu Trữ Dữ Liệu Thô (Raw Table)
Bảng sử dụng Engine `MergeTree` tối ưu hóa ghi và đọc theo cột, sắp xếp theo khóa `(region, product_id, timestamp)` để tăng tốc độ lọc dữ liệu:
```sql
CREATE TABLE IF NOT EXISTS fmcg.pos_transactions (
    transaction_id String,
    pos_id String,
    product_id String,
    product_name String,
    category LowCardinality(String),
    quantity Int32,
    unit_price Float64,
    total_amount Float64,
    region LowCardinality(String),
    store_type LowCardinality(String),
    timestamp DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (region, product_id, timestamp)
SETTINGS index_granularity = 8192;
```

#### Bảng Tổng Hợp Doanh Thu Theo Giờ (Materialized View Target)
Sử dụng engine `SummingMergeTree` để tự động cộng dồn doanh thu và số lượng bán khi có dữ liệu mới ghi vào:
```sql
CREATE TABLE IF NOT EXISTS fmcg.pos_hourly_agg (
    hour DateTime,
    region LowCardinality(String),
    category LowCardinality(String),
    total_revenue Float64,
    total_quantity Int64,
    transaction_count UInt64
) ENGINE = SummingMergeTree()
PRIMARY KEY (hour, region, category)
ORDER BY (hour, region, category);
```

#### Cầu Nối Materialized View
```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS fmcg.mv_pos_hourly_agg
TO fmcg.pos_hourly_agg AS
SELECT
    toStartOfHour(timestamp) AS hour,
    region,
    category,
    sum(total_amount) AS total_revenue,
    sum(quantity) AS total_quantity,
    count() AS transaction_count
FROM fmcg.pos_transactions
GROUP BY hour, region, category;
```

### B. Apache Iceberg (Cold Storage via Trino)

Bảng lịch sử trong Iceberg sử dụng phân vùng kép theo `region` và `category` để thu hẹp phạm vi quét tệp tin khi thực hiện các phân tích quy mô lớn:
```sql
CREATE TABLE iceberg.fmcg.pos_transactions_historical (
    transaction_id VARCHAR,
    pos_id VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    category VARCHAR,
    quantity INTEGER,
    unit_price DOUBLE,
    total_amount DOUBLE,
    region VARCHAR,
    store_type VARCHAR,
    timestamp TIMESTAMP
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['region', 'category']
);
```

---

## 4. Truy Vấn Liên Kết (Federated Queries via Trino)

Trino hoạt động như một MPP (Massively Parallel Processing) SQL query coordinator. Nó kết nối đồng thời đến ClickHouse qua JDBC connector và MinIO qua Iceberg connector.

Khi người dùng thực hiện truy vấn liên kết, công cụ lập kế hoạch truy vấn (Query Planner) của Trino sẽ tối ưu hóa câu lệnh bằng cách đẩy các tác vụ lọc và giới hạn xuống ClickHouse và Iceberg một cách song song (predicate pushdown), sau đó thực hiện join kết quả trung gian tại bộ nhớ RAM của Trino.

### Giải Pháp Tương Thích Kiểu Dữ Liệu
ClickHouse tối ưu hóa chuỗi ký tự bằng định dạng `LowCardinality(String)`. Khi Trino kết nối qua JDBC, kiểu dữ liệu này có thể gây lỗi không tương thích. Để xử lý triệt để, hệ thống định nghĩa các View chuẩn hóa trong ClickHouse để ép kiểu về chuỗi ký tự UTF-8 tiêu chuẩn trước khi Trino đọc dữ liệu:
```sql
-- ClickHouse View dùng làm cầu nối với Trino
CREATE VIEW fmcg.v_pos_transactions AS
SELECT
    transaction_id,
    pos_id,
    product_id,
    product_name,
    cast(category AS String) AS category,
    quantity,
    unit_price,
    total_amount,
    cast(region AS String) AS region,
    cast(store_type AS String) AS store_type,
    timestamp
FROM fmcg.pos_transactions;
```

### Câu Lệnh Truy Vấn Liên Kết Mẫu (Federated Query)
Dưới đây là câu SQL JOIN dữ liệu giao dịch 24 giờ qua trong ClickHouse (bảng nóng) với dữ liệu lịch sử 30 ngày trước lưu trong Iceberg (bảng lạnh) để tính toán mức độ tăng trưởng doanh thu theo vùng:
```sql
WITH combined_sales AS (
    -- Lấy dữ liệu nóng từ ClickHouse
    SELECT region, total_amount, timestamp FROM clickhouse.fmcg.v_pos_transactions
    WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
    
    UNION ALL
    
    -- Lấy dữ liệu lạnh từ Apache Iceberg
    SELECT region, total_amount, timestamp FROM iceberg.fmcg.pos_transactions_historical
    WHERE timestamp < CURRENT_TIMESTAMP - INTERVAL '1' DAY
      AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
)
SELECT
    region,
    sum(total_amount) AS total_revenue,
    count() AS total_transactions
FROM combined_sales
GROUP BY region
ORDER BY total_revenue DESC;
```

---

## 5. Tầng Ngữ Nghĩa (Semantic Layer with Cube.js)

Để tránh tình trạng mỗi bộ phận tự định nghĩa một công thức tính chỉ số khác nhau trên Dashboard, Cube.js được sử dụng để xây dựng một nguồn định nghĩa chỉ số duy nhất (Single Source of Truth).

### Định Nghĩa Schema (`PosTransactions.js`)
Cube định nghĩa rõ các thuộc tính đo lường (measures) và các chiều phân tích (dimensions):
```javascript
cube(`PosTransactions`, {
  sql: `SELECT * FROM trino.iceberg.fmcg.pos_transactions_historical`,
  
  measures: {
    count: {
      type: `count`,
      description: `Tổng số lượng giao dịch`
    },
    revenue: {
      sql: `total_amount`,
      type: `sum`,
      format: `currency`,
      description: `Tổng doanh thu`
    },
    unitsSold: {
      sql: `quantity`,
      type: `sum`,
      description: `Tổng sản phẩm bán ra`
    },
    avgBasketSize: {
      sql: `${revenue} / NULLIF(${count}, 0)`,
      type: `number`,
      format: `currency`,
      description: `Giá trị trung bình trên một giỏ hàng`
    }
  },
  
  dimensions: {
    region: {
      sql: `region`,
      type: `string`
    },
    category: {
      sql: `category`,
      type: `string`
    },
    storeType: {
      sql: `store_type`,
      type: `string`
    },
    timestamp: {
      sql: `timestamp`,
      type: `time`
    }
  }
});
```

### Cơ Chế Caching Pre-Aggregation
Cube.js có công cụ pre-aggregation tự động chạy ngầm. Công cụ này quét định kỳ qua Trino và tạo các bảng tổng hợp lưu tạm trong cơ sở dữ liệu đệm. Khi Grafana gửi yêu cầu truy vấn doanh thu, Cube.js trả kết quả tức thì từ cache (< 10ms) mà không cần quét lại hàng triệu dòng trên MinIO hay ClickHouse, giúp bảo vệ tài nguyên tính toán của hệ thống.

---

## 6. Kết Quả Đo Đạc Hiệu Năng (Benchmarks & Performance)

Thực thi chạy benchmark so sánh thời gian phản hồi giữa cơ sở dữ liệu quan hệ (PostgreSQL 16) và ClickHouse 24.5 trên cùng cấu hình tài nguyên:

### A. So Sánh Thời Gian Chạy Truy Vấn (10,000,000 bản ghi)
*   **COUNT(\*)**: PostgreSQL mất 8.24s do phải quét tất cả các dòng dữ liệu. ClickHouse (bảng raw MergeTree) mất 0.31s nhờ chỉ quét cột khóa. ClickHouse sử dụng Materialized View chỉ mất **0.002s** (2ms) - nhanh gấp **4,120 lần**.
*   **REVENUE_BY_REGION**: PostgreSQL mất 12.45s. ClickHouse Merge Tree mất 0.78s. ClickHouse với Materialized View hoàn thành trong **0.045s** (45ms) - nhanh gấp **276 lần**.
*   **SALES_BY_CATEGORY**: PostgreSQL mất 18.72s. ClickHouse Merge Tree mất 1.15s. ClickHouse với Materialized View hoàn thành trong **0.052s** (52ms) - nhanh gấp **360 lần**.

### B. Nguyên Nhân ClickHouse Đạt Hiệu Năng Vượt Trội
1.  **Columnar Storage**: PostgreSQL lưu trữ dữ liệu theo hàng (row-based). Khi chạy tính tổng doanh thu, PostgreSQL buộc phải quét toàn bộ dòng chứa cả ID giao dịch, POS ID, tên sản phẩm... ClickHouse chỉ quét đúng hai cột `total_amount` và `region`, giảm tải đọc đĩa đến 95%.
2.  **Vectorized Query Execution**: ClickHouse xử lý dữ liệu theo từng khối (data blocks) và tận dụng tập lệnh SIMD của CPU để xử lý song song nhiều giá trị số trong một chu kỳ xung nhịp.
3.  **Data Compression**: Dữ liệu lưu trữ theo cột có độ tương đồng cao (ví dụ: cột `region` lặp đi lặp lại chữ 'HN' hoặc 'HCM') được nén bằng thuật toán LZ4 hoặc ZSTD đạt tỷ lệ nén lên tới 70-80%, giảm đáng kể băng thông đọc đĩa I/O.

---

## 7. Khả Năng Mở Rộng Ở Quy Mô Sản Xuất (Production Scalability)

Khi triển khai hệ thống này trên môi trường sản xuất thực tế với hàng tỷ giao dịch, kiến trúc sẽ được thiết kế mở rộng như sau:

```
[POS Terminals] ──► [Load Balancer] ──► [Ingestion API Nodes (Autoscaling)]
                                                 │
                                                 ▼
[Apache Kafka Cluster: 3 Brokers, Partitioned by Region Key]
       │                                         │
       ├─► [ClickHouse Cluster]                  ├─► [Kafka Connect Cluster]
       │   - Distributed Tables                  │   - Multiple S3 Tasks
       │   - ReplicatedMergeTree (ZooKeeper)     │
       ▼                                         ▼
[Trino Coordinator Node] ◄───────────────────────┴──► [MinIO HA Storage / AWS S3]
  └─► [Trino Worker 1]
  └─► [Trino Worker 2] (Autoscaling based on Query Load)
```

### A. Phân Vùng Dữ Liệu Trong Apache Kafka
*   Tăng số lượng Partition cho topic `pos.transactions` (ví dụ: 12 partitions).
*   Sử dụng thuộc tính `region` làm **Partition Key** khi producer gửi tin nhắn. Việc này đảm bảo toàn bộ giao dịch của cùng một vùng địa lý luôn được ghi vào cùng một partition cụ thể, giúp bảo toàn thứ tự giao dịch và phân phối tải đồng đều.

### B. Phân Cụm ClickHouse (ClickHouse Clustering)
*   Sử dụng engine `ReplicatedMergeTree` kết hợp với hệ thống điều phối ClickHouse Keeper (hoặc ZooKeeper) để đồng bộ dữ liệu tự động giữa các bản sao (replicas), nâng cao tính sẵn sàng của dữ liệu khi có node gặp sự cố.
*   Cấu hình bảng phân tán `Distributed` để tự động băm (shard) dữ liệu và phân chia tải ghi/đọc xuống nhiều máy chủ vật lý khác nhau.

### C. Mở Rộng Trino
*   Tách biệt vai trò giữa node điều phối (Trino Coordinator) và các node tính toán (Trino Workers).
*   Áp dụng cơ chế tự động co giãn (Autoscaling) cho các Trino Worker dựa trên mức tiêu thụ CPU và số lượng truy vấn đồng thời trong hàng chờ.

### D. Triển Khai Trên Kubernetes (K8s Deployment)
*   **Strimzi Kafka Operator**: Để quản lý vòng đời và tự động hóa vận hành Kafka cluster trên Kubernetes.
*   **ClickHouse Operator**: Giúp quản lý cấu hình phân cụm ClickHouse, cấu hình ổ đĩa lưu trữ Persistent Volumes và cập nhật phần mềm không gián đoạn dịch vụ.

---

## 8. Vận Hành & Giám Sát (Operations & Observability)

Hệ thống tích hợp sẵn stack giám sát tiêu chuẩn doanh nghiệp để đảm bảo đáp ứng cam kết chất lượng dịch vụ (SLA):

### A. Các Chỉ Số Quan Trọng (Key Metrics)
*   **Kafka Consumer Lag**: Đo lượng tin nhắn tồn đọng trong hàng đợi chưa kịp tiêu thụ bởi ClickHouse và Kafka Connect. Được hiển thị trực tiếp trên dashboard Grafana bằng cách thu thập số liệu qua Kafka Exporter. Cam kết SLA vận hành là lag không quá 10,000 thông điệp.
*   **ClickHouse Ingestion Lag**: Sự khác biệt thời gian giữa cột `timestamp` của giao dịch gốc và cột `insert_time` lúc ghi nhận thành công vào ClickHouse. Mục tiêu đảm bảo trễ đầu-cuối luôn dưới 5 giây.
*   **Trino Query Memory Limit**: Giám sát lượng RAM tiêu thụ khi chạy các câu lệnh JOIN lớn để kịp thời tối ưu hóa hoặc nâng cấp tài nguyên cho các node worker.

### B. Điểm Kết Nối & Port Vận Hành Cục Bộ
*   FastAPI Ingest API: `localhost:8000` (Endpoint tiếp nhận).
*   Kafka UI: `localhost:8080` (Trực quan quản lý Kafka).
*   Kafka Connect REST API: `localhost:8083` (Quản lý connector).
*   MinIO Console: `localhost:9006` (Quản trị tập tin S3).
*   Trino UI Coordinator: `localhost:8090` (Theo dõi truy vấn SQL).
*   Cube.js API & Playground: `localhost:4000` (Phát triển tầng ngữ nghĩa).
*   Grafana Dashboard: `localhost:3000` (Xem kết quả báo cáo).
