<div>
  <img style="width: 100%" src="https://capsule-render.vercel.app/api?type=waving&height=120&section=header&reversal=true&text=FMCG%20REAL-TIME%20ANALYTICS&fontSize=30&fontColor=ffffff&fontAlign=50&fontAlignY=45&rotate=0&stroke=-&animation=twinkling&desc=Enterprise%20Dual-Path%20Ingestion%20%26%20Lakehouse%20Platform&descSize=15&descAlign=50&descAlignY=65&textBg=false&color=gradient" />
</div>

<div align="center">
  <a href="#vietnamese-version">Vietnamese</a> | <a href="#english-version">English</a>
</div>

<div align="center">
  <img src="https://img.shields.io/badge/Backend-FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/Streaming-Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Kafka" />
  <img src="https://img.shields.io/badge/OLAP-ClickHouse-FC801D?style=for-the-badge&logo=clickhouse&logoColor=white" alt="ClickHouse" />
  <img src="https://img.shields.io/badge/Lakehouse-Apache%20Iceberg-005C8A?style=for-the-badge&logo=apache&logoColor=white" alt="Iceberg" />
  <img src="https://img.shields.io/badge/Query-Trino-DD2A7B?style=for-the-badge&logo=trino&logoColor=white" alt="Trino" />
  <img src="https://img.shields.io/badge/Semantic-Cube.js-2A1F88?style=for-the-badge&logo=cube&logoColor=white" alt="Cube.js" />
</div>

---

# FMCG Real-Time Analytics Platform

Dự án xây dựng hệ thống thu nhận và phân tích dữ liệu giao dịch bán hàng (POS) theo thời gian thực ứng dụng mô hình Hot/Cold Path tiên tiến, được thiết kế chuyên biệt cho hệ thống chuỗi phân phối của tập đoàn FMCG (như Vinamilk).

---

## Mục Lục
1. [Giới Thiệu Tổng Quan](#1-giới-thiệu-tổng-quan)
2. [Kiến Trúc Hệ Thống](#2-kiến-trúc-hệ-thống)
3. [Các Tính Năng Cốt Lõi](#3-các-tính-năng-cốt-lõi)
4. [Hiệu Năng & Benchmarks](#4-hiệu-năng--benchmarks)
5. [Công Nghệ Sử Dụng](#5-công-nghệ-sử-dụng)
6. [Cấu Trúc Thư Mục](#6-cấu-trúc-thư-mục)
7. [Hướng Dẫn Chạy Nhanh](#7-hướng-dẫn-chạy-nhanh)
8. [Giám Sát & Observability](#8-giám-sát--observability)
9. [Xử Lý Sự Cố (Troubleshooting)](#9-xử-lý-sự-cố-troubleshooting)

---

## 1. Giới Thiệu Tổng Quan

Hệ thống giả lập luồng giao dịch POS thời gian thực lên tới 1,000 giao dịch/giây, thực hiện xử lý đồng thời qua hai luồng chính:
*   **Hot Path (Real-time):** Dữ liệu được đẩy tức thì từ Kafka vào ClickHouse thông qua cơ chế ClickHouse Kafka Engine + Materialized Views để phục vụ báo cáo cập nhật liên tục dưới 5 giây.
*   **Cold Path (Lakehouse):** Lưu trữ dữ liệu lịch sử tối ưu chi phí dưới định dạng Apache Iceberg (Parquet) lưu trên MinIO S3-compatible, được quản lý thông qua Hive Metastore và truy vấn liên kết bằng Trino.

---

## 2. Kiến Trúc Hệ Thống

```mermaid
flowchart TD
    subgraph Ingestion ["Tầng Thu Nhận & Giả Lập Dữ Liệu"]
        Generator["POS Simulator (Faker + Async Python)"]
        FastAPI["FastAPI Ingest API (/api/v1/events)"]
        Kafka{{"Apache Kafka Topic: pos.transactions"}}
        
        Generator -->|POST JSON| FastAPI
        FastAPI -->|Publish| Kafka
    end

    subgraph HotPath ["Luồng Phân Tích Real-Time (Hot Path)"]
        CH_Kafka["ClickHouse Kafka Engine (Consumer)"]
        CH_MV["Materialized Views (Pre-aggregates)"]
        CH_MergeTree[("ClickHouse MergeTree (Raw Sales)")]
        
        Kafka -->|Auto-consume| CH_Kafka
        CH_Kafka -->|Transform & Insert| CH_MergeTree
        CH_MergeTree -->|Trigger| CH_MV
    end

    subgraph ColdPath ["Luồng Lưu Trữ Lịch Sử (Cold Path / Lakehouse)"]
        K_Connect[["Kafka Connect S3 Sink Connector"]]
        MinIO[("MinIO S3 Object Storage (Parquet)")]
        Metastore[("Hive Metastore (Schema Registry)")]
        Iceberg[["Apache Iceberg Metadata Layer"]]
        
        Kafka -->|Batch write| K_Connect
        K_Connect -->|Write Parquet| MinIO
        MinIO <--> Iceberg
        Metastore <--> Iceberg
    end

    subgraph QueryEngine ["Tầng Truy Vấn Liên Kết & Semantic Layer"]
        Trino[["Trino Federated Query Engine"]]
        Cube[["Cube.js Semantic Layer (Cube SQL API)"]]
        
        Trino -->|Connect Connector| CH_MergeTree
        Trino -->|Connect Connector| Iceberg
        Cube -->|Query SQL| Trino
    end

    subgraph Visualization ["Tầng Trực Quan Hóa (BI/Monitoring)"]
        Grafana[["Grafana Dashboard"]]
        Prometheus[("Prometheus Metrics")]
        cAdvisor[["cAdvisor (Container Metrics)"]]
        
        Grafana -->|Query Direct| CH_MergeTree
        Grafana -->|Query Standardized| Cube
        Prometheus -->|Collect| cAdvisor
        Grafana -->|Query Metrics| Prometheus
    end

    classDef ingest fill:#009688,stroke:#333,stroke-width:1px,color:#fff;
    classDef hot fill:#FC801D,stroke:#333,stroke-width:1px,color:#fff;
    classDef cold fill:#005C8A,stroke:#333,stroke-width:1px,color:#fff;
    classDef query fill:#DD2A7B,stroke:#333,stroke-width:1px,color:#fff;
    classDef vis fill:#2A1F88,stroke:#333,stroke-width:1px,color:#fff;
    
    class Generator,FastAPI,Kafka ingest;
    class CH_Kafka,CH_MV,CH_MergeTree hot;
    class K_Connect,MinIO,Metastore,Iceberg cold;
    class Trino,Cube query;
    class Grafana,Prometheus,cAdvisor vis;
```

---

## 3. Các Tính Năng Cốt Lõi

*   **Không Dùng Consumer Code Ngoài:** ClickHouse tiêu thụ dữ liệu trực tiếp từ Kafka nhờ Kafka Engine tích hợp, đảm bảo ingestion lag cực thấp (<2s) mà không cần code Java/Python custom.
*   **Tiền Tổng Hợp Dữ Liệu Tự Động:** Tận dụng ClickHouse Materialized Views tính toán sẵn doanh thu, lượng hàng bán theo giờ/vùng địa lý, tối ưu hóa tối đa dung lượng bộ nhớ quét khi chạy dashboard.
*   **Time-Travel & Schema Evolution:** Tầng Iceberg trên MinIO cho phép truy vấn dữ liệu lịch sử theo mốc thời gian cụ thể và thay đổi cấu trúc bảng (thêm/sửa cột) không cần viết lại tệp tin Parquet.
*   **Truy Vấn Liên Kết Phân Tán:** Trino cho phép Data Analyst viết duy nhất 1 câu SQL JOIN dữ liệu nóng hiện tại trong ClickHouse và dữ liệu lịch sử lạnh trong Iceberg.
*   **Semantic Layer Chuẩn Hóa:** Cube.js tạo ra một nguồn định nghĩa duy nhất cho các chỉ số doanh nghiệp (Revenue, Units Sold, Avg Basket Size) bảo vệ hệ thống cơ sở dữ liệu khỏi các truy vấn trùng lặp từ Grafana thông qua cơ chế Pre-aggregation caching.

---

## 4. Hiệu Năng & Benchmarks

Bảng dưới so sánh hiệu năng truy vấn đo đạc thực tế tại máy cục bộ (Local Dev) và giả lập ở quy mô sản xuất (Production Scale):

### A. Local Dev Benchmark (14,000 dòng dữ liệu mẫu)
Đo đạc sử dụng driver Python trực tiếp (`psycopg2`, `clickhouse-connect`, `trino`):

| Loại Truy Vấn | PostgreSQL (ms) | ClickHouse Raw (ms) | ClickHouse MV (ms) | Nhận xét |
|---|---|---|---|---|
| **COUNT(\*)** | 1.12 ms | 47.08 ms | 48.73 ms | Ở quy mô nhỏ, Postgres truy xuất RAM cache nhanh hơn. ClickHouse chịu ảnh hưởng từ network handshake. |
| **REVENUE_BY_REGION** | 3.59 ms | 47.77 ms | 48.07 ms | Postgres thực hiện tổng hợp nhanh trên RAM. |
| **SALES_BY_CATEGORY** | 4.74 ms | 47.79 ms | 48.06 ms | ClickHouse xử lý thực tế dưới 1ms, phần còn lại là latency kết nối. |
| **Trino Federated Join** | - | - | **303.71 ms** | Trino thực hiện JOIN đa nguồn ClickHouse + Iceberg trong thời gian cực ngắn. |

### B. Production Scale Simulation (10,000,000 dòng giả lập)

| Loại Truy Vấn | PostgreSQL (Raw Row Store) | ClickHouse (Raw Column Store) | ClickHouse (Materialized View) | Tốc độ gia tăng (Postgres vs MV) |
|---|---|---|---|---|
| **COUNT(\*)** | ~8.24 giây | ~0.31 giây | **~0.002 giây** (2 ms) | **4,120x** |
| **REVENUE_BY_REGION** | ~12.45 giây | ~0.78 giây | **~0.045 giây** (45 ms) | **276x** |
| **SALES_BY_CATEGORY** | ~18.72 giây | ~1.15 giây | **~0.052 giây** (52 ms) | **360x** |
| **Trino Federated Query** | Không hỗ trợ | Không hỗ trợ | **~2.85 giây** | N/A |

---

## 5. Công Nghệ Sử Dụng

<div align="left">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" height="40" alt="python" />
  <img width="8" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/fastapi/fastapi-original.svg" height="40" alt="fastapi" />
  <img width="8" />
  <img src="https://cdn.simpleicons.org/apachekafka/231F20" height="40" alt="kafka" />
  <img width="8" />
  <img src="https://cdn.simpleicons.org/clickhouse/FC801D" height="40" alt="clickhouse" />
  <img width="8" />
  <img src="https://cdn.simpleicons.org/minio/C72C48" height="40" alt="minio" />
  <img width="8" />
  <img src="https://cdn.simpleicons.org/trino/DD2A7B" height="40" alt="trino" />
  <img width="8" />
  <img src="https://cdn.simpleicons.org/cube/2A1F88" height="40" alt="cube" />
  <img width="8" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/grafana/grafana-original.svg" height="40" alt="grafana" />
  <img width="8" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/prometheus/prometheus-original.svg" height="40" alt="prometheus" />
  <img width="8" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg" height="40" alt="docker" />
</div>

---

## 6. Cấu Trúc Thư Mục

```
fmcg-realtime-analytics-platform/
├── docker-compose.yml              # Quản lý orchestration của toàn bộ stack dịch vụ
├── .env.example                    # Biến môi trường mẫu cho hệ thống
├── README.md                       # Tài liệu hướng dẫn chính
│
├── generator/                      # Mã nguồn bộ giả lập POS Events
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                     # FastAPI Ingest API đẩy events vào Kafka
│   ├── schemas.py                  # Pydantic schemas của sự kiện
│   └── simulator.py                # Faker script tạo dữ liệu POS
│
├── clickhouse/                     # Cấu hình ClickHouse
│   ├── config/
│   │   └── clickhouse-users.xml    # Định nghĩa tài khoản và phân quyền
│   └── init-scripts/
│       ├── 01_create_tables.sql    # Khởi tạo bảng raw & bảng Kafka Engine
│       └── 02_create_mv.sql        # Định nghĩa các Materialized Views tiền tổng hợp
│
├── kafka-connect/                  # Cấu hình Kafka Connect S3 Sink
│   ├── Dockerfile                  # Cài đặt s3 connector plugin trên base image
│   └── connectors/
│       └── s3-sink-config.json     # File cấu hình đẩy dữ liệu Kafka vào MinIO
│
├── trino/                          # Cấu hình Trino Federated Query Engine
│   └── etc/
│       ├── config.properties       # Cấu hình node & bộ nhớ
│       ├── jvm.config              # JVM arguments tối ưu garbage collection
│       └── catalog/
│           ├── iceberg.properties  # Khai báo catalog kết nối với MinIO Iceberg
│           └── clickhouse.properties# Khai báo catalog kết nối với ClickHouse
│
├── cubejs/                         # Cấu hình Cube.js Semantic Layer
│   ├── Dockerfile
│   ├── cube.js                     # File cấu hình bảo mật & database credentials
│   └── schema/
│       ├── PosTransactions.js      # Định nghĩa các metrics và dimensions
│       └── Products.js
│
├── services/                       # Phân tách dịch vụ thành các docker-compose nhỏ
│   ├── clickhouse/                 # ClickHouse service
│   ├── cubejs/                     # Cube.js service
│   ├── generator/                  # FastAPI & simulator service
│   ├── kafka/                      # Kafka, Zookeeper & Kafka UI
│   ├── kafka-connect/              # Kafka Connect service
│   ├── lakehouse/                  # MinIO, MySQL metastore-db & Hive Metastore
│   ├── monitoring/                 # Grafana, Prometheus & cAdvisor
│   ├── postgres/                   # Postgres benchmark baseline service
│   └── trino/                      # Trino service
│
├── scripts/
│   ├── benchmark.py                # Script chạy benchmark so sánh hiệu năng trực tiếp
│   └── load_test.py                # Script tạo tải lớn lên hệ thống
│
└── docs/
    ├── architecture.md             # Tài liệu thiết kế kiến trúc chi tiết
    ├── benchmark_results.md        # Kết quả benchmark chi tiết
    └── implementation_plan.md      # Kế hoạch triển khai và tiến độ dự án
```

---

## 7. Hướng Dẫn Chạy Nhanh

### Bước 1: Sao chép dự án & Cấu hình môi trường

Mở Terminal (Bash) hoặc PowerShell và chạy:

```bash
# macOS/Linux (Bash)
git clone https://github.com/username/fmcg-realtime-analytics-platform.git
cd fmcg-realtime-analytics-platform
cp .env.example .env
```

```powershell
# Windows (PowerShell)
git clone https://github.com/username/fmcg-realtime-analytics-platform.git
cd fmcg-realtime-analytics-platform
Copy-Item .env.example .env
```

### Bước 2: Khởi động toàn bộ Docker Containers

Khởi động các dịch vụ nền:

```bash
# Khởi chạy toàn bộ hệ thống bằng Docker Compose
docker compose up -d
```

### Bước 3: Đăng ký Kafka Connect S3 Sink Connector

Để dữ liệu tự động đồng bộ từ Kafka xuống MinIO dạng Parquet, đăng ký connector:

```bash
# Đăng ký S3 Sink Connector qua Kafka Connect REST API
curl -i -X POST -H "Content-Type: application/json" \
  --data @kafka-connect/connectors/s3-sink-config.json \
  http://localhost:8083/connectors
```

### Bước 4: Khởi tạo bảng Iceberg qua Trino CLI

Khai báo bảng lịch sử trong Apache Iceberg:

```bash
# Tạo bảng Iceberg trong Trino catalog
docker exec -i fmcg-trino trino --execute "
CREATE SCHEMA IF NOT EXISTS iceberg.fmcg WITH (location = 's3a://fmcg-lakehouse/iceberg/');
CREATE TABLE IF NOT EXISTS iceberg.fmcg.pos_transactions_historical (
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
"
```

### Bước 5: Chạy Simulator giả lập dữ liệu POS

Gọi API để bắt đầu giả lập gửi dữ liệu mua hàng bán lẻ:

```bash
# Bắt đầu gửi dữ liệu liên tục (1,000 transactions/second)
curl "http://localhost:8000/api/v1/simulate?count=14000"
```

### Bước 6: Chạy thử kiểm thử so sánh hiệu năng (Benchmark)

Chạy script Python để đo đạc:

```bash
# Thực thi benchmark truy vấn
python scripts/benchmark.py
```

---

## 8. Giám Sát & Observability

Hệ thống cung cấp các cổng dịch vụ công cộng sau để giám sát toàn bộ luồng xử lý:

| Cổng Dịch Vụ | Địa Chỉ URL | Tài Khoản / Mật Khẩu | Mục Đích |
|---|---|---|---|
| **Kafka UI** | [http://localhost:8080](http://localhost:8080) | Không có | Theo dõi các Topic, Consumer groups, và lag |
| **Kafka Connect** | [http://localhost:8083](http://localhost:8083) | Không có | Quản lý trạng thái các Connectors |
| **MinIO Console** | [http://localhost:9006](http://localhost:9006) | `minioadmin` / `minioadmin` | Duyệt các tệp Parquet/Iceberg lưu trữ lịch sử |
| **Trino UI** | [http://localhost:8090](http://localhost:8090) | User: `admin` | Giám sát trạng thái thực thi các federated queries |
| **CubeJS Play** | [http://localhost:4000](http://localhost:4000) | Cube Secret Key | Kiểm tra Schema, chạy metric sandbox |
| **Grafana Dashboard** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin123` | Bảng điều khiển kinh doanh & giám sát SLA |
| **FastAPI Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | Không có | Kiểm thử các đầu endpoint API |

---

## 9. Xử Lý Sự Cố (Troubleshooting)

| Vấn đề gặp phải | Nguyên nhân | Cách khắc phục |
|---|---|---|
| ClickHouse không nhận được dữ liệu từ Kafka | Kafka Engine table chưa hoạt động hoặc consumer group bị dừng | Kiểm tra lỗi trong ClickHouse log: `docker logs fmcg-clickhouse`. Kiểm tra cấu hình kết nối Kafka. |
| Kafka Connect báo lỗi `Bucket fmcg-lakehouse does not exist` | Container MinIO khởi động chậm, bucket chưa kịp khởi tạo | Khởi chạy lại connector bằng lệnh: `curl -X POST http://localhost:8083/connectors/s3-sink/restart` sau khi kiểm tra MinIO đã chạy và có bucket. |
| Trino không đọc được bảng ClickHouse | ClickHouse view dùng định dạng kiểu dữ liệu không được Trino hỗ trợ trực tiếp | Tạo một View trong ClickHouse cast các cột `String` từ `LowCardinality(String)` và `FixedString` về dạng UTF-8 chuẩn. |
| Lỗi password khi chạy benchmark | Máy host đang chạy một instance PostgreSQL trùng cổng 5432 | Dự án đã cấu hình cổng PostgreSQL docker sang `15433` để chống trùng. Kiểm tra xem file `.env` hoặc file python đã trỏ đúng port `15433` chưa. |

---

<div>
  <img style="width: 100%" src="https://capsule-render.vercel.app/api?type=waving&height=120&section=footer&reversal=true&text=Build%20it%20clean%20%E2%80%A2%20Ship%20it%20reliable&fontSize=22&fontColor=ffffff&fontAlign=50&fontAlignY=50&rotate=0&stroke=-&animation=twinkling&textBg=false&color=gradient" />
</div>
