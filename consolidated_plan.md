# 🚀 Kế Hoạch Dự Án Tích Hợp — Vinamilk Data Engineer

> **Mục tiêu:** Lấp gap kỹ thuật → Pass CV screening → Nhận Offer
> **Deadline nộp hồ sơ:** 26/07/2026 | **Thời gian còn lại:** ~25 ngày (tính từ 01/07)

---

## 📊 Đánh Giá Hệ Thống Gap vs JD Vinamilk

| Nhóm kỹ năng JD yêu cầu | Tình trạng CV hiện tại | Dự án bù đắp |
|---|---|---|
| ClickHouse / Doris (OLAP Engine) | ❌ Hoàn toàn thiếu | → Project A |
| S3 / Lakehouse (storage layout) | ❌ Chỉ là concept | → Project A |
| Trino (Query Federation) | ❌ Hoàn toàn thiếu | → Project A |
| Cube.js (Metric/Semantic Layer) | ❌ Hoàn toàn thiếu | → Project A |
| OpenMetadata / OpenLineage (Governance) | ❌ Hoàn toàn thiếu | → Project B |
| Kafka + Event Streaming | ✅ Rất mạnh | Đã có |
| Debezium CDC | ✅ Rất mạnh | Đã có |
| dbt, Airflow, ETL/ELT | ✅ Mạnh | Đã có |
| Prometheus / Grafana (Observability) | ✅ Có | Đã có |

> [!IMPORTANT]
> **Kết luận chiến lược:** Cần tạo **2 project mới** tách biệt thay vì 1 project lớn khó trình bày. Mỗi project có GitHub riêng, README chi tiết và README trong CV thể hiện rõ 1 nhóm kỹ năng cốt lõi.

---

## 🔵 Project A: FMCG Real-Time Analytics Platform

> **Từ khóa CV:** `ClickHouse` · `MinIO/S3` · `Apache Iceberg` · `Trino` · `Cube.js` · `Kafka` · `Grafana` · `Docker`

### 🎯 Use Case

Một công ty FMCG quy mô lớn (như Vinamilk) vận hành chuỗi **1.000+ điểm bán (POS)** trên toàn quốc. Mỗi giây có hàng trăm giao dịch mua hàng xảy ra đồng thời. Bộ phận Kinh doanh cần:
1. **Nhìn thấy doanh thu từng phút** để kịp thời điều phối khuyến mãi hoặc bổ sung hàng.
2. **Phân tích xu hướng 2–3 năm** để lập kế hoạch sản xuất theo mùa vụ.
3. **Truy vấn kết hợp** cả dữ liệu tươi (hôm nay) lẫn dữ liệu lịch sử mà không cần chờ ETL nặng.

### ❌ Problem

Hệ thống hiện tại chỉ có PostgreSQL làm DB duy nhất:
- **Bottleneck OLAP:** Báo cáo tổng hợp (aggregation) chạy trên PostgreSQL tranh tài nguyên với giao dịch vận hành, khiến cả 2 đều chậm.
- **Chi phí lưu trữ:** Dữ liệu lịch sử 3 năm (hàng tỷ bản ghi) nếu lưu trên database thì chi phí rất đắt, nhưng nếu xóa đi thì mất khả năng phân tích xu hướng.
- **Silos truy vấn:** Data Analyst phải query 2 hệ thống riêng biệt bằng 2 tool khác nhau, tốn công.

### ✅ Solution & Kiến Trúc

```
                  ┌─[Hot Path]──────────────────────────────────────────────────┐
                  │                                                              │
[POS Event]──►[FastAPI]──►[Kafka]──►[ClickHouse Kafka Engine]──►[ClickHouse]   │
  Generator        │                  (auto-ingest)              (MergeTree +   │
  (mock script)    │                                           Materialized View)│
                  │                                                              │
                  └─[Cold Path]─────────────────────────────────────────────────┘
                  │
                  [Kafka]──►[Kafka Connect S3 Sink]──►[MinIO (S3)]──►[Apache Iceberg]
                                                                           │
                                                                    [Trino Query Engine]
                                                                    (Federated: ClickHouse + Iceberg)
                                                                           │
                                                                    [Cube.js Semantic Layer]
                                                                    (Business metric API)
                                                                           │
                                                                    [Grafana Dashboard]
```

**Giải thích từng layer:**

| Layer | Công nghệ | Mục đích |
|---|---|---|
| Event Generator | Python script / FastAPI | Giả lập 1.000 POS transaction/giây |
| Message Bus | Apache Kafka | Buffer & decouple producers/consumers |
| **Hot Path** | ClickHouse Kafka Engine | Ingest trực tiếp vào OLAP DB, query < 50ms |
| **Cold Path** | MinIO + Apache Iceberg | Lưu trữ S3-compatible, cost-effective, hỗ trợ time-travel |
| Federated Query | Trino | Query cross-system (ClickHouse + Iceberg) bằng 1 SQL |
| Semantic Layer | Cube.js | Định nghĩa metric tập trung: `revenue`, `units_sold`, `avg_basket` |
| Visualization | Grafana | Dashboard realtime + SLA alert |

### 📋 Điểm Học Được & Đưa Vào CV

- **ClickHouse:** Thiết kế MergeTree với `ORDER BY` tối ưu, Monthly partitioning, Materialized View pre-aggregate → query time < 50ms trên 10M+ bản ghi.
- **Apache Iceberg:** Partition evolution, time-travel query, schema evolution không cần rebuild bảng.
- **Trino:** Cấu hình Iceberg catalog + ClickHouse connector, viết Federated Query.
- **Cube.js:** Định nghĩa schema metric, expose REST API → tách biệt business logic khỏi SQL.

### ⏱ Timeline

| Ngày | Việc làm | Output |
|------|------|------|
| 01–02/07 | Học ClickHouse docs: MergeTree, Kafka Engine, Materialized View | Notes |
| 03–04/07 | Docker Compose: Kafka + ClickHouse + mock generator | Hot path chạy được |
| 05–06/07 | Thêm Kafka Connect S3 Sink → MinIO → Iceberg table | Cold path chạy được |
| 07–08/07 | Triển khai Trino, cấu hình connector Iceberg + ClickHouse | Federated query |
| 09–10/07 | Thêm Cube.js + Grafana dashboard | Full stack demo |
| 11/07 | Viết README, architecture diagram, benchmark query | GitHub ready |

---

## 🟢 Project B: Data Platform Governance & Observability Stack

> **Từ khóa CV:** `OpenMetadata` · `OpenLineage` · `Airflow` · `dbt` · `Prometheus` · `AlertManager` · `Grafana Loki` · `Docker`

### 🎯 Use Case

Sau khi hệ thống dữ liệu của Vinamilk mở rộng (nhiều pipeline, nhiều team), xuất hiện vấn đề không thể kiểm soát:
- Data Analyst hỏi: *"Bảng `sales_mart` này lấy dữ liệu từ đâu? Có tin được không?"*
- Data Engineer hỏi: *"Pipeline Airflow chạy lúc 3 giờ sáng bị lỗi, tại sao tôi không biết?"*
- Manager hỏi: *"SLA pipeline 6 giờ sáng có số liệu cho meeting 8 giờ, pipeline có chạy đúng giờ không?"*

### ❌ Problem

- **Thiếu Data Catalog:** Không ai biết schema và nguồn gốc của từng bảng dữ liệu.
- **Thiếu Lineage:** Khi 1 bảng nguồn thay đổi schema, không theo dõi được pipeline nào bị ảnh hưởng.
- **Thiếu Alerting chuẩn:** Airflow gửi email nhưng không có SLO rõ ràng, không phân loại mức độ nghiêm trọng.

### ✅ Solution & Kiến Trúc

```
[Airflow DAGs]──►[OpenLineage Listener]──┐
                                          ├──►[OpenMetadata Server]──►[Web UI: Data Catalog]
[dbt Models]───►[dbt-openlineage]────────┘    (Lineage Graph + Schema + Quality Score)

[Airflow]──►[Prometheus Exporter]──►[Prometheus]──►[Grafana Dashboard]
                                                           │
                                                    [AlertManager]──►[Slack Webhook]
                                                    (SLO: pipeline finish < 6AM)

[Kafka / ClickHouse]──►[Prometheus Exporter]──►[Grafana Loki]
                                                 (Centralized Logging)
```

### 📋 Điểm Học Được & Đưa Vào CV

- **OpenMetadata:** Deploy catalog, ingest từ PostgreSQL/ClickHouse, xem schema tự động.
- **OpenLineage:** Kết nối Airflow + dbt → auto-generate lineage graph end-to-end.
- **SLO/SLA Dashboard:** Định nghĩa SLO (pipeline hoàn thành trong X phút), alert khi vi phạm.
- **Grafana Loki:** Centralize logs từ Airflow + Kafka, query bằng LogQL.

### ⏱ Timeline

| Ngày | Việc làm | Output |
|------|------|------|
| 12–13/07 | Deploy OpenMetadata Docker Compose, ingest PostgreSQL/ClickHouse metadata | Catalog web UI |
| 14–15/07 | Cài airflow-openlineage provider, test lineage graph từ Airflow DAGs | Lineage graph |
| 16/07 | Tích hợp dbt-openlineage → dbt model lineage | dbt lineage visible |
| 17–18/07 | Cấu hình Prometheus Airflow exporter + SLO alert rule + AlertManager Slack | SLO alert chạy |
| 19/07 | Thêm Grafana Loki, ship Airflow logs | Centralized logging |
| 20/07 | Viết README, capture screenshots, push GitHub | GitHub ready |

---

## 📋 PSR Summary — Hai Dự Án Mới Viết Vào CV

### Project A — FMCG Real-Time Analytics Platform
> *"Built an open-source analytics platform for FMCG retail data using Kafka event streaming, ClickHouse OLAP (Hot path with Materialized Views), and Apache Iceberg on MinIO (Cold path). Deployed Trino as a federated query engine to unify real-time and historical data access, and Cube.js to expose centralized business metrics as REST API. Achieved sub-50ms OLAP query performance on 10M+ records vs. 8s on PostgreSQL baseline."*

### Project B — Data Platform Governance & Observability Stack
> *"Deployed an enterprise-grade data governance stack integrating OpenMetadata as a centralized data catalog (auto-ingesting schema from PostgreSQL and ClickHouse) and OpenLineage to automatically track end-to-end lineage across Airflow pipelines and dbt models. Implemented SLO-based alerting (AlertManager + Slack) and centralized log aggregation (Grafana Loki) for pipeline reliability monitoring."*

---

## 🗓 Lộ Trình Tổng Hợp (01/07 → 26/07)

```
Tuần 1 (01–07/07): Project A — Hot Path
   ClickHouse + Kafka + Event Generator + Grafana baseline

Tuần 2 (08–11/07): Project A — Cold Path + Full Stack
   MinIO + Iceberg + Trino + Cube.js → README + push GitHub

Tuần 2–3 (12–20/07): Project B — Governance & Observability
   OpenMetadata + OpenLineage + SLO Alerting + Loki → push GitHub

Tuần 3 (21/07): CV Finalize
   Cập nhật CV: thêm 2 project mới, viết lại AFFINA bullets theo PSR

Tuần 4 (22–26/07): Polish & Submit
   README review, end-to-end test, nộp hồ sơ ngày 26/07
```

---

## 🔑 Nguyên Tắc Thực Thi

> [!TIP]
> **Chỉ đưa ClickHouse, MinIO, Trino, Cube.js vào CV sau khi bạn thực sự chạy được nó** — dù chỉ là local Docker. Nếu phỏng vấn hỏi "Anh/chị dùng ClickHouse MergeTree thế nào?" mà không trả lời được, điểm âm nặng hơn là không có skill đó.

> [!NOTE]
> **Tên GitHub repos gợi ý:**
> - `fmcg-realtime-analytics-platform` (Project A)
> - `data-platform-governance-stack` (Project B)

*Cập nhật: 01/07/2026*
