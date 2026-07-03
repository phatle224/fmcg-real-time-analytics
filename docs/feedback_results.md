# Báo Cáo Kết Quả Kiểm Thử Tích Hợp Hệ Thống (Integration Test Report)

Báo cáo này được tự động tạo lập từ script kiểm thử tích hợp thực tế trên môi trường Docker cục bộ để xác nhận độ tin cậy và hiệu năng của toàn bộ đường ống dữ liệu (Data Pipeline).

---

## 1. Trạng Thái Hoạt Động Các Dịch Vụ (Service Health Status)

| Dịch Vụ / Component | Endpoint Kiểm Tra | Trạng Thái | Mô Tả |
| :--- | :--- | :--- | :--- |
| **FastAPI Ingestion** | `http://localhost:8000/health` | ✅ Hoạt động | API phản hồi khỏe mạnh, kết nối Kafka Broker ổn định. |
| **Apache Kafka** | `kafka:29092` | ✅ Hoạt động | Topic `pos.transactions` phân phối tin nhắn chính xác. |
| **ClickHouse OLAP** | `localhost:8123` | ✅ Hoạt động | Tìm thấy giao dịch TX-TEST-1783094568 (Product: Vinamilk Strawberry Yogurt, Total Amount: 5.00 USD, Region: HCM). |
| **MinIO & Iceberg** | `localhost:9006 / 8060` | ✅ Hoạt động | Lưu trữ Parquet trên S3 và đồng bộ thông tin qua Metastore. |
| **Trino Query Engine** | `localhost:8060` | ✅ Hoạt động | Đọc và tối ưu hóa câu lệnh SQL từ ClickHouse và Iceberg. |

---

## 2. Kịch Bản Kiểm Thử Gửi Giao Dịch Đơn Lẻ (Single Event Ingestion)

*   **Mã giao dịch thử nghiệm:** `TX-TEST-1783094568`
*   **Chi tiết giao dịch:**
    *   **Điểm bán (POS ID):** `POS-HCM-101`
    *   **Sản phẩm:** `Vinamilk Strawberry Yogurt` (Danh mục: `yogurt`)
    *   **Số lượng:** 10 hộp
    *   **Thành tiền:** 5.00 USD
    *   **Khu vực:** HCM (Loại hình cửa hàng: siêu thị)

### Kết quả truyền dẫn:
1.  **FastAPI POST `/api/v1/events`:**
    *   Trạng thái trả về: `200 OK` (Thành công).
    *   Phản hồi JSON: `{"status":"success","transaction_id":"TX-TEST-1783094568"}`.
2.  **ClickHouse raw table query:**
    *   Trạng thái: ✅ Đã tìm thấy bản ghi.
    *   Chi tiết: Product: Vinamilk Strawberry Yogurt, Total Amount: 5.00 USD, Region: HCM

---

## 3. Thống Kê Số Lượng Bản Ghi Trên Toàn Stack

*   **ClickHouse (Hot Path - Lưu trữ nóng 30 ngày):**
    *   Số lượng dòng dữ liệu: **4** bản ghi.
*   **Apache Iceberg (Cold Path - Lưu trữ lạnh lịch sử):**
    *   Số lượng dòng dữ liệu nén Parquet: **12000** bản ghi.

---

## 4. Kiểm Thử Truy Vấn Liên Kết Đa Nguồn (Trino Federated Query)

Hệ thống đã thực hiện thành công câu lệnh `UNION ALL` gộp dữ liệu nóng từ ClickHouse và dữ liệu lạnh lịch sử từ Iceberg (MinIO).

### Top 5 Doanh Thu Theo Khu Vực & Danh Mục (JOIN ClickHouse + Iceberg):
```sql
WITH combined AS (
    SELECT 
        from_utf8(region) AS region, 
        from_utf8(category) AS category, 
        CAST(total_amount AS DOUBLE) AS total_amount
    FROM clickhouse.default.pos_transactions_trino_view
    UNION ALL
    SELECT region, category, total_amount FROM iceberg.fmcg.pos_transactions_historical
)
SELECT region, category, sum(total_amount) AS total_revenue
FROM combined
GROUP BY region, category
ORDER BY total_revenue DESC
LIMIT 5;
```

**Bảng kết quả trả về từ Trino:**

| Thứ Hạng | Khu Vực (Region) | Danh Mục Sản Phẩm (Category) | Tổng Doanh Thu (USD) |
| :---: | :---: | :---: | :---: |
| #1 | HCM | dairy | $1,494,506,500.00 |
| #2 | HN | dairy | $1,165,733,007.50 |
| #3 | DN | dairy | $540,846,000.00 |
| #4 | CT | dairy | $412,020,000.00 |
| #5 | HP | dairy | $322,942,500.00 |

---

## 5. Kết Luận Kiểm Thử (Final Verdict)

*   **Tính sẵn sàng:** 100% dịch vụ hoạt động ổn định và cấu hình đúng cổng mạng.
*   **Đường dẫn Hot Path:** Hoạt động đúng thiết kế, ClickHouse Kafka Engine tự động phân giải JSON và lưu vào MergeTree thông qua Materialized View tức thời.
*   **Đường dẫn Cold Path:** Kafka Connect S3 Sink hoạt động chính xác, dữ liệu được phân vùng dưới dạng tệp Parquet và cập nhật thông tin metadata của Iceberg.
*   **Trino Federated Query:** Khả năng kết nối JDBC ClickHouse và Iceberg hoạt động hoàn hảo, khắc phục thành công vấn đề tương thích kiểu dữ liệu bằng hàm `from_utf8`.

**Verdict: PASS** ✅
