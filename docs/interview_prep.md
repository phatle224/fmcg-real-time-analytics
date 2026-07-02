# 🧠 Vinamilk Data Engineer Interview Preparation Guide

Tài liệu này tổng hợp các câu hỏi phỏng vấn kỹ thuật chuyên sâu và các câu trả lời tương ứng dựa trên kiến trúc của dự án **FMCG Real-Time Analytics Platform**. Mục tiêu là giúp ứng viên tự tin trả lời phỏng vấn, chứng minh năng lực thực tế phù hợp với JD của Vinamilk.

---

## 1. Các Câu Hỏi Kiến Trúc Hệ Thống (Architecture & Design)

### Q1: "Tại sao bạn thiết kế kiến trúc Dual-Path (Hot/Cold Path) thay vì đẩy toàn bộ dữ liệu vào ClickHouse để lưu trữ lâu dài?"
*   **Câu trả lời:**
    *   **Tối ưu hóa chi phí (Cost Efficiency):** ClickHouse yêu cầu tài nguyên lưu trữ hiệu năng cao (SSD/NVMe) để phục vụ các truy vấn phân tích thời gian thực cực nhanh. Nếu lưu trữ toàn bộ dữ liệu lịch sử nhiều năm trong ClickHouse, chi phí phần cứng sẽ cực kỳ đắt đỏ. Bằng cách đẩy dữ liệu lịch sử sang MinIO (hoặc AWS S3 trên production) dưới dạng Parquet nén sâu, chi phí lưu trữ giảm từ 5 đến 8 lần.
    *   **Tối ưu hóa hiệu năng (Query Isolation):** Tách biệt dữ liệu nóng (phục vụ dashboard real-time trong ngày với tần suất query cao) khỏi dữ liệu lạnh (phục vụ phân tích xu hướng dài hạn, chạy báo cáo tuần/tháng với lượng dữ liệu lớn). Điều này giúp tránh việc các truy vấn lịch sử nặng làm nghẽn tài nguyên CPU/RAM của ClickHouse dành cho hot path.
    *   **Mở rộng khả năng tích hợp (Lakehouse Openness):** Lưu trữ dữ liệu lịch sử dưới định dạng chuẩn mở Apache Iceberg giúp các hệ thống khác (như Apache Spark cho AI/ML, Flink cho streaming, dbt cho batch processing) dễ dàng truy cập và khai thác chung mà không phụ thuộc vào ClickHouse.

### Q2: "Tại sao bạn lựa chọn Apache Iceberg thay vì lưu trữ các tệp Parquet thuần túy (Raw Parquet) trên S3?"
*   **Câu trả lời:**
    *   Raw Parquet trên S3 gặp các vấn đề lớn khi chạy ở môi trường sản xuất như: không có tính ACID (dẫn đến ghi trùng lặp hoặc mất dữ liệu khi concurrent writes), không hỗ trợ schema evolution, và hiệu năng truy vấn thấp do phải quét toàn bộ thư mục (Directory Listing).
    *   **Apache Iceberg giải quyết triệt để nhờ metadata layer:**
        1.  **ACID Transactions:** Đảm bảo các tiến trình ghi dữ liệu (như Kafka Connect S3 Sink) diễn ra an toàn. Người dùng sẽ không bao giờ đọc phải dữ liệu rác hay dữ liệu chưa ghi xong.
        2.  **Schema Evolution:** Cho phép thêm, đổi tên, hoặc xóa cột mà không cần chạy ETL để ghi đè lại toàn bộ tệp Parquet cũ.
        3.  **Partition Evolution:** Hỗ trợ thay đổi sơ đồ phân vùng (ví dụ: chuyển từ phân vùng theo Region sang phân vùng theo Region + Ngày) mà dữ liệu cũ không bị ảnh hưởng.
        4.  **Hidden Partitioning:** Trino tự động nhận biết phân vùng mà không bắt buộc người dùng phải chỉ định cột phân vùng trong mệnh đề WHERE, tránh các truy vấn quét toàn bộ đĩa đắt đỏ.
        5.  **Time Travel:** Hỗ trợ truy vấn lịch sử trạng thái của bảng tại một mốc thời gian cụ thể: `FOR TIMESTAMP AS OF ...`.

---

## 2. Các Câu Hỏi Về ClickHouse (Real-Time OLAP)

### Q3: "ClickHouse Kafka Engine hoạt động như thế nào? Tại sao cần Materialized View làm cầu nối?"
*   **Câu trả lời:**
    *   `Kafka Engine` trong ClickHouse là một công cụ bảng đặc biệt (Special Table Engine). Nó không lưu trữ dữ liệu mà đóng vai trò như một Kafka Consumer tích hợp sẵn trong database engine. Nó liên tục kết nối với Kafka topic, đọc tin nhắn và duy trì offset.
    *   **Tại sao cần Materialized View:**
        *   Bảng Kafka Engine chỉ có thể đọc được một lần (giống như đọc một luồng tin nhắn). Khi bạn chạy câu lệnh `SELECT` trực tiếp trên bảng này, dữ liệu sẽ biến mất khỏi buffer.
        *   Do đó, chúng ta cần một `Materialized View` đóng vai trò là một Trigger liên tục. Mỗi khi có message mới chảy vào bảng Kafka Engine, Materialized View sẽ tự động bắt lấy, thực hiện các biến đổi (như ép kiểu, giải nén JSON) rồi chèn (INSERT) dữ liệu đó vào bảng lưu trữ vật lý thực sự (bảng MergeTree).
        *   Điều này giúp hệ thống tự động thu nhận hàng chục ngàn dòng/giây mà không cần viết các ứng dụng Consumer trung gian bằng Python hay Java, giảm thiểu tối đa điểm lỗi (Point of Failure) và Ingestion Lag (<2s).

### Q4: "Cách bạn thiết kế khóa ORDER BY trong ClickHouse MergeTree? Bạn tối ưu nó như thế nào?"
*   **Câu trả lời:**
    *   Trong ClickHouse, khóa `ORDER BY` quyết định cách dữ liệu được sắp xếp vật lý trên đĩa và xác định chỉ mục chính (Primary Index). Nó khác hoàn toàn với Primary Key trong các RDBMS như PostgreSQL (vốn dùng để đảm bảo tính duy nhất).
    *   **Quy tắc thiết kế:**
        1.  Đặt các cột có độ chọn lọc thấp (Low Cardinality) và thường xuyên được lọc trong mệnh đề WHERE lên trước (ví dụ: `region`, `store_type`).
        2.  Đặt các cột có độ chọn lọc cao hơn ở phía sau (ví dụ: `product_id`).
        3.  Luôn đưa cột thời gian đã được làm tròn (`toStartOfHour(timestamp)`) vào khóa để tối ưu các truy vấn lọc theo khoảng thời gian.
        4.  Tránh đưa quá nhiều cột vào khóa `ORDER BY` vì nó sẽ làm phình chỉ mục trong RAM (ClickHouse lưu chỉ mục chính hoàn toàn trên RAM).
    *   Trong dự án này, khóa được thiết kế là: `ORDER BY (region, product_id, toStartOfHour(timestamp))`. Thiết kế này giúp ClickHouse dễ dàng định vị các block dữ liệu cần đọc và bỏ qua các block không liên quan khi người dùng lọc theo khu vực hoặc thời gian, tăng tốc độ truy vấn lên hàng trăm lần.

---

## 3. Các Câu Hỏi Về Trino & Cube.js (Federation & Semantic Layer)

### Q5: "Trino xử lý các truy vấn liên kết (Federated Queries) như thế nào để đảm bảo tốc độ phản hồi nhanh?"
*   **Câu trả lời:**
    *   Trino là một công cụ truy vấn phân tán kiểu MPP (Massively Parallel Processing). Khi nhận một câu lệnh SQL liên kết như JOIN ClickHouse và Iceberg:
        1.  **Phân tích & Tối ưu hóa (Query Planner):** Trino phân tách câu lệnh thành các phân đoạn logic.
        2.  **Đẩy truy vấn xuống nguồn (Predicate Pushdown):** Trino tự động đẩy các phép lọc (WHERE) và phép chiếu (SELECT) xuống ClickHouse để ClickHouse tự thực hiện tính toán cục bộ và chỉ trả về lượng kết quả đã thu gọn qua mạng.
        3.  **Quét song song tệp Parquet (Iceberg Connector):** Với tầng Iceberg trên MinIO, Trino đọc trực tiếp các tệp tin metadata để xác định đúng các tệp Parquet cần thiết, sau đó phân chia cho nhiều worker nodes đọc song song mà không cần thông qua bất kỳ API trung gian nào.
        4.  **Thực thi JOIN trên bộ nhớ (In-Memory Join):** Các worker nodes của Trino nhận luồng dữ liệu từ cả hai connector ClickHouse và Iceberg, sau đó thực hiện thuật toán Hash Join trực tiếp trên RAM để trả kết quả về cho client.

### Q6: "Tại sao bạn sử dụng Cube.js làm Semantic Layer thay vì cho Grafana truy vấn trực tiếp ClickHouse hay Trino?"
*   **Câu trả lời:**
    *   **Chuẩn hóa định nghĩa chỉ số (Single Source of Truth):** Nếu không có semantic layer, định nghĩa về chỉ số kinh doanh như Doanh thu (Revenue) có thể bị viết khác nhau ở từng Dashboard (ví dụ dashboard A tính bằng `SUM(total_amount)`, dashboard B tính bằng `SUM(quantity * unit_price)` dẫn đến sai lệch số liệu). Với Cube.js, các chỉ số được định nghĩa duy nhất một lần tại file schema `PosTransactions.js`, mọi BI tool hoặc API client đều tiêu thụ chung một định nghĩa.
    *   **Bảo vệ Database (Pre-aggregation Caching):** BI tools hoặc người dùng thường xuyên chạy các truy vấn lọc lặp đi lặp lại. Cube.js có công cụ pre-aggregation tự động nhận biết cấu trúc truy vấn, tạo ra các bảng tổng hợp tạm thời và lưu đệm kết quả. Khi Grafana gọi API, Cube.js trả kết quả ngay lập tức trong <50ms từ cache mà không cần phát vấn xuống ClickHouse hay Trino, tiết kiệm tài nguyên hệ thống tối đa.
    *   **Hỗ trợ giao thức đa dạng:** Cube.js cung cấp cả REST API, GraphQL API, và Cube SQL API (tương thích giao thức PostgreSQL/MySQL) giúp hệ thống cực kỳ dễ tích hợp với mọi nền tảng hạ tầng của doanh nghiệp.

---

## 4. Tình Huống Thực Tế & Scalability

### Q7: "Nếu lượng dữ liệu POS tăng lên 50,000 transactions/second, bạn sẽ scale hệ thống này như thế nào trên môi trường Cloud?"
*   **Câu trả lời:**
    *   **Tầng Thu Nhận (Ingestion):** Scale FastAPI Ingest API chạy trên Kubernetes (EKS/GKE) bằng cơ chế Horizontal Pod Autoscaler (HPA) dựa trên CPU/Memory usage.
    *   **Tầng Message Bus (Kafka):** Tăng số lượng Partition của topic `pos.transactions` lên tương ứng (ví dụ: 12-24 partitions) và phân tán các partition này trên nhiều Kafka Brokers. Cấu hình khóa phân vùng (Partition Key) theo `region` để đảm bảo dữ liệu của cùng một khu vực luôn được gửi đến cùng một partition theo thứ tự thời gian.
    *   **Tầng Real-Time OLAP (ClickHouse):** Triển khai ClickHouse Cluster dạng phân tán (Distributed) sử dụng công cụ `ReplicatedMergeTree` kết hợp với ClickHouse Operator trên Kubernetes. Dữ liệu sẽ được sharding theo `region` và replicate chéo giữa các nodes để đảm bảo tính sẵn sàng cao (High Availability).
    *   **Tầng Lưu Trữ Lịch Sử (Cold Storage):** Chuyển MinIO sang Amazon S3 hoặc Google Cloud Storage (GCS). Đây là các dịch vụ Object Storage có tính năng tự động scale băng thông và dung lượng lưu trữ vô hạn.
    *   **Tầng Federated Query (Trino):** Tăng số lượng Worker Nodes trong cụm Trino trên Kubernetes để tăng khả năng xử lý song song khi đọc các tệp tin Parquet lớn từ S3.
