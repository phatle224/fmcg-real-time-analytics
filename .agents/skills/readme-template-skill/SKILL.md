---
name: readme-template-skill
description: Skill for generating or formatting enterprise-grade, highly-visual README.md files for Data Engineering, Backend, and Web applications. Enforces capsule-render headers/footers, shields.io badges, Mermaid architecture diagrams, performance benchmark tables, devicon grids, directory trees, quick start steps, and troubleshooting sections.
---

# tasteskill: Premium README.md Generation Skill

This skill defines the standards, rules, and structures required to create or format a README.md for a project, matching the style of the enterprise data engineering platform.

---

## 1. STRUCTURE & SECTIONS (Cấu trúc bắt buộc)

Mỗi file README.md được tạo hoặc định dạng bằng skill này phải tuân thủ nghiêm ngặt cấu trúc gồm 14 phần sau:

1.  **Header Banner:** Sử dụng SVG `capsule-render` dạng waving gradient có animation twinkling.
2.  **Language Selector & Tagline:** Đường dẫn chuyển đổi ngôn ngữ (English | Vietnamese) và một thẻ tiêu đề phụ trung tâm.
3.  **Status Badges:** Bộ nhãn dán công nghệ (Shields.io) dạng `for-the-badge`.
4.  **Table of Contents:** Mục lục đánh số liên kết neo.
5.  **Project Overview:** Giới thiệu mục tiêu cốt lõi của dự án và ảnh chụp giao diện chính (nếu có).
6.  **System Architecture & Data Flow:** Sơ đồ Mermaid (Flowchart/Sequence) mô tả luồng dữ liệu của hệ thống.
7.  **Core Features:** Mô tả các tính năng cốt lõi, cơ chế thuật toán, và design patterns được áp dụng.
8.  **System Performance & Benchmarks:** Bảng số liệu benchmark đo lường hiệu năng thực tế.
9.  **Tech Stack:** Danh sách icon công nghệ chất lượng cao (Devicon / Simple Icons).
10. **Directory Structure:** Cây thư mục sạch sẽ kèm giải thích nhiệm vụ của từng thư mục.
11. **Quick Start Guide:** Hướng dẫn chạy thử từng bước chi tiết (cung cấp cả lệnh Bash và PowerShell).
12. **Monitoring & Observability:** Bảng quản trị metric, ports, và đường dẫn.
13. **Troubleshooting:** Bảng lỗi thường gặp (Error - Cause - Fix).
14. **Footer Banner:** Banner đóng chân trang đồng nhất.

---

## 2. DETAILED STYLING RULES (Quy tắc trình bày chi tiết)

### A. Header & Footer Banners (`capsule-render`)
*   **Header Template:**
    ```html
    <div>
      <img style="width: 100%" src="https://capsule-render.vercel.app/api?type=waving&height=120&section=header&reversal=true&text=YOUR%20PROJECT%20TITLE&fontSize=30&fontColor=ffffff&fontAlign=50&fontAlignY=45&rotate=0&stroke=-&animation=twinkling&desc=Sub%20Description%20Here&descSize=15&descAlign=50&descAlignY=65&textBg=false&color=gradient" />
    </div>
    ```
*   **Footer Template:**
    ```html
    <div>
      <img style="width: 100%" src="https://capsule-render.vercel.app/api?type=waving&height=120&section=footer&reversal=true&text=Build%20it%20clean%20%E2%80%A2%20Ship%20it%20reliable&fontSize=22&fontColor=ffffff&fontAlign=50&fontAlignY=50&rotate=0&stroke=-&animation=twinkling&textBg=false&color=gradient" />
    </div>
    ```

### B. Shields.io Badges
*   Phải căn giữa (`<div align="center">`) và sử dụng kiểu dáng `style=for-the-badge`.
*   Ví dụ cấu trúc badge:
    ```html
    <img src="https://img.shields.io/badge/Backend-FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="backend badge" />
    ```

### C. Mermaid Flowcharts
*   Sơ đồ Mermaid phải trực quan, chia subgraph rõ ràng theo các tầng nghiệp vụ (ví dụ: Ingestion, Staging, Transform, Reporting, Observability).
*   Sử dụng các hình dạng node khác nhau đại diện cho loại tài nguyên (`["Process"]`, `[("Database")]`, `[["Files"]]`, `{{\"Queue\"}}`).

### D. Tech Stack Grid (Devicons & Simple Icons)
*   Hiển thị icon logo kích thước lớn (`height="40"`) căn lề trái.
*   Cú pháp sử dụng:
    *   **Devicon:** `https://cdn.jsdelivr.net/gh/devicons/devicon/icons/{tech}/{tech}-original.svg`
    *   **Simple Icons:** `https://cdn.simpleicons.org/{slug}/{color-hex}`
    *   Dùng `<img width="8" />` để tạo khoảng cách ngang giữa các icon.

### E. Quick Start Guides
*   **Không viết chung chung.** Cung cấp lệnh chạy cụ thể.
*   Khi có sự khác biệt hệ điều hành, viết rõ lệnh cho cả **Bash** (macOS/Linux) và **PowerShell** (Windows).
    ```powershell
    # Windows (PowerShell)
    Copy-Item .env.example .env
    ```
    ```bash
    # macOS/Linux (Bash)
    cp .env.example .env
    ```

---

## 3. ANTI-SLOP & QUALITY CONTROL (Cấm tuyệt đối các lỗi sau)

*   **BẮT BUỘC KHÔNG DÙNG EM-DASH (`—` hoặc `–`):** Không dùng gạch ngang dài làm phần tử trang trí hay phân cách trong văn bản. Thay thế bằng dấu gạch ngang thường (`-`), dấu hai chấm (`:`), hoặc xuống dòng.
*   **KHÔNG sử dụng liên kết Unsplash mặc định:** Tránh dùng ảnh generic từ Unsplash. Nếu cần placeholder, hãy dùng Picsum với seed mô tả rõ ràng: `https://picsum.photos/seed/{mo-ta-ngu-canh}/{w}/{h}` hoặc dùng công cụ tạo ảnh thực tế.
*   **KHÔNG có code/lệnh mẫu chưa hoàn thiện:** Tất cả script, cấu hình docker-compose, endpoints đều phải thực tế, copy-paste được ngay và không chứa ghi chú `// TODO: tự điền`.
*   **Quy chuẩn tên công nghệ:** Viết đúng định dạng chữ hoa/thường: `FastAPI` (không viết Fastapi), `PostgreSQL` (không viết Postgresql), `dbt` (không viết DBT), `Kafka` (không viết kafka), `Docker` (không viết docker), `React` (không viết react).
