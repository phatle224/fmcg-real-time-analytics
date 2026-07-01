**LE HONG PHAT**

**Data Engineer**

<hongphatle224@gmail.com> | +84 899 932 767 | Ho Chi Minh City | [LinkedIn](https://www.linkedin.com/in/phat-le-674640330/) | [Github](https://github.com/phatle224)

**SUMMARY**

Final-year Information Technology student with 9 months of production experience at AFFINA Insurance, specializing in data pipelines, CDC, ETL/ELT, and data warehousing. Built real-time and batch data systems using Kafka, dbt, Airflow, and PostgreSQL. Seeking a Data Engineer role to contribute to scalable data infrastructure and analytics platforms.

**TECHNICAL SKILLS**

- **Languages**: Python, SQL
- **Data Pipeline & Orchestration**: Airflow, Kafka, Spark, Debezium, dbt, ETL/ELT
- **Databases**: PostgreSQL, Oracle, MySQL, MongoDB, Redis, Cassandra
- **Infrastructure & Monitoring**: Docker, Prometheus, Grafana, Linux, Git
- **Cloud Exposure**: AWS, GCP
- **Concepts:** CDC, ETL/ELT, Data Modeling, Stream/Batch Processing, Data Warehouse/Lakehouse

**WORK EXPERIENCE**

**Data Engineer Intern** _Sep 2025 - May 2026_

**AFFINA Insurance**

- Data Platform Architecture (Phase 1): Built the end-to-end data platform capturing real-time MySQL CDC events via Debezium and scheduled Excel data, consolidating them into staging tables and normalized marts with under 2-second ingestion latency.
- Enterprise Data Consolidation (Phase 2): Evolved the platform to resolve complex online-offline data integration issues; implemented custom Contract Pre-Processing and a Policy Parser to standardize schema discrepancies (e.g., splitting multi-insured contracts, mapping default beneficiaries).
- Idempotency & Deduplication: Designed a real-time deduplication component utilizing Redis Contract Caching to track and validate record uniqueness, ensuring zero data loss and exact-once insertion into the Operational Data Store.
- Service Integration & Automation: Collaborated with AI Factory and downstream teams to configure RabbitMQ event routing for 5 consumer applications.

**PROJECTS**

**Hybrid Data Ingestion & Streaming Platform** _2026_

**github.com/phatle224/hybrid-data-ingestion-streaming-platform**

- Built a hybrid ELT platform combining CDC (Debezium + Kafka) and batch ingestion into PostgreSQL for near real-time analytics.
- Designed a 4-layer Medallion dbt pipeline (Staging → Intermediate → Warehouse → Mart) with 54 automated data quality tests across 5 dimension and 2 fact tables.
- Implemented idempotent deduplication using composite business keys and ROW_NUMBER() window functions to eliminate duplicate records from CDC streams.
- Built observability dashboards with Prometheus and Grafana to monitor Kafka consumer lag, PostgreSQL metrics, and pipeline health.

**Tech:** _Python • FastAPI • Apache Kafka • Debezium • dbt • PostgreSQL • Prometheus • Grafana • Docker_

**Agent SQL: Multi-Agent NL2SQL System** _2026_

**github.com/phatle224/Agent_SQL**

Collaborative project - responsible for AI pipeline, secure query execution, Kafka integration, and frontend dashboard.

- Designed and implemented a multi-agent NL2SQL workflow (Architect, Generator, Validator) with prompt-injection protection and schema hallucination validation.
- Developed a secure execution layer supporting PostgreSQL, MySQL, MongoDB, Redis, SQLite, and DuckDB with SELECT-only validation, connection pooling, and query timeout limits.
- Integrated Apache Kafka to decouple AI query generation from execution, and embedded a semantic modeling engine to generate dialect-specific SQL from business metrics.
- Built a Next.js dashboard for database connection management, query history, and SQL result visualization.

**Tech:** _Python • FastAPI • Next.js • React • Apache Kafka • PostgreSQL • Redis • MongoDB • DuckDB • Docker_

**EDUCATION**

**Bachelor of Information Technology** _2022 - Present (Expected 2027)_

**Saigon University (SGU), Ho Chi Minh City**

- Academic Excellence Scholarship - awarded for 3 consecutive semesters (2025 & 2026).
- Relevant coursework: Database Systems, Data Structures & Algorithms, Big Data Technologies, Software Engineering.

**CERTIFICATIONS**

**IBM Data Engineering Professional Certificate - Coursera / IBM** _Present_

**IBM Data Analyst Professional Certificate - Coursera / IBM** _Feb 2025_

**SQL (Advanced) Certificate - HackerRank** _Dec_ _2025_

**LANGUAGES**

- **Vietnamese** Native
- **English** B2 (IELTS in progress)