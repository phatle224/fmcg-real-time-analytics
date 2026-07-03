<div>
  <img style="width: 100%" src="https://capsule-render.vercel.app/api?type=waving&height=120&section=header&reversal=true&text=Hybrid%20ETL%20Platform&fontSize=30&fontColor=ffffff&fontAlign=50&fontAlignY=45&rotate=0&stroke=-&animation=twinkling&desc=Real-time%20CDC%20%E2%80%A2%20Offline%20Excel%20Ingestion&descSize=15&descAlign=50&descAlignY=65&textBg=false&color=gradient" />
</div>

<div align="center">
  <strong>English</strong> | <a href="README_VI.md">Vietnamese</a>
</div>

<h3 align="center">Enterprise Data Engineering Platform combining real-time CDC and offline Excel batch ingestion</h3>

<div align="center">
  <img src="https://img.shields.io/badge/Frontend-React%2018-61dafb?style=for-the-badge&logo=react&logoColor=black" alt="frontend badge" />
  <img src="https://img.shields.io/badge/Backend-FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="backend badge" />
  <img src="https://img.shields.io/badge/Database-PostgreSQL%2016-336791?style=for-the-badge&logo=postgresql&logoColor=white" alt="database badge" />
  <img src="https://img.shields.io/badge/Streaming-Apache%20Kafka-black?style=for-the-badge&logo=apachekafka&logoColor=white" alt="kafka badge" />
  <img src="https://img.shields.io/badge/CDC-Debezium-red?style=for-the-badge&logo=debezium&logoColor=white" alt="debezium badge" />
  <img src="https://img.shields.io/badge/Transform-dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt badge" />
</div>

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [System Architecture & Data Flow](#system-architecture--data-flow)
3. [Core Features](#core-features)
4. [System Performance & Benchmarks](#system-performance--benchmarks)
5. [Tech Stack](#tech-stack)
6. [Directory Structure](#directory-structure)
7. [Quick Start Guide](#quick-start-guide)
8. [Monitoring & Observability](#monitoring--observability)
9. [Troubleshooting](#troubleshooting)

---

## Project Overview

This project implements a **Hybrid Data Ingestion & Streaming ETL Platform** designed for managing insurance contracts. The system seamlessly integrates two fundamentally different data channels:
1. **Online Real-time CDC (Change Data Capture)**: Automatically captures data change events (INSERT, UPDATE, DELETE) directly from the production database of the sales system.
2. **Offline Batch Ingestion Portal**: Allows partners or administrators to manually upload raw contract reports via Excel files.

The core objective is to automate data collection, perform cross-deduplication, standardize, and build a centralized analytical data warehouse (**Star Schema**), providing the enterprise with a comprehensive and accurate view of its business operations.

### Portal Management Interface (Excel Upload UI)
![Portal UI Landing Page](docs/images/landing_page.png)

---

## System Architecture & Data Flow

The architecture is fully containerized using Docker, ensuring a smooth data flow from source systems to the reporting layer.

### Project Workflow
![Project Workflow](docs/images/project_workflow.png)

### Ingestion & Transformation Pipeline
```mermaid
flowchart TB
    subgraph "Online CDC Channel"
        SRC_DB[("Production DB<br/>(insustream_sale)")]
        DBZ_SRC["Debezium Source<br/>(Binlog Reader)"]
        KF_SRC{{"Kafka Topics<br/>(source.public.*)"}}
        CDC_CONS["CDC Consumer<br/>(Source to Staging)"]
    end

    subgraph "Offline Ingestion Channel"
        EXCEL[["Excel Files<br/>(Health, Travel, Vehicle...)"]]
        PORTAL_FE["Portal UI (React)<br/>Port: 3010"]
        PORTAL_BE["Portal API (FastAPI)<br/>Port: 3011"]
    end

    subgraph "Staging Layer"
        STG_DB[("Staging DB<br/>(staging schema)")]
    end

    subgraph "dbt ELT Transformation"
        SCHEDULER["dbt Scheduler Daemon /<br/>Airflow Operator"]
        DBT_ANALYTICS["dbt Analytics Project<br/>(Models, Tests & Marts)"]
    end

    subgraph "Reporting Layer"
        RPT_DB[("Reporting DB<br/>(reporting schema)")]
        WIDE_TAB["Dimensions & Facts<br/>(dim_*, fct_*)"]
        MARTS["Data Marts<br/>(dm_*)"]
    end

    subgraph "Observability Stack"
        PROM["Prometheus Server<br/>Port: 9090"]
        GRAF["Grafana Dashboards<br/>Port: 3030"]
        K_EXP["Kafka Exporter<br/>Port: 9308"]
        P_EXP["PostgreSQL Exporter<br/>Port: 9187"]
    end

    %% Online Channel
    SRC_DB -->|PostgreSQL Binlog| DBZ_SRC
    DBZ_SRC --> KF_SRC
    KF_SRC --> CDC_CONS
    CDC_CONS -->|Transform & UPSERT| STG_DB

    %% Offline Channel
    EXCEL -->|Upload| PORTAL_FE
    PORTAL_FE -->|REST API| PORTAL_BE
    PORTAL_BE <-->|Batch Check 7 Keys| STG_DB
    PORTAL_BE -->|Write Offline Data| STG_DB

    %% dbt ELT
    STG_DB --> DBT_ANALYTICS
    SCHEDULER -->|Run models & tests| DBT_ANALYTICS
    DBT_ANALYTICS --> WIDE_TAB
    WIDE_TAB --> MARTS
    MARTS --> RPT_DB

    %% Observability Connections
    KF_SRC -.->|Track offsets| K_EXP
    K_EXP -->|Metrics| PROM
    STG_DB -.->|Track connections & size| P_EXP
    RPT_DB -.->|Track connections & size| P_EXP
    P_EXP -->|Metrics| PROM
    PROM -->|Query metrics| GRAF
```

---

## Core Features

### 1. Excel Processing using Design Patterns
The Portal Backend is built with FastAPI applying strict Object-Oriented Design patterns:
*   **Factory Pattern (`ProcessorFactory`)**: Identifies the insurance type from the uploaded file to initialize the specific processor.
*   **Strategy Pattern (`IInsuranceProcessor`)**: Standardizes data structure and types independently for each business domain (Motorcycle, Vehicle, Health, Travel...).
*   **Template Method Pattern**: Fixes the 4-step processing workflow: `parse_excel()` $\rightarrow$ `pre_process()` $\rightarrow$ `transform()` $\rightarrow$ `post_process()`.

### 2. Cross-Deduplication Mechanism (SQL & dbt)
To prevent manually uploaded data (Offline Excel) from overwriting official data (Online CDC) based on the **Online Wins** principle:
*   **At API Level (Staging)**: The Portal Backend queries the Staging database directly to check for duplicates via batch queries. If the 7 core business keys match, the record is discarded early.
*   **At dbt Level (ELT)**: The `int_contracts_deduped.sql` model uses the `ROW_NUMBER() OVER (PARTITION BY 7_business_keys ORDER BY online_first)` window function as a final deduplication filter, prioritizing online data streams.

The 7 core business keys:
```
{contractId} + {peopleName} + {majorName} + {companyProviderName} + {startDate} + {endDate} + {feeInsurance}
```

### 3. Visualized Excel Upload Status
The system visually displays different scenarios of data processing results on the Portal UI:

````carousel
![Successful Upload](docs/images/successful_upload.png)
<!-- slide -->
![Duplicated Upload](docs/images/duplicated_upload.png)
<!-- slide -->
![Failed Upload (Format Error)](docs/images/failed_upload.png)
````

---

## System Performance & Benchmarks

Performance metrics measured on a local Docker environment (8 vCPU, 16 GB RAM):

| Metric | Value | Description |
|--------|-------|-------------|
| **Online CDC Throughput** | ~500 events/sec (peak) | Debezium captures WAL events, Kafka buffers, Consumer writes to staging |
| **Offline Batch Throughput** | ~50,000 records/batch | Excel upload processed via FastAPI + Pandas in a single API call |
| **End-to-End CDC Latency** | < 1.5 seconds | Time from production DB write to staging table materialization |
| **Deduplication Rate** | ~18% of offline uploads | Records matched and rejected by 7-business-key cross-channel dedup |
| **dbt Incremental Run** | ~8 seconds | Incremental models process only new/changed data since last run |
| **dbt Full-Refresh Run** | ~45 seconds | Complete rebuild of all warehouse and mart tables from scratch |
| **dbt Test Coverage** | 54 tests across 3 layers | Staging (source + model), Warehouse (dimensions + facts), Mart |
| **Kafka Consumer Lag** | < 50 messages (steady state) | Measured via Kafka Exporter + Grafana dashboard |

> **Note**: Metrics are based on a dataset of ~120,000 contract records and ~8,000 claims. Production environments with horizontal scaling (multiple Kafka partitions + consumer instances) can achieve significantly higher throughput.

---

## Tech Stack

### Frontend
<div align="left">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/react/react-original.svg" height="40" alt="react" />
  <img width="8" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/typescript/typescript-original.svg" height="40" alt="typescript" />
  <img width="8" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/bootstrap/bootstrap-original.svg" height="40" alt="bootstrap" />
</div>

*   **React 18 & TypeScript**: Component-driven UI development, robust upload state management.
*   **Tailwind CSS & Vanilla CSS**: Modern, responsive, and intuitive interface.

### Analytics & Backend
<div align="left">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" height="40" alt="python" />
  <img width="8" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/fastapi/fastapi-original.svg" height="40" alt="fastapi" />
  <img width="8" />
  <img src="https://cdn.simpleicons.org/sqlalchemy/d71f00" height="40" alt="sqlalchemy" />
  <img width="8" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" height="40" alt="postgresql" />
  <img width="8" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apachekafka/apachekafka-original.svg" height="40" alt="kafka" />
  <img width="8" />
  <img src="docs/images/debezium.png" height="40" alt="debezium" />
  <img width="8" />
  <img src="docs/images/dbt.png" height="40" alt="dbt" />
  <img width="8" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg" height="40" alt="docker" />
</div>

*   **FastAPI & SQLAlchemy ORM**: Handles incoming Excel files via asynchronous data streams.
*   **Kafka & Debezium**: Captures and tracks data changes directly from the source database.
*   **dbt Core**: Executes incremental ELT models to normalize data into a Star Schema.
*   **PostgreSQL 16**: Serves as the Production DB, Staging DB, and Reporting Data Warehouse.

---

## Directory Structure

```
hybrid-data-ingestion-platform/
├── configs/                         # Debezium connectors registration configs
├── database/                        # SQL scripts for DB initialization (Staging, Reporting)
├── docs/                            # System specifications and guides
│   ├── images/                      # UI and workflow images
│   ├── AIRFLOW_MIGRATION_GUIDE.md   # Production Airflow DAG migration blueprint
│   ├── PROJECT_ARCHITECTURE.md      # Detailed architecture and data flow
│   └── DEPLOYMENT_GUIDE.md          # Step-by-step deployment instructions
├── monitoring/                      # Observability stack configuration
│   ├── prometheus/prometheus.yml    # Prometheus scrape targets
│   └── grafana/                     # Grafana provisioning and dashboards
│       ├── provisioning/            # Auto-configured datasources and dashboard providers
│       └── dashboards/              # Pre-built JSON dashboard definitions
├── services/                        # Independent system services
│   ├── cdc_consumer/                # Syncs DB Source -> DB Staging via Kafka
│   ├── dbt_analytics/               # dbt Project (Transformations, DWH, Data Marts)
│   ├── shared/                      # Shared Python libraries (logger, db connections)
│   ├── portal_backend/              # FastAPI Backend receiving offline Excel files
│   └── portal_frontend/             # React + TypeScript Frontend
├── docker-compose.kafka.yml         # Manages Zookeeper, Kafka, and Kafka-UI
├── docker-compose.debezium.yml      # Manages Debezium Connect and Debezium-UI
├── docker-compose.consumer.yml      # Manages CDC Consumer
├── docker-compose.scheduler.yml     # Manages dbt Scheduler Daemon
├── docker-compose.portal.yml        # Manages Portal Frontend & Backend
├── docker-compose.monitoring.yml    # Manages Prometheus, Grafana, and Exporters
├── .env.example                     # Environment variables template
└── README.md                        # Project overview (this file)
```

---

## Quick Start Guide

### Prerequisites
*   **Docker** and **Docker Compose** installed.
*   A running PostgreSQL database system (or via Docker).

### Step 1: Copy Environment File
1. Copy the environment configuration template:
   ```powershell
   # Windows (PowerShell)
   Copy-Item .env.example .env
   
   # macOS/Linux (Bash)
   cp .env.example .env
   ```
2. Update the database and Kafka connection parameters to fit your environment.

### Step 2: Create Shared Docker Network
Initialize an internal network shared across the project stack:
```bash
docker network create cdc-network
```

### Step 3: Launch Infrastructure
```bash
# 1. Start databases (Source & Target)
docker compose -f docker-compose.db.yml up -d

# 2. Start Kafka Cluster & UI
docker compose -f docker-compose.kafka.yml up -d

# 3. Start Debezium Connector
docker compose -f docker-compose.debezium.yml up -d
```
*Wait about 15-20 seconds for the services to fully initialize.*

### Step 4: Register Debezium Connectors
Push the JSON configuration files to register table change tracking to Debezium:
```powershell
# Windows (PowerShell)
Invoke-RestMethod -Uri "http://localhost:8083/connectors" `
  -Method Post `
  -ContentType "application/json" `
  -Body (Get-Content configs\register-source-connector.json -Raw)
```

### Step 5: Start Services & Portal
```bash
# 1. Start CDC Consumer (Kafka -> Staging DB)
docker compose -f docker-compose.consumer.yml up -d --build

# 2. Start dbt Scheduler (Runs dbt transform every 5 minutes)
docker compose -f docker-compose.scheduler.yml up -d --build

# 3. Start Portal FE & BE
docker compose -f docker-compose.portal.yml up -d --build
```

---

## Monitoring & Observability

The platform includes a full observability stack powered by **Prometheus** and **Grafana** for real-time monitoring of Kafka consumer lag, PostgreSQL health, and ingestion throughput.

### CDC Platform Overview Dashboard
![CDC Platform Overview Dashboard](docs/images/monitoring_dashboard.png)

### Start the Monitoring Stack
```bash
docker compose -f docker-compose.monitoring.yml up -d
```

### Dashboards & Interfaces
| Service | URL | Purpose |
|---------|-----|--------|
| **Grafana** | [http://localhost:3030](http://localhost:3030) | CDC Platform Overview dashboard (Kafka lag, DB stats) |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | Raw metrics and query explorer |
| **Kafka-UI** | [http://localhost:8080](http://localhost:8080) | Topic inspection, consumer group monitoring |
| **Debezium-UI** | [http://localhost:8084](http://localhost:8084) | Connector operational status |
| **Portal UI** | [http://localhost:3010](http://localhost:3010) | Excel file upload interface |
| **Portal API Docs** | [http://localhost:3011/docs](http://localhost:3011/docs) | Swagger/OpenAPI endpoint explorer |

> Default Grafana credentials: `admin` / `admin`

### System Logs
```bash
docker compose -f docker-compose.<service>.yml logs -f
```

---

## Troubleshooting

*   **Error: `Debezium Connector fails to run (FAILED)`**
    *   *Cause:* The source database (`insure_production`) is not configured with `wal_level` set to `logical`.
    *   *Fix:* Execute the SQL command `ALTER SYSTEM SET wal_level = 'logical';` on the source database and restart it.
*   **Error: `Consumer not receiving messages from Kafka`**
    *   *Cause:* Network mismatch in `cdc-network` or incorrect Kafka bootstrap server mapping between the container environment and localhost.
    *   *Fix:* Ensure the `KAFKA_BOOTSTRAP_SERVERS` variable in `.env` is set to `kafka:9093` for containers and `localhost:9092` for processes running directly on the host machine.
*   **Local code changes are not reflected in the container?**
    *   *Fix:* Re-run docker compose with the build flag to recompile the image: `docker compose -f docker-compose.<name>.yml up -d --build`.

---

<div>
  <img style="width: 100%" src="https://capsule-render.vercel.app/api?type=waving&height=120&section=footer&reversal=true&text=Build%20it%20clean%20%E2%80%A2%20Ship%20it%20reliable&fontSize=22&fontColor=ffffff&fontAlign=50&fontAlignY=50&rotate=0&stroke=-&animation=twinkling&textBg=false&color=gradient" />
</div>
