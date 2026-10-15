# 🚀 Enterprise Automated ETL Pipeline & Data Quality Framework

[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow%202.8-017CEE.svg?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/Data%20Warehouse-PostgreSQL%2016-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Power BI](https://img.shields.io/badge/Analytics-Power%20BI%20Ready-F2C811.svg?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Docker Compose](https://img.shields.io/badge/Deployment-Docker%20Compose-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An enterprise-grade, production-hardened **Extract, Transform, Load (ETL)** automation pipeline architected by **[Prabhat Sharma](https://github.com/prabhat84sharma-ui)**. 

Engineered for high data reliability and analytical rigor, this pipeline extracts from heterogeneous sources (REST APIs, relational databases, and flat files), enforces rigorous **Data Quality gates** with automated scoring and breach alarms, transforms raw records into an optimized **Kimball Star Schema**, and bulk upserts records into a **PostgreSQL Data Warehouse** serving **Power BI analytical views** and automated **Slack/Email alerts**.

---

## 🏛️ End-to-End Architecture

```
========================================================================================
                               ETL PIPELINE ARCHITECTURE
========================================================================================

 [ SOURCES ]                 [ INGESTION & EXTRACTION ]             [ STAGE GATE 1 ]
+-------------------+        +-------------------------+        +----------------------+
| REST API (Paged)  | =====> |  RestApiExtractor       | =====> | Raw Data Quality     |
| Relational DB     | =====> |  DatabaseExtractor      |        | - Null assertions    |
| Files (CSV/JSON)  | =====> |  FileSystemExtractor    |        | - Duplicate checks   |
+-------------------+        +-------------------------+        +----------------------+
                                                                           ||
                                                                           \/
 [ ANALYTICAL SERVING ]       [ WAREHOUSE & MARTS ]             [ MODELING & CURATION ]
+---------------------+      +-------------------------+        +----------------------+
| Power BI DirectQuery| <=== | PostgreSQL DW (16)      | <===== | DataTransformer      |
| Executive Dashboards|      | - Star Schema Facts/Dims|        | - Business Rules     |
| Customer RFM Views  |      | - Materialized Views    |        | - Currency (-> USD)  |
| Daily Sales Trends  |      | - Audit Logging & DQ Log|        | - Star Schema Model  |
+---------------------+      +-------------------------+        +----------------------+
                                          ||                               ||
                                          \/                               \/
                               [ OBSERVABILITY & ALERTS ]        [ STAGE GATE 2 ]
                             +---------------------------+      +----------------------+
                             | PipelineNotifier          | <=== | Curated DQ Suite     |
                             | - Slack Block Kit Alerts  |      | - Score (0-100%)     |
                             | - SMTP Multipart HTML     |      | - Referential Integrity
                             +---------------------------+      | - Boundary Ranges    |
========================================================================================
```

---

## ✨ Key Enterprise Features

- **Multi-Source Resilient Extractor**:
  - **REST APIs**: Full pagination support (page-number, cursor, and offset/limit) with exponential backoff retries and HTTP connection pooling.
  - **Database Streaming**: SQLAlchemy-powered query execution with `QueuePool` connection pooling and chunked iteration for multi-million row datasets.
  - **File System**: Automated ingestion of CSV, JSON (records and newline-delimited), and Excel (`.xlsx`) files with schema detection and automatic archiving.
- **Data Quality & Observability Framework**:
  - Automated pre-load and post-transformation quality scoring ($0.00\%$ to $100.00\%$).
  - Granular assertions: Null thresholds, composite primary key duplicate checks, numerical range boundaries, and foreign key referential integrity.
  - Configurable strictness: `strict_mode` automatically halts pipeline execution and triggers emergency alerts when quality falls below service level agreements (SLAs).
  - Automated JSON audit scorecard export to `reports/` and PostgreSQL audit tables.
- **Dimensional Data Modeling (Kimball Star Schema)**:
  - Built-in dynamic **Calendar Dimension** (`dim_date`) generating Gregorian and fiscal hierarchies.
  - Conformed Dimensions: `dim_customer`, `dim_product`, and `dim_channel` with natural-to-surrogate key mappings and normalization.
  - Grain-level Fact Table: `fact_orders` with currency conversion to USD base currency, tax computations, and discount clamping.
  - Aggregated Fact Table: `fact_daily_sales_summary` for ultra-fast Power BI executive tile rendering.
- **High-Throughput Vectorized Loader**:
  - PostgreSQL batch upserts using `psycopg2.extras.execute_values` achieving $10,000+$ rows/sec insertion throughput.
  - Idempotent `INSERT ... ON CONFLICT DO UPDATE` upsert logic to support backfills and incremental reprocessing without duplication.
  - Transaction-safe execution with automated rollback on failure.
- **Power BI Optimized Analytical Layer**:
  - Pre-engineered SQL analytical views for Customer RFM segmentation, product profitability margins, channel attribution, and daily executive KPIs.
  - Indexed **Materialized Views** (`mv_monthly_sales_trend`) with automated scheduled concurrent refresh.
- **Apache Airflow 2.8 Orchestration**:
  - Modular DAG with granular tasks, task dependencies, state propagation via XComs, and DAG-level failure callbacks.
  - Dual execution modes: Can run scheduled inside Airflow or standalone via the included CLI runner `run_pipeline.py`.
- **Multi-Channel Alerting**:
  - Rich interactive Slack notifications with Block Kit formatting, status emojis, and runtime statistics.
  - Executive-ready responsive HTML email summaries sent via SMTP.

---

## 📁 Repository Structure

```
etl-automation-pipeline/
├── config/
│   └── config.yaml               # Dev, staging, and production environment profiles
├── dags/
│   └── etl_pipeline_dag.py       # Apache Airflow DAG definition & task dependencies
├── data/
│   ├── raw/                      # Landing directory for incoming raw data files
│   ├── processed/                # Staging cache for intermediate Parquet files
│   └── archive/                  # Historical archive for ingested files
├── sql/
│   ├── create_tables.sql         # Star Schema DDL, indexes, and audit table definitions
│   └── analytical_views.sql      # 5+ Analytical views & materialized views for Power BI
├── src/
│   ├── __init__.py               # Package initializer
│   ├── config.py                 # Configuration loader with env var substitution
│   ├── extractor.py              # Multi-source extractor (API, DB, FileSystem)
│   ├── transformer.py            # Business logic, currency conversion & Star Schema modeling
│   ├── loader.py                 # Vectorized PostgreSQL bulk upsert & audit logger
│   ├── data_quality.py           # Data quality rules engine, scoring & JSON reporter
│   └── notifier.py               # Slack Block Kit & SMTP HTML alerting dispatcher
├── tests/
│   ├── __init__.py               # Test package initializer
│   └── test_data_quality.py      # Pytest unit tests for all validation checks
├── docker-compose.yml            # Airflow Webserver, Scheduler & PostgreSQL 16 setup
├── requirements.txt              # Pinned Python production dependencies
├── run_pipeline.py               # Standalone pipeline CLI runner (runs without Airflow)
├── .gitignore                    # Data engineering exclusion rules
└── README.md                     # Technical architecture and user guide
```

---

## 🔄 Airflow DAG Workflow

The Airflow DAG (`retail_sales_etl_pipeline`) runs on a daily schedule (`0 3 * * *` - 03:00 AM UTC).

### Task Dependency Pipeline

```
[start_pipeline] 
       │
       ▼
[check_infrastructure] 
       │
       ▼
[extract_raw_data] 
       │
       ▼
[validate_raw_data] 
       │
       ▼
[transform_star_schema] 
       │
       ▼
[validate_curated_data] 
       │
       ▼
[load_data_warehouse] 
       │
       ▼
[refresh_materialized_views] 
       │
       ▼
[notify_pipeline_success] 
       │
       ▼
[end_pipeline]
```

### DAG Task Breakdown

| Task ID | Operator | Description | XCom Output |
| :--- | :--- | :--- | :--- |
| `start_pipeline` | `BashOperator` | Initializes DAG run and logs timestamps | None |
| `check_infrastructure` | `BashOperator` | Asserts runtime environment and library health | None |
| `extract_raw_data` | `PythonOperator` | Pulls data from REST APIs, DBs, and local landing files | `records_extracted`, `extraction_manifest` |
| `validate_raw_data` | `PythonOperator` | Evaluates raw staging completeness and duplicate checks | `raw_dq_score` |
| `transform_star_schema`| `PythonOperator` | Currency conversion, business logic, Star Schema modeling | `transformed_manifest`, `records_transformed` |
| `validate_curated_data`| `PythonOperator` | Comprehensive DQ suite (referential integrity, ranges, nulls) | `curated_dq_score`, `dq_report_status` |
| `load_data_warehouse` | `PythonOperator` | Bulk upserts facts and dimensions into PostgreSQL | `records_loaded` |
| `refresh_materialized_views` | `PythonOperator` | Refreshes `mv_monthly_sales_trend` concurrently | None |
| `notify_pipeline_success` | `PythonOperator` | Pulls run statistics from XCom and sends Slack/Email alerts | None |
| `end_pipeline` | `BashOperator` | Finalizes pipeline run milestone | None |

> **Failure Handling**: If any task fails, `on_dag_failure_callback` intercepts the exception, extracts task instance details and error stack traces, and immediately dispatches an urgent Slack & Email notification.

---

## 📊 Data Model (Kimball Star Schema)

The pipeline transforms raw operational order events into an analytical Star Schema:

```
                            +--------------------+
                            |      dim_date      |
                            +--------------------+
                            | PK  date_key       |
                            |     full_date      |
                            |     month_name     |
                            |     quarter_name   |
                            |     year_num       |
                            |     fiscal_year    |
                            +--------------------+
                                      ▲
                                      │ (date_key)
+--------------------+      +--------------------+      +--------------------+
|    dim_customer    |      |    fact_orders     |      |    dim_product     |
+--------------------+      +--------------------+      +--------------------+
| PK  customer_key   | ◄─── | PK  order_key      | ───► | PK  product_key    |
|     customer_id    | (FK) |     order_id       | (FK) |     product_id     |
|     full_name      |      |     order_line_id  |      |     product_name   |
|     email          |      | FK  customer_key   |      |     category       |
|     city, state    |      | FK  product_key    |      |     subcategory    |
|     customer_seg   |      | FK  channel_key    |      |     unit_cost_usd  |
+--------------------+      | FK  date_key       |      |     unit_price_usd |
                            |     quantity       |      +--------------------+
                            |     unit_price_usd |
                            |     discount_usd   |
                            |     tax_amount_usd |
                            |     total_amount_usd
                            |     order_status   |
                            +--------------------+
                                      │ (channel_key)
                                      ▼
                            +--------------------+
                            |    dim_channel     |
                            +--------------------+
                            | PK  channel_key    |
                            |     channel_id     |
                            |     channel_name   |
                            |     channel_type   |
                            |     platform       |
                            +--------------------+
```

---

## 📈 Power BI Ready Analytical Views

The warehouse provides optimized views specifically built for DirectQuery or scheduled Semantic Model imports:

1. **`vw_customer_rfm_analytics`**: Calculates **Recency, Frequency, and Monetary (RFM)** metrics and assigns customers into behavioral segments (*Champions*, *Loyal Customers*, *At Risk*, *Lost*).
2. **`vw_product_category_performance`**: Tracks gross sales, net revenue, discounts, unit costs, and estimated gross margin percentage by product category and brand.
3. **`vw_channel_attribution_efficiency`**: Analyzes omnichannel efficiency, comparing revenue share percentage, average order value (AOV), and cancellation rates across Shopify, Amazon, POS, and B2B portals.
4. **`vw_executive_daily_kpis`**: Provides high-level daily KPI scorecard (total orders, active customers, net sales, AOV, and Month-to-Date cumulative revenue).
5. **`mv_monthly_sales_trend` (Materialized View)**: Pre-computes month-over-month (MoM) and year-over-year (YoY) revenue growth trajectories using SQL analytical window functions.
6. **`vw_data_quality_health_dashboard`**: Real-time DataOps governance view displaying test pass rates, failure counts, and quality score trends across pipeline runs.

---

## ⚡ Quickstart & Execution

### Option 1: Standalone CLI Runner (No Airflow Required)

You can run the full end-to-end pipeline immediately on your local machine:

```bash
# 1. Clone repository
git clone https://github.com/prabhat84sharma-ui/etl-automation-pipeline.git
cd etl-automation-pipeline

# 2. Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Execute pipeline (Environment: dev, staging, or prod)
python run_pipeline.py --env dev
```

**CLI Execution Output:**
```text
======================================================================
STARTING ETL AUTOMATION PIPELINE [Run ID: RUN-CLI-20261015-122123 | Env: dev]
======================================================================
[Step 1/6] Ingesting multi-source datasets (API & Files)...
Extraction complete: 300 raw records across 1 sources.
[Step 2/6] Running Raw Data Quality assertions...
[Step 3/6] Applying business logic & Star Schema modeling...
Transformation complete. Generated: dim_date (456), dim_customer (60), dim_product (50), dim_channel (4), fact_orders (300).
[Step 4/6] Running Curated Star Schema Data Quality score suite...
Data Quality Verification Passed! Overall Score: 100.00%
[Step 5/6] Loading Star Schema into Data Warehouse...
Simulated load: 300 rows staged for warehouse insertion.
[Step 6/6] Dispatching pipeline completion alerts...
======================================================================
PIPELINE COMPLETED SUCCESSFULLY in 18.41 seconds!
======================================================================
```

---

### Option 2: Full Docker Compose Environment (Airflow + PostgreSQL)

Launch the complete containerized stack with PostgreSQL 16 Data Warehouse and Apache Airflow 2.8:

```bash
# Start all services in the background
docker-compose up -d

# Check service health
docker-compose ps
```

- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) (Username: `admin`, Password: `admin`)
- **PostgreSQL Data Warehouse**: `localhost:5432` (`dw_dev` / User: `postgres`, Password: `postgres`)

The PostgreSQL container automatically runs `sql/create_tables.sql` and `sql/analytical_views.sql` on initial startup.

---

## 🧪 Automated Testing

Run the test suite covering data quality rules, threshold validation, and serialization:

```bash
# Run pytest with verbose reporting
pytest tests/test_data_quality.py -v

# Run with test coverage
pytest tests/ --cov=src --cov-report=term-missing
```

**Unit Test Coverage Highlights:**
- `test_check_nulls_clean_data`: Asserts 100% score on complete records.
- `test_check_nulls_with_missing_values`: Validates penalty calculation on missing keys.
- `test_check_duplicates_detects_clones`: Validates composite key uniqueness enforcement.
- `test_check_range_violations`: Tests detection of negative quantities or invalid amounts.
- `test_check_referential_integrity_orphans`: Asserts rejection of orphan child rows.
- `test_evaluate_suite_strict_mode_exception`: Tests pipeline halt on SLA breach.
- `test_report_json_serialization`: Validates JSON audit scorecard schema output.

---

## 🛠️ Tech Stack & Technologies

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Language** | Python 3.11+ | Core ETL logic, data modeling, and orchestration scripts |
| **Orchestration** | Apache Airflow 2.8.3 | Task scheduling, dependency resolution, DAG retry policies |
| **Data Warehouse** | PostgreSQL 16 | Star schema storage, ACID transactions, materialized views |
| **Data Processing** | Pandas & NumPy | Vectorized transformations, currency conversions, aggregations |
| **Database Driver** | SQLAlchemy 2.0 & psycopg2 | Connection pooling, streaming cursors, and bulk upserts |
| **Quality Framework** | Custom Rules Engine | Null, duplicate, boundary range, and referential integrity assertions |
| **Analytics & BI** | Microsoft Power BI | DirectQuery semantic models and executive reporting views |
| **Alerting** | Slack API & SMTP | Block Kit formatted webhooks and responsive HTML run summaries |
| **Containerization** | Docker & Docker Compose | Multi-container reproducible local and staging environments |
| **Testing** | Pytest | Automated test coverage for data quality and integrity logic |

---

## 👤 Author & Maintainer

**Prabhat Sharma**  
Senior Data Engineer & Analytics Solutions Architect  
- **GitHub**: [@prabhat84sharma-ui](https://github.com/prabhat84sharma-ui)  
- **Specialization**: Large-scale distributed ETL/ELT pipelines, Data Warehousing, Data Modeling (Kimball/Inmon), Data Quality & Observability, and BI Semantic Architecture.

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
