# Olist E-Commerce End-to-End Data Pipeline

## 🎯 เป้าหมาย
สร้างโปรเจค Data Engineering แบบ End-to-End สำหรับ **Brazilian E-Commerce Public Dataset by Olist** โดยใช้สถาปัตยกรรมเดียวกับ [nyc-taxi-airflow-minio-duckdb-pipeline](https://github.com/kriangsak0066/nyc-taxi-airflow-minio-duckdb-pipeline) แต่ปรับปรุงและเพิ่ม tech stack ให้แข็งแกร่งขึ้น

---

## 📊 เข้าใจ Dataset ก่อน

Olist Dataset มี **9 ตาราง** ที่เชื่อมโยงกัน:

| ตาราง | คำอธิบาย |
|-------|----------|
| `olist_orders_dataset` | ตารางกลาง — สถานะ, timestamps, customer_id |
| `olist_order_items_dataset` | รายการสินค้าในแต่ละ order (1:N) |
| `olist_order_payments_dataset` | ข้อมูลการชำระเงิน |
| `olist_order_reviews_dataset` | รีวิวและคะแนนความพอใจ |
| `olist_products_dataset` | ข้อมูลสินค้า (หมวดหมู่, น้ำหนัก, ขนาด) |
| `olist_customers_dataset` | ข้อมูลลูกค้าและพิกัดที่ตั้ง |
| `olist_sellers_dataset` | ข้อมูลผู้ขาย |
| `olist_geolocation_dataset` | Zip code → lat/lng |
| `product_category_name_translation` | แปลชื่อหมวดหมู่ PT → EN |

---

## 🏗️ Architecture Overview

```text
Olist Raw CSV Files (Kaggle Download)
        ↓
Airflow DAG: Validate & Ingest Raw CSVs
        ↓
Upload to MinIO (Local Object Storage)
        ↓
MinIO Bucket: olist-raw
        ↓
Airflow DAG: Verify MinIO Objects
        ↓
DuckDB Local Warehouse (Bronze Layer)
        ↓
dbt Transformations (Silver → Gold)
        ↓
Staging → Intermediate → Mart Tables
        ↓
Great Expectations: Data Quality Checks
        ↓
Airflow DAG: Export Marts to CSV/Parquet
        ↓
Dashboard-ready Outputs (Power BI / Streamlit)
```

---

## 🛠️ Tech Stack ที่แนะนำ (พร้อมเหตุผล)

### เปรียบเทียบกับโปรเจค Reference

| Layer | Reference (NYC Taxi) | แนะนำ (Olist) | เหตุผล |
|-------|---------------------|---------------|--------|
| **Orchestration** | Apache Airflow | **Apache Airflow** | มาตรฐาน industry, DAG-based, มี UI monitoring |
| **Container** | Docker + WSL2 | **Docker Compose** | ครบ stack ใน single command |
| **Object Storage** | MinIO | **MinIO** | S3-compatible, simulate cloud locally |
| **Warehouse** | DuckDB (raw SQL) | **DuckDB** | เร็วมาก, in-process, columnar |
| **Transformation** | Raw SQL ใน DAGs | **dbt-core (dbt-duckdb)** ✨ | แยก transformation logic, testable, documented |
| **Data Quality** | ไม่มี | **Great Expectations / dbt tests** ✨ | เพิ่ม data reliability |
| **Data Format** | Parquet | **CSV → Parquet** | Olist เป็น CSV, แปลงเป็น Parquet เพื่อ performance |
| **Dashboard** | Power BI (CSV export) | **Power BI** | Export CSV/Parquet → Power BI Dashboard |
| **Version Control** | Git | **Git + Pre-commit hooks** | Code quality automation |
| **CI/CD** | ไม่มี | **GitHub Actions** ✨ | Automated testing & linting |

### Tech Stack แบบละเอียด

#### 1. 🐳 Infrastructure Layer
- **Docker Desktop + Docker Compose** — รัน Airflow, MinIO, PostgreSQL, Redis ใน containers
- **WSL 2** (Windows) — สำหรับ Linux compatibility

#### 2. 📦 Orchestration Layer
- **Apache Airflow 2.9+** — DAG orchestration
  - **CeleryExecutor** — สำหรับ parallel task execution
  - **PostgreSQL** — Airflow metadata database
  - **Redis** — Celery message broker

#### 3. 💾 Storage Layer
- **MinIO** — Local S3-compatible object storage
  - Bucket: `olist-raw` (raw CSV files)
  - Bucket: `olist-staging` (Parquet files)
  - Bucket: `olist-exports` (dashboard-ready outputs)

#### 4. 🦆 Warehouse Layer
- **DuckDB** — In-process analytical database
  - **Bronze** (raw): อ่านจาก MinIO โดยตรง
  - **Silver** (staging): Cleaned & typed
  - **Gold** (marts): Business-ready aggregations

#### 5. 🔄 Transformation Layer (dbt)
- **dbt-core + dbt-duckdb adapter**
  - `models/staging/` — Clean & type cast raw data
  - `models/intermediate/` — Business logic joins
  - `models/marts/` — Final aggregated tables
  - `tests/` — Data quality assertions

#### 6. ✅ Data Quality Layer
- **dbt tests** — schema tests (not_null, unique, relationships)
- **Great Expectations** (optional) — custom validation rules

#### 7. 📊 Visualization Layer
- **CSV/Parquet exports** → Power BI Desktop

---

## 📁 โครงสร้างโปรเจค (Project Structure)

```
olist-data-pipeline-ecommerce/
│
├── .env.example                    # Environment variables template
├── .gitignore
├── docker-compose.yml              # All services: Airflow, MinIO, PostgreSQL, Redis
├── Dockerfile                      # Custom Airflow image with dependencies
├── requirements.txt                # Python dependencies
├── README.md
├── Makefile                        # Quick commands (make up, make down, etc.)
│
├── config/
│   ├── airflow.env                 # Airflow environment config
│   └── minio.env                   # MinIO credentials
│
├── dags/                           # Airflow DAGs
│   ├── 01_validate_raw_files.py    # Check CSV files exist & valid
│   ├── 02_upload_to_minio.py       # Upload raw CSVs to MinIO
│   ├── 03_csv_to_parquet.py        # Convert CSV to Parquet in MinIO
│   ├── 04_build_duckdb_warehouse.py # Load data into DuckDB
│   ├── 05_run_dbt_transforms.py    # Execute dbt models
│   ├── 06_data_quality_checks.py   # Run Great Expectations / dbt tests
│   └── 07_export_marts.py          # Export final marts to CSV/Parquet
│
├── data/
│   ├── raw/                        # Olist CSV files (git-ignored)
│   ├── processed/                  # Intermediate outputs
│   ├── warehouse/                  # DuckDB database file
│   └── rejected/                   # Failed quality check records
│
├── dbt_olist/                      # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _stg_models.yml     # Schema & tests
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_sellers.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_payments.sql
│   │   │   ├── stg_reviews.sql
│   │   │   └── stg_geolocation.sql
│   │   ├── intermediate/
│   │   │   ├── int_orders_enriched.sql    # Orders + Items + Payments
│   │   │   ├── int_seller_performance.sql
│   │   │   └── int_customer_segmentation.sql
│   │   └── marts/
│   │       ├── mart_sales_overview.sql        # Revenue, AOV, trends
│   │       ├── mart_delivery_performance.sql  # On-time rate, avg delivery time
│   │       ├── mart_product_analytics.sql     # Top categories, products
│   │       ├── mart_customer_rfm.sql          # RFM segmentation
│   │       ├── mart_seller_ranking.sql        # Seller KPIs
│   │       └── mart_geo_analysis.sql          # Geographic distribution
│   ├── macros/                     # Reusable SQL macros
│   └── tests/                      # Custom data tests
│
├── sql/                            # Standalone SQL scripts
│   ├── duckdb/
│   │   ├── 01_create_raw_tables.sql
│   │   ├── 02_create_staging_views.sql
│   │   └── 03_data_quality_checks.sql
│   └── analysis/                   # Ad-hoc analysis queries
│
├── src/                            # Python utility modules
│   ├── __init__.py
│   ├── config.py                   # Centralized config
│   ├── minio_utils.py              # MinIO operations helper
│   ├── duckdb_utils.py             # DuckDB connection helper
│   ├── file_utils.py               # File validation utilities
│   ├── pipeline.py                 # Core pipeline logic
│   └── logging_utils.py
│
├── dashboards/
│   ├── olist_dashboard.pbix        # Power BI file
│   └── images/                     # Dashboard screenshots
│
├── exports/                        # Exported CSV/Parquet for dashboards
│
├── tests/                          # Unit tests
│   ├── test_file_utils.py
│   ├── test_pipeline.py
│   └── test_minio_utils.py
│
├── docs/                           # Documentation
│   ├── STEP_BY_STEP_GUIDE.md
│   ├── DATA_MODEL.md
│   ├── DATA_DICTIONARY.md
│   ├── DASHBOARD_DESIGN.md
│   └── PROJECT_ROADMAP.md
│
├── logs/                           # Airflow logs
├── plugins/                        # Airflow plugins
└── reports/                        # Data quality reports
```

---

## 🗺️ Pipeline Phases (แผนการทำงาน)

### Phase 1: Foundation Setup ⚙️
- [ ] สร้าง Docker Compose (Airflow + MinIO + PostgreSQL + Redis)
- [ ] สร้าง `.env`, config files
- [ ] สร้าง Dockerfile (Airflow + Python dependencies)
- [ ] ทดสอบ `docker compose up` ว่า services ทำงานได้
- [ ] สร้าง Makefile สำหรับ shortcut commands

### Phase 2: Data Ingestion 📥
- [ ] ดาวน์โหลด Olist dataset จาก Kaggle → `data/raw/`
- [ ] เขียน DAG: Validate raw CSV files (check file count, row count, schema)
- [ ] เขียน DAG: Upload CSV files to MinIO bucket `olist-raw`
- [ ] เขียน DAG: Convert CSV → Parquet ใน MinIO bucket `olist-staging`

### Phase 3: Data Warehouse 🦆
- [ ] สร้าง DuckDB database
- [ ] เขียน SQL: Create raw tables from Parquet in MinIO
- [ ] เขียน DAG: Load data into DuckDB Bronze layer

### Phase 4: Transformation (dbt) 🔄
- [ ] Init dbt project (`dbt_olist`)
- [ ] เขียน staging models (8 tables)
- [ ] เขียน intermediate models (enriched orders, customer segmentation)
- [ ] เขียน mart models (6 business-facing tables)
- [ ] เขียน schema tests & documentation
- [ ] เขียน DAG: Run `dbt run` + `dbt test`

### Phase 5: Data Quality ✅
- [ ] เพิ่ม dbt tests (not_null, unique, accepted_values, relationships)
- [ ] เขียน custom data quality checks
- [ ] เขียน DAG: Run quality checks + save reports

### Phase 6: Export & Dashboard 📊
- [ ] เขียน DAG: Export mart tables to CSV/Parquet
- [ ] สร้าง Power BI dashboard
- [ ] เอา screenshots มาใส่ `dashboards/images/`

### Phase 7: Documentation & Polish 📝
- [ ] เขียน README.md ให้สมบูรณ์
- [ ] เขียน docs (Step-by-step, Data Model, Data Dictionary)
- [ ] เพิ่ม architecture diagram
- [ ] Setup `.gitignore` ให้ครบ

---

## 🔑 Mart Tables ที่จะสร้าง (Business Value)

| Mart | คำอธิบาย | KPIs |
|------|----------|------|
| `mart_sales_overview` | ภาพรวมยอดขาย | Revenue, Order Count, AOV, Monthly Trend |
| `mart_delivery_performance` | ประสิทธิภาพการจัดส่ง | On-time Rate, Avg Delivery Days, Late Delivery % |
| `mart_product_analytics` | วิเคราะห์สินค้า | Top Categories, Revenue by Category, Avg Review Score |
| `mart_customer_rfm` | RFM Segmentation | Recency, Frequency, Monetary per Customer |
| `mart_seller_ranking` | จัดอันดับผู้ขาย | Order Count, Revenue, Avg Rating, Delivery Speed |
| `mart_geo_analysis` | วิเคราะห์ภูมิศาสตร์ | Orders by State/City, Avg Freight by Distance |

---

## User Review Required

> [!IMPORTANT]
> **Dataset Location**: คุณมี Olist CSV files อยู่ที่ไหนแล้วหรือยัง? ต้องดาวน์โหลดจาก Kaggle ก่อนมั้ย?

> [!IMPORTANT]
> **Docker Desktop**: ติดตั้ง Docker Desktop + WSL 2 บน Windows แล้วหรือยัง?

> [!IMPORTANT]
> **dbt**: อยากใช้ dbt-core สำหรับ transformation layer ไหม? หรือต้องการใช้ raw SQL ใน Airflow DAGs เหมือนโปรเจค reference?

---

## Open Questions

> [!IMPORTANT]
> 1. **Dashboard Tool**: อยากใช้ Power BI, Streamlit, หรือทั้งสองอย่าง?
> 2. **ต้องการ CI/CD (GitHub Actions)** สำหรับ automated testing หรือยัง? หรือทำ local ก่อน?
> 3. **Scope**: อยากทำทุก Phase ตั้งแต่ต้นจนจบเลย หรืออยากเริ่มจาก Phase 1-3 ก่อน แล้วค่อยเพิ่ม dbt + dashboard ทีหลัง?
> 4. **Great Expectations**: อยากเพิ่ม data quality framework แยกต่างหาก หรือใช้แค่ dbt tests ก็เพียงพอ?

---

## Verification Plan

### Automated Tests
```bash
# ทดสอบ Docker services
docker compose ps

# ทดสอบ Airflow DAGs
docker exec airflow-webserver airflow dags list

# ทดสอบ MinIO connectivity
docker exec airflow-worker python -c "from minio import Minio; print('MinIO OK')"

# ทดสอบ dbt
cd dbt_olist && dbt debug && dbt run && dbt test

# ทดสอบ unit tests
pytest tests/ -v
```

### Manual Verification
- เปิด Airflow UI (http://localhost:8080) → ดู DAGs ทั้งหมด
- เปิด MinIO Console (http://localhost:9001) → ดู buckets
- รัน DAGs ทีละ phase → ดู logs ว่าไม่มี errors
- เปิด Power BI → connect CSV/Parquet exports → ดูว่า charts แสดงข้อมูลถูกต้อง
