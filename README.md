# Party Master Data Hub

A complete end-to-end **Party Reference Data Master** system built to simulate enterprise-grade data engineering practices used in financial services firms.

This project demonstrates key skills required for Senior Data Engineer roles including cloud-native data pipelines, party data mastering, identity resolution, golden record creation, and event-driven architectures.

---

## Architecture Overview

```
Source Systems          Pipeline              Master Store         Serving
──────────────         ─────────────         ────────────         ───────
Internal CRM    ──►                                               
D&B Feed        ──►   Ingest & Clean  ──►   Golden Records  ──►  FastAPI
Markit Feed     ──►   (PySpark/Glue)        (PostgreSQL)         
Kafka Events    ──►                                               
```

## AWS Service Mapping (Local → Cloud)

| Local Tool | AWS Equivalent | Purpose |
|------------|---------------|---------|
| PostgreSQL (Docker) | RDS Aurora/Postgres | Golden record store |
| Apache Kafka (Docker) | MSK / Kinesis | Event streaming |
| LocalStack S3 | AWS S3 | Data lake (raw/curated/golden zones) |
| PySpark | AWS Glue | ETL pipelines |
| Python scripts | AWS Lambda | Event-driven functions |
| FastAPI | API Gateway + Lambda | Data serving layer |

---

## Project Structure

```
party-master-hub/
├── docker/                 # Docker Compose for all local services
├── config/                 # Environment config (not committed)
├── data/                   # Raw and processed data (not committed)
├── src/
│   ├── ingestion/          # Layer 1 - Data ingestion from source systems
│   ├── pipeline/           # Layer 2 - PySpark ETL jobs
│   ├── mastering/          # Layer 3 - Deduplication and golden records
│   ├── quality/            # Layer 4 - Great Expectations data quality
│   ├── api/                # Layer 5 - FastAPI data serving layer
│   ├── streaming/          # Layer 6 - Kafka producer/consumer
│   └── utils/              # Shared utilities
└── tests/                  # Unit and integration tests
```

---

## Local Setup

### Prerequisites
- Docker Desktop
- Python 3.11+
- Git

### Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/party-master-hub.git
cd party-master-hub

# 2. Start all services
cd docker && docker-compose up -d && cd ..

# 3. Set up Python environment
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt

# 4. Copy and configure environment
copy config\.env.example config\.env

# 5. Initialize database and S3
python src/utils/init_db.py
python src/utils/init_localstack.py

# 6. Health check
python src/utils/health_check.py
```

---

## Key Concepts Demonstrated

- **Party Reference Data** — individuals, organizations, and legal entities
- **Identity Resolution** — matching records across sources using Tax ID, LEI, and fuzzy name matching
- **Golden Record Creation** — survivorship rules to produce a single master record
- **Data Lineage** — full audit trail of how each golden record was built
- **Event-Driven Architecture** — Kafka-based real-time updates
- **Data Quality** — Great Expectations validation framework
- **Cloud-Native Design** — mirrors AWS production architecture locally

---

## Tech Stack

Python 3.12 · PySpark · FastAPI · PostgreSQL · Apache Kafka · LocalStack · Great Expectations · SQLAlchemy · Pandas · Docker
