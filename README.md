# Party Master Data Hub

A production-grade **party reference data mastering system** built to demonstrate enterprise data engineering skills for financial services. Simulates the exact architecture used at firms like LPL Financial for client onboarding, KYC/AML compliance, and regulatory reporting.

---

## What This Project Does

Financial institutions manage millions of records about clients, counterparties, and legal entities across dozens of source systems. The same person or company often appears multiple times under different names, addresses, or identifiers. This system:

- **Ingests** party data from multiple source systems (CRM, D&B, S&P Markit)
- **Resolves** duplicate records across sources using Tax ID matching and fuzzy name/address similarity
- **Masters** a single golden record per real-world entity using survivorship rules
- **Validates** data quality at every stage of the pipeline
- **Streams** real-time updates via Kafka event processing
- **Serves** golden records through a REST API
- **Visualizes** the entire pipeline through a live control center dashboard

---

## Architecture

```
+-----------------------------------------------------------------+
|                    DATA SOURCES                                  |
|   Internal CRM        D&B Feed          S&P Markit Feed         |
|   (Individuals)    (Organizations)    (Legal Entities)          |
+----------+----------------+-----------------+-------------------+
           |                |                 |
           v                v                 v
+-----------------------------------------------------------------+
|                    INGESTION LAYER                               |
|         Normalize -> Load PostgreSQL -> Upload S3 Raw Zone       |
|              (Simulates AWS Glue ETL)                           |
+------------------------------+----------------------------------+
                               |
                               v
+-----------------------------------------------------------------+
|                    DATA MASTERING                                |
|   Identity Resolution (Tax ID exact + fuzzy name/address)       |
|   Survivorship Rules (source priority per field)                |
|   Golden Record Creation -> party_clusters -> party_lineage     |
+------------------------------+----------------------------------+
                               |
                    +----------+-----------+
                    v                      v
+-------------------------+  +------------------------------+
|    DATA QUALITY          |  |    S3 EXPORT                  |
|  3 validation suites     |  |  Parquet files partitioned    |
|  Results -> dq_results   |  |  by party type (golden zone)  |
+-------------------------+  +------------------------------+
                               |
                               v
+-----------------------------------------------------------------+
|                    STREAMING LAYER                               |
|              Kafka Producer -> party.events.raw                  |
|              Kafka Consumer -> PostgreSQL updates               |
|              -> party.events.updates (confirmations)            |
|              -> S3 refresh after processing                     |
+------------------------------+----------------------------------+
                               |
                               v
+-----------------------------------------------------------------+
|                    SERVING LAYER                                 |
|              FastAPI REST API (18+ endpoints)                   |
|              Party search, lineage, source records              |
|              Pipeline triggers, system stats                    |
+------------------------------+----------------------------------+
                               |
                               v
+-----------------------------------------------------------------+
|              PARTY MASTER CONTROL CENTER                        |
|         React dashboard served via Nginx                        |
|   Pipeline flow | Charts | S3 Explorer | Kafka Monitor          |
+-----------------------------------------------------------------+
```

---

## AWS Production Mapping

| Local Component | AWS Equivalent |
|----------------|---------------|
| PostgreSQL (Docker) | RDS Aurora PostgreSQL |
| Kafka (Docker) | MSK (Managed Streaming for Kafka) |
| LocalStack S3 | S3 |
| Python ETL scripts | AWS Glue |
| FastAPI | Lambda + API Gateway |
| Nginx + React | CloudFront + S3 Static Hosting |
| Docker Compose | ECS Fargate |

---

## Tech Stack

**Backend:** Python 3.12, FastAPI, psycopg2, PyArrow, recordlinkage, confluent-kafka, boto3, Faker, loguru

**Frontend:** React 18, Recharts, IBM Plex Mono

**Infrastructure:** Docker, PostgreSQL 15, Apache Kafka, LocalStack, Nginx

---

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.12+
- Node.js 20+ (for dashboard development only)
- Git

### 1 - Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/party-master-hub.git
cd party-master-hub
```

### 2 - Start all infrastructure
```bash
cd docker
docker-compose up -d
```

### 3 - Set up Python environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux
pip install -r requirements.txt
```

### 4 - Initialize database and S3
```bash
python src/utils/init_db.py
python src/utils/init_localstack.py
```

### 5 - Run the full pipeline
```bash
python src/ingestion/generate_party_data.py
python src/ingestion/ingest_to_postgres.py
python src/mastering/master_parties.py
python src/quality/run_quality_checks.py
python src/pipeline/export_golden_to_s3.py
```

### 6 - Start the API
```bash
python src/api/dashboard_api.py
```

### 7 - Open the dashboard
```
http://localhost:3000      <- Control Center Dashboard
http://localhost:8000/docs <- Swagger API UI
http://localhost:8080      <- Kafka UI
http://localhost:5050      <- pgAdmin
```

---

## Data Model

| Table | Description |
|-------|-------------|
| source_parties | Raw records from each source system before mastering |
| golden_parties | One master record per unique real-world entity |
| party_clusters | Links source records to their golden record |
| party_lineage | Full audit trail of every golden record event |
| dq_results | Historical data quality run results |

---

## API Endpoints

| Method | Endpoint | Description |
|--------|---------|-------------|
| GET | `/health` | System health check |
| GET | `/parties/stats` | Summary statistics |
| GET | `/parties/search` | Search with filters |
| GET | `/parties/{id}` | Fetch golden record |
| GET | `/parties/{id}/lineage` | Full audit trail |
| GET | `/parties/{id}/sources` | Contributing sources |
| GET | `/pipeline/status` | Pipeline freshness |
| POST | `/pipeline/trigger/{step}` | Trigger pipeline step |
| GET | `/system/postgres/stats` | Database statistics |
| GET | `/system/s3/files` | S3 file listing |
| GET | `/system/s3/preview/{zone}/{path}` | File preview |
| GET | `/system/kafka/stats` | Kafka metrics |
| GET | `/dashboard/charts` | Chart data |

---

## Running the Kafka Event Stream

Open two terminals:

**Terminal 1 - Consumer (start first):**
```bash
python src/streaming/party_consumer.py
```

**Terminal 2 - Producer:**
```bash
python src/streaming/party_producer.py
```

---

## License

MIT
