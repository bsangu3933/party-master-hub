# Installation Guide — Party Master Data Hub
# Complete setup from zero to fully running system

---

## Prerequisites

Install these before anything else:

| Tool | Version | Download |
|------|---------|----------|
| Docker Desktop | Latest | https://www.docker.com/products/docker-desktop |
| Python | 3.12+ | https://www.python.org/downloads |
| Node.js | 20 LTS | https://nodejs.org |
| Git | Latest | https://git-scm.com |
| VS Code | Latest | https://code.visualstudio.com |

After installing each tool, close and reopen your terminal to reload PATH.

Verify installations:
```powershell
docker --version
python --version
node --version
npm --version
git --version
```

---

## Step 1 — Clone the Repository

```powershell
git clone https://github.com/YOUR_USERNAME/party-master-hub.git
cd party-master-hub
```

---

## Step 2 — Python Virtual Environment

```powershell
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Mac/Linux

pip install -r requirements.txt
```

If requirements.txt is missing, install manually:
```powershell
pip install fastapi uvicorn psycopg2-binary boto3 confluent-kafka
pip install pandas pyarrow faker python-dotenv loguru recordlinkage
pip install great-expectations requests
```

---

## Step 3 — Environment Variables

Create the config folder and .env file:
```powershell
mkdir config
```

Create `config/.env` with this content:
```
DATABASE_URL=postgresql://party_admin:party_secret@localhost:5433/party_master_db
AWS_ENDPOINT_URL=http://localhost:4566
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
S3_RAW_BUCKET=party-raw-zone
S3_CURATED_BUCKET=party-curated-zone
S3_GOLDEN_BUCKET=party-golden-zone
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_PARTY_EVENTS=party.events.raw
KAFKA_TOPIC_PARTY_UPDATES=party.events.updates
API_HOST=0.0.0.0
API_PORT=8000
```

---

## Step 4 — Start Docker Infrastructure

```powershell
cd docker
docker-compose up -d
```

Wait 30-60 seconds for all containers to be healthy:
```powershell
docker ps
```

All 6 base services should show as healthy:
```
party_postgres     Up (healthy)
party_zookeeper    Up
party_kafka        Up (healthy)
party_kafka_ui     Up
party_localstack   Up (healthy)
party_pgadmin      Up
```

---

## Step 5 — Initialize Database Schema

```powershell
cd ..   # back to project root
python src/utils/init_db.py
```

This creates the `party_master` schema and all 5 tables:
- source_parties
- golden_parties
- party_clusters
- party_lineage
- dq_results

Verify in pgAdmin at http://localhost:5050
- Email: admin@party.com
- Password: admin
- Add server: host=localhost, port=5433, user=party_admin, password=party_secret

---

## Step 6 — Initialize S3 Buckets

```powershell
python src/utils/init_localstack.py
```

This creates 3 S3 buckets in LocalStack:
- party-raw-zone
- party-curated-zone
- party-golden-zone

---

## Step 7 — Run the Full Pipeline

Run each step in order:

```powershell
# Generate 485 fake party records across 3 source systems
python src/ingestion/generate_party_data.py

# Ingest CSV files into PostgreSQL and upload to S3 raw zone
python src/ingestion/ingest_to_postgres.py

# Run identity resolution and create 450 golden records
python src/mastering/master_parties.py

# Run 3 data quality validation suites
python src/quality/run_quality_checks.py

# Export golden records to S3 as Parquet files
python src/pipeline/export_golden_to_s3.py
```

Expected output after mastering:
```
485 source records ingested
450 golden records created
35 duplicates resolved
```

---

## Step 8 — Start the API

```powershell
python src/api/dashboard_api.py
```

Verify at http://localhost:8000/health — should return:
```json
{"status": "healthy", "database": "connected"}
```

Swagger UI at http://localhost:8000/docs

---

## Step 9 — Build and Start the Dashboard

**Option A — Local development (npm):**
```powershell
cd dashboard
npm install
npm start
```
Opens automatically at http://localhost:3000

**Option B — Full Docker (recommended):**
```powershell
# Build React app first
cd dashboard
npm run build
cd ..

# Start API and dashboard containers
cd docker
docker-compose up -d api dashboard
```

---

## Step 10 — Run Kafka Event Streaming

Open two terminals:

**Terminal 1 — Start consumer first:**
```powershell
venv\Scripts\activate
python src/streaming/party_consumer.py
```

**Terminal 2 — Run producer:**
```powershell
venv\Scripts\activate
python src/streaming/party_producer.py
```

The producer publishes 20 events. The consumer processes them,
updates PostgreSQL, and refreshes S3. Watch the consumer terminal
for real-time processing output.

---

## Service URLs Summary

| Service | URL | Credentials |
|---------|-----|-------------|
| Dashboard | http://localhost:3000 | — |
| API | http://localhost:8000 | — |
| Swagger UI | http://localhost:8000/docs | — |
| pgAdmin | http://localhost:5050 | admin@party.com / admin |
| Kafka UI | http://localhost:8080 | — |
| LocalStack | http://localhost:4566 | — |

---

## Common Issues and Fixes

**Docker containers not starting:**
```powershell
docker-compose down
docker-compose up -d
```

**PostgreSQL connection refused:**
- Make sure port 5433 is not in use by another application
- Wait 30 seconds after docker-compose up before connecting

**ModuleNotFoundError:**
- Make sure venv is activated: `venv\Scripts\activate`
- Reinstall: `pip install -r requirements.txt`

**Kafka connection error:**
- Wait 60 seconds after docker-compose up — Kafka takes longer to start
- Check kafka is healthy: `docker ps | grep kafka`

**S3 buckets not found:**
- Re-run init_localstack.py — LocalStack resets on container restart
- LocalStack does NOT persist data between restarts unless you mount a volume

**npm not recognized:**
- Close and reopen VS Code after installing Node.js
- Node.js must be added to PATH during installation

---

## Starting Everything Daily

Once everything is set up, your daily startup is:

```powershell
# 1. Start Docker services
cd docker
docker-compose up -d

# 2. Re-initialize S3 buckets (LocalStack resets on restart)
cd ..
venv\Scripts\activate
python src/utils/init_localstack.py

# 3. Re-run pipeline (if you want fresh data)
python src/ingestion/generate_party_data.py
python src/ingestion/ingest_to_postgres.py
python src/mastering/master_parties.py
python src/quality/run_quality_checks.py
python src/pipeline/export_golden_to_s3.py

# 4. Start API (if not using Docker for API)
python src/api/dashboard_api.py
```

Dashboard at http://localhost:3000 is ready.

---

## Resetting Everything

To wipe all data and start completely fresh:

```powershell
cd docker
docker-compose down -v    # -v removes all volumes including database
docker-compose up -d
```

Then repeat Steps 5-10.
