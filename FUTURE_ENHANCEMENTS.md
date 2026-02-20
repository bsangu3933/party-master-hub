# Future Enhancements Roadmap — Party Master Data Hub

Grouped by effort and impact. Each enhancement maps to a real-world production capability.

---

## Phase 1 — AWS Migration (High Impact, Medium Effort)

The local architecture is a direct mirror of AWS. Migration is straightforward:

### 1.1 Real S3
Replace LocalStack with actual S3 buckets. Change only the boto3 endpoint URL — all code stays identical.

### 1.2 RDS Aurora PostgreSQL
Provision an RDS instance. Change only the DATABASE_URL connection string. The schema, queries, and psycopg2 driver are all identical.

### 1.3 MSK (Managed Kafka)
Replace the local Kafka container with an MSK cluster. Change only the bootstrap servers in the .env file. The confluent-kafka client works identically against MSK.

### 1.4 ECS Fargate
Deploy the API and dashboard containers to ECS Fargate using the existing Dockerfiles. Add an Application Load Balancer in front of the API. Move the React dashboard to S3 + CloudFront for global distribution.

### 1.5 Secrets Manager
Replace .env file credentials with AWS Secrets Manager. Add boto3 calls to fetch secrets at startup — a 10-line change that dramatically improves security posture.

---

## Phase 2 — Pipeline Orchestration (High Impact, Medium Effort)

### 2.1 Apache Airflow
Replace the manual pipeline triggers with an Airflow DAG. Each pipeline step becomes a PythonOperator with explicit upstream dependencies. Add scheduling (daily at 2am), retry logic (3 retries with 5-minute backoff), SLA monitoring, and email alerting on failure.

```
generate_data >> ingest >> master >> [quality, export_s3]
```

### 2.2 AWS Step Functions
Alternative to Airflow for AWS-native orchestration. Each pipeline step becomes a Lambda function or ECS task. Step Functions handles retries, error handling, and parallel execution natively.

### 2.3 Event-Driven Triggers
Replace scheduled runs with event-driven triggers. When a new file lands in S3 raw zone, EventBridge fires → triggers the ingest step → which triggers mastering → which triggers quality checks. Zero polling, instant pipeline execution.

---

## Phase 3 — Data Quality Improvements (High Impact, Low Effort)

### 3.1 Cross-Source Consistency Checks
Validate that the same entity's address in D&B matches the CRM within acceptable tolerance. Flag records where sources significantly disagree — these are candidates for manual review.

### 3.2 Freshness Monitoring
Flag golden records that haven't been updated from any source in over 90 days. These records may represent stale client data that needs refresh.

### 3.3 Quality Trend Dashboard
Extend the DQ panel to show pass rates over time as a line chart — tracking whether data quality is improving or degrading across pipeline runs.

### 3.4 Threshold Alerting
Send alerts when quality metrics drop below thresholds. If the Tax ID null rate jumps from 2% to 15% between runs, trigger a Slack or email alert before bad data propagates.

### 3.5 Reference Data Validation
Validate against external reference lists — GLEIF for LEI validation, OFAC for sanctions screening, ISO 3166 for country codes. Flag records with invalid reference values before mastering.

---

## Phase 4 — Identity Resolution Improvements (High Impact, High Effort)

### 4.1 Machine Learning Matching
Replace the rule-based fuzzy matching with a trained classifier. Use labeled match/no-match pairs to train a logistic regression or gradient boosting model. Libraries like Splink use probabilistic record linkage based on the Fellegi-Sunter model and learn optimal field weights from the data.

### 4.2 Phonetic Blocking
Add Soundex or Double Metaphone blocking on company names to catch phonetic variants — "Smith" and "Smyth", "MacDonald" and "McDonald". This dramatically improves recall on individual name matching.

### 4.3 Address Standardization
Normalize addresses before matching using USPS CASS certification or a library like usaddress. "123 Main St" and "123 Main Street Suite 100" should match if they represent the same entity.

### 4.4 Entity Hierarchy Resolution
For legal entities, resolve the ownership hierarchy using D&B family tree data or GLEIF parent/child LEI relationships. Link subsidiaries to their ultimate parent — critical for AML beneficial ownership analysis.

---

## Phase 5 — Streaming Enhancements (Medium Impact, Medium Effort)

### 5.1 Kafka Schema Registry
Add Confluent Schema Registry to enforce Avro schemas on all Kafka messages. Prevents producers from publishing malformed events that break consumers. Schema evolution with backward/forward compatibility.

### 5.2 Dead Letter Queue
Route failed events to a `party.events.dlq` topic instead of silently dropping them. Build a separate consumer that processes DLQ events with enhanced error logging and alerting.

### 5.3 Exactly-Once Semantics
Enable Kafka transactions on the consumer to guarantee exactly-once processing. Prevents duplicate database updates if the consumer crashes mid-processing.

### 5.4 Real-Time S3 Streaming
Instead of batch S3 refresh after consumer run, write each golden record update to S3 immediately using a micro-batch approach. Use PyArrow to append to existing Parquet files rather than full rewrites.

---

## Phase 6 — API and Dashboard Improvements (Medium Impact, Low Effort)

### 6.1 Authentication
Add JWT authentication to the API. Require a bearer token for all endpoints. Different token scopes for read-only vs pipeline trigger access.

### 6.2 Rate Limiting
Add per-client rate limiting using FastAPI middleware. Prevent abuse of expensive endpoints like search and preview.

### 6.3 Search Improvements
Add full-text search using PostgreSQL tsvector on name fields. Add fuzzy search capability so a user searching "Jon Smith" finds "John Smith". Add pagination cursors instead of offset-based pagination for large result sets.

### 6.4 Dashboard — Party Detail View
Add a drilldown panel where clicking a golden record shows its full details, all contributing source records side by side, the complete lineage timeline, and confidence score breakdown.

### 6.5 Dashboard — Data Steward Workflow
Add a manual review queue for uncertain matches — records with confidence scores between 0.7 and 0.9. Data stewards can confirm or reject proposed merges directly from the UI.

### 6.6 Dashboard — Real-Time Kafka Feed
Replace the lineage-based Kafka monitor with a real WebSocket feed showing events as they flow through the system in real time.

---

## Phase 7 — Production Hardening (Essential Before Go-Live)

### 7.1 Secrets Management
Move all credentials from .env files to AWS Secrets Manager or HashiCorp Vault. Rotate database passwords automatically.

### 7.2 Monitoring and Observability
Add Prometheus metrics to FastAPI. Build a Grafana dashboard showing API latency, error rates, pipeline run durations, and Kafka consumer lag. Set up PagerDuty alerting for critical failures.

### 7.3 Disaster Recovery
Enable automated RDS snapshots with point-in-time recovery. Configure S3 versioning on the golden zone bucket. Document RTO (recovery time objective) and RPO (recovery point objective).

### 7.4 CI/CD Pipeline
Add GitHub Actions workflows for automated testing on every pull request, Docker image builds on merge to main, and automated deployment to staging and production environments.

### 7.5 Data Encryption
Enable encryption at rest on RDS (AES-256) and S3 (SSE-S3 or SSE-KMS). Enable TLS in transit for all Kafka connections. Encrypt sensitive fields (SSN, Tax ID) at the application layer using AWS KMS before storing in PostgreSQL.

### 7.6 Compliance Controls
Add field-level access control — not every API consumer should see Tax IDs or SSNs. Implement data masking for non-privileged consumers. Add GDPR right-to-erasure support in the lineage system. Document data retention policies per field.

---

## Priority Order for a Real Production Deployment

1. AWS Migration (Phase 1) — gets you off local infrastructure
2. Secrets Management (Phase 7.1) — security baseline
3. Orchestration (Phase 2.1 Airflow) — operational reliability
4. CI/CD (Phase 7.4) — engineering best practice
5. Monitoring (Phase 7.2) — visibility into production
6. Schema Registry (Phase 5.1) — streaming reliability
7. ML Matching (Phase 4.1) — better match quality
8. Data Steward Workflow (Phase 6.5) — operational completeness
