"""
dashboard_api.py
----------------
Extended FastAPI endpoints to power the Party Master Control Center dashboard.

New endpoint groups:
  /pipeline  — pipeline step status and triggers
  /system    — PostgreSQL, S3, and Kafka system stats
  /dashboard — aggregated data for charts and metrics

Usage:
    python src/api/dashboard_api.py
    Then open: http://localhost:8000/docs
"""

import sys
import os
import json
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List

BASE_DIR = Path(__file__).resolve().parent.parent.parent
VENV_PYTHON = str(BASE_DIR / 'venv' / 'Scripts' / 'python.exe')
sys.path.append(str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(dotenv_path=BASE_DIR / 'config' / '.env')

import psycopg2
import psycopg2.extras
import boto3
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from loguru import logger

# Import existing party endpoints
sys.path.insert(0, str(BASE_DIR / 'src' / 'api'))
from main import (
    app as party_app,
    get_db_connection,
    serialize_row,
    GoldenParty,
    SearchResult,
    StatsResult,
    HealthResponse,
    LineageEvent,
    SourceRecord,
)

# ============================================================
# APP SETUP — extend the existing party app
# ============================================================
app = party_app
app.title = "Party Master Control Center API"
app.description = """
Complete API powering the Party Master Control Center dashboard.
Includes party data endpoints plus pipeline management,
system monitoring, and dashboard analytics.
"""

# ============================================================
# CONFIG
# ============================================================
DB_URL           = os.getenv('DATABASE_URL')
S3_ENDPOINT      = os.getenv('AWS_ENDPOINT_URL', 'http://localhost:4566')
S3_RAW_BUCKET    = os.getenv('S3_RAW_BUCKET', 'party-raw-zone')
S3_CURATED_BUCKET= os.getenv('S3_CURATED_BUCKET', 'party-curated-zone')
S3_GOLDEN_BUCKET = os.getenv('S3_GOLDEN_BUCKET', 'party-golden-zone')
AWS_KEY          = os.getenv('AWS_ACCESS_KEY_ID', 'test')
AWS_SECRET       = os.getenv('AWS_SECRET_ACCESS_KEY', 'test')
AWS_REGION       = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
KAFKA_SERVERS    = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC_RAW  = os.getenv('KAFKA_TOPIC_PARTY_EVENTS', 'party.events.raw')
KAFKA_TOPIC_UPD  = os.getenv('KAFKA_TOPIC_PARTY_UPDATES', 'party.events.updates')

# Pipeline steps in dependency order
PIPELINE_STEPS = [
    'generate_data',
    'ingest',
    'master',
    'quality',
    'export_s3',
]

PIPELINE_SCRIPTS = {
    'generate_data': BASE_DIR / 'src' / 'ingestion' / 'generate_party_data.py',
    'ingest':        BASE_DIR / 'src' / 'ingestion' / 'ingest_to_postgres.py',
    'master':        BASE_DIR / 'src' / 'mastering' / 'master_parties.py',
    'quality':       BASE_DIR / 'src' / 'quality'   / 'run_quality_checks.py',
    'export_s3':     BASE_DIR / 'src' / 'pipeline'  / 'export_golden_to_s3.py',
}

# In-memory store for pipeline run status
pipeline_status = {step: {
    'status':       'unknown',
    'last_run':     None,
    'duration_sec': None,
    'message':      'Not yet run',
} for step in PIPELINE_STEPS}

# Track background task status
running_tasks = {}


# ============================================================
# HELPERS
# ============================================================
def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION
    )


def get_kafka_admin():
    return AdminClient({'bootstrap.servers': KAFKA_SERVERS})


# ============================================================
# PIPELINE ENDPOINTS
# ============================================================

@app.get("/pipeline/status", tags=["Pipeline"])
def get_pipeline_status():
    """
    Get the current status and freshness of each pipeline step.
    Returns last run time, duration, and whether data is stale.
    """
    conn = get_db_connection()
    result = {}

    try:
        with conn.cursor() as cur:

            # generate_data — check if raw CSV files exist
            raw_files = list((BASE_DIR / 'data' / 'raw').rglob('*.csv'))
            if raw_files:
                latest = max(f.stat().st_mtime for f in raw_files)
                last_run = datetime.fromtimestamp(latest)
                age_hours = (datetime.now() - last_run).total_seconds() / 3600
                result['generate_data'] = {
                    'status':     'success',
                    'last_run':   last_run.isoformat(),
                    'age_hours':  round(age_hours, 1),
                    'is_stale':   age_hours > 24,
                    'details':    f"{len(raw_files)} CSV files generated",
                }
            else:
                result['generate_data'] = {
                    'status':   'not_run',
                    'last_run': None,
                    'is_stale': True,
                    'details':  'No raw data files found',
                }

            # ingest — check source_parties table
            cur.execute("""
                SELECT COUNT(*) as count,
                       MAX(ingested_at) as last_ingested
                FROM party_master.source_parties
            """)
            row = dict(cur.fetchone())
            if row['count'] > 0:
                last_run = row['last_ingested']
                age_hours = (datetime.now() - last_run).total_seconds() / 3600
                result['ingest'] = {
                    'status':     'success',
                    'last_run':   last_run.isoformat(),
                    'age_hours':  round(age_hours, 1),
                    'is_stale':   age_hours > 24,
                    'details':    f"{row['count']} source records loaded",
                }
            else:
                result['ingest'] = {
                    'status':   'not_run',
                    'last_run': None,
                    'is_stale': True,
                    'details':  'No source records found',
                }

            # master — check golden_parties table
            cur.execute("""
                SELECT COUNT(*) as count,
                       MAX(created_at) as last_created
                FROM party_master.golden_parties
            """)
            row = dict(cur.fetchone())
            if row['count'] > 0:
                last_run = row['last_created']
                age_hours = (datetime.now() - last_run).total_seconds() / 3600
                result['master'] = {
                    'status':     'success',
                    'last_run':   last_run.isoformat(),
                    'age_hours':  round(age_hours, 1),
                    'is_stale':   age_hours > 24,
                    'details':    f"{row['count']} golden records created",
                }
            else:
                result['master'] = {
                    'status':   'not_run',
                    'last_run': None,
                    'is_stale': True,
                    'details':  'No golden records found',
                }

            # quality — check dq_results table
            cur.execute("""
                SELECT COUNT(*) as runs,
                       MAX(run_at) as last_run,
                       AVG(pass_rate) as avg_pass_rate
                FROM party_master.dq_results
            """)
            row = dict(cur.fetchone())
            if row['runs'] > 0:
                last_run = row['last_run']
                age_hours = (datetime.now() - last_run).total_seconds() / 3600
                result['quality'] = {
                    'status':       'success',
                    'last_run':     last_run.isoformat(),
                    'age_hours':    round(age_hours, 1),
                    'is_stale':     age_hours > 24,
                    'details':      f"Pass rate: {round(float(row['avg_pass_rate'] or 0)*100, 1)}%",
                }
            else:
                result['quality'] = {
                    'status':   'not_run',
                    'last_run': None,
                    'is_stale': True,
                    'details':  'No quality runs found',
                }

    finally:
        conn.close()

    # export_s3 — check S3 golden zone
    try:
        s3 = get_s3_client()
        response = s3.list_objects_v2(
            Bucket=S3_GOLDEN_BUCKET,
            Prefix='golden_records/full/'
        )
        if 'Contents' in response and response['Contents']:
            obj = response['Contents'][0]
            last_run = obj['LastModified'].replace(tzinfo=None)
            age_hours = (datetime.now() - last_run).total_seconds() / 3600
            result['export_s3'] = {
                'status':     'success',
                'last_run':   last_run.isoformat(),
                'age_hours':  round(age_hours, 1),
                'is_stale':   age_hours > 24,
                'details':    f"Size: {round(obj['Size']/1024, 1)} KB",
            }
        else:
            result['export_s3'] = {
                'status':   'not_run',
                'last_run': None,
                'is_stale': True,
                'details':  'No Parquet files found in S3',
            }
    except Exception as e:
        result['export_s3'] = {
            'status':   'error',
            'last_run': None,
            'is_stale': True,
            'details':  str(e),
        }

    return {
        'pipeline_steps': result,
        'checked_at':     datetime.now().isoformat(),
    }


@app.post("/pipeline/trigger/{step}", tags=["Pipeline"])
def trigger_pipeline_step(step: str, background_tasks: BackgroundTasks):
    """
    Trigger a specific pipeline step to run.
    Steps run as background tasks so the API stays responsive.
    Dependencies must be completed before triggering a step.
    """
    if step not in PIPELINE_SCRIPTS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown step '{step}'. Valid: {list(PIPELINE_SCRIPTS.keys())}"
        )

    if running_tasks.get(step) == 'running':
        raise HTTPException(
            status_code=409,
            detail=f"Step '{step}' is already running"
        )

    def run_step(step_name: str):
        running_tasks[step_name] = 'running'
        pipeline_status[step_name]['status'] = 'running'
        start = datetime.now()
        logger.info(f"Triggering pipeline step: {step_name}")

        try:
            script = PIPELINE_SCRIPTS[step_name]
            result = subprocess.run(
                [VENV_PYTHON, str(script)],
                capture_output=True,
                text=True,
                timeout=300,
                cwd=str(BASE_DIR)
            )
            duration = (datetime.now() - start).total_seconds()

            if result.returncode == 0:
                pipeline_status[step_name] = {
                    'status':       'success',
                    'last_run':     datetime.now().isoformat(),
                    'duration_sec': round(duration, 1),
                    'message':      'Completed successfully',
                }
                running_tasks[step_name] = 'success'
                logger.success(f"Step {step_name} completed in {duration:.1f}s")
            else:
                pipeline_status[step_name] = {
                    'status':       'error',
                    'last_run':     datetime.now().isoformat(),
                    'duration_sec': round(duration, 1),
                    'message':      result.stderr[-500:] if result.stderr else 'Unknown error',
                }
                running_tasks[step_name] = 'error'
                logger.error(f"Step {step_name} failed: {result.stderr[-200:]}")

        except subprocess.TimeoutExpired:
            pipeline_status[step_name]['status'] = 'timeout'
            running_tasks[step_name] = 'error'
            logger.error(f"Step {step_name} timed out")
        except Exception as e:
            pipeline_status[step_name]['status'] = 'error'
            running_tasks[step_name] = 'error'
            logger.error(f"Step {step_name} error: {e}")

    background_tasks.add_task(run_step, step)

    return {
        'message':    f"Step '{step}' triggered successfully",
        'step':       step,
        'status':     'running',
        'started_at': datetime.now().isoformat(),
    }


@app.get("/pipeline/trigger/{step}/status", tags=["Pipeline"])
def get_trigger_status(step: str):
    """Get the current status of a triggered pipeline step."""
    if step not in PIPELINE_SCRIPTS:
        raise HTTPException(status_code=400, detail=f"Unknown step '{step}'")

    return {
        'step':   step,
        'status': running_tasks.get(step, 'not_triggered'),
        'details': pipeline_status.get(step, {}),
    }


# ============================================================
# SYSTEM — POSTGRESQL STATS
# ============================================================

@app.get("/system/postgres/stats", tags=["System"])
def get_postgres_stats():
    """
    Get detailed statistics from all PostgreSQL tables.
    Powers the PostgreSQL stats panel in the dashboard.
    """
    conn = get_db_connection()
    try:
        stats = {}
        with conn.cursor() as cur:

            # source_parties breakdown
            cur.execute("""
                SELECT source_system, party_type, entity_status,
                       COUNT(*) as count,
                       MAX(ingested_at) as last_ingested
                FROM party_master.source_parties
                GROUP BY source_system, party_type, entity_status
                ORDER BY source_system, count DESC
            """)
            stats['source_parties'] = {
                'rows': [dict(r) for r in cur.fetchall()]
            }
            cur.execute("SELECT COUNT(*) as total FROM party_master.source_parties")
            stats['source_parties']['total'] = cur.fetchone()['total']

            # golden_parties breakdown
            cur.execute("""
                SELECT party_type, entity_status,
                       COUNT(*) as count,
                       ROUND(AVG(confidence_score)::numeric, 3) as avg_confidence,
                       ROUND(MIN(confidence_score)::numeric, 3) as min_confidence
                FROM party_master.golden_parties
                GROUP BY party_type, entity_status
                ORDER BY party_type
            """)
            stats['golden_parties'] = {
                'rows': [dict(r) for r in cur.fetchall()]
            }
            cur.execute("SELECT COUNT(*) as total FROM party_master.golden_parties")
            stats['golden_parties']['total'] = cur.fetchone()['total']

            # party_clusters — duplicate resolution stats
            cur.execute("""
                SELECT
                    COUNT(DISTINCT golden_id) as total_golden,
                    COUNT(*) as total_links,
                    SUM(CASE WHEN is_survivor THEN 1 ELSE 0 END) as survivors,
                    COUNT(*) - COUNT(DISTINCT golden_id) as duplicates_resolved
                FROM party_master.party_clusters
            """)
            stats['party_clusters'] = dict(cur.fetchone())

            # party_lineage — event breakdown
            cur.execute("""
                SELECT event_type, COUNT(*) as count,
                       MAX(event_at) as last_event
                FROM party_master.party_lineage
                GROUP BY event_type
                ORDER BY count DESC
            """)
            lineage_rows = [dict(r) for r in cur.fetchall()]
            cur.execute("SELECT COUNT(*) as total FROM party_master.party_lineage")
            stats['party_lineage'] = {
                'total': cur.fetchone()['total'],
                'rows':  lineage_rows,
            }

            # dq_results — quality history
            cur.execute("""
                SELECT run_id, source_system, expectation_suite,
                       total_records, passed_records, pass_rate, run_at
                FROM party_master.dq_results
                ORDER BY run_at DESC
                LIMIT 20
            """)
            dq_rows = [dict(r) for r in cur.fetchall()]
            cur.execute("SELECT COUNT(DISTINCT run_id) as runs FROM party_master.dq_results")
            stats['dq_results'] = {
                'total_runs': cur.fetchone()['runs'],
                'rows':       dq_rows,
            }

        return {
            'stats':      stats,
            'fetched_at': datetime.now().isoformat(),
        }
    finally:
        conn.close()


# ============================================================
# SYSTEM — S3 STATS
# ============================================================

@app.get("/system/s3/files", tags=["System"])
def get_s3_files():
    """
    List all files across all S3 buckets with metadata.
    Powers the S3 Explorer panel in the dashboard.
    """
    s3 = get_s3_client()
    buckets = {
        'raw':     S3_RAW_BUCKET,
        'curated': S3_CURATED_BUCKET,
        'golden':  S3_GOLDEN_BUCKET,
    }

    result = {}
    total_size = 0
    total_files = 0

    for zone, bucket in buckets.items():
        try:
            response = s3.list_objects_v2(Bucket=bucket)
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('/'):
                        continue  # Skip folder placeholders
                    file_info = {
                        'key':           obj['Key'],
                        'size_bytes':    obj['Size'],
                        'size_kb':       round(obj['Size'] / 1024, 2),
                        'last_modified': obj['LastModified'].isoformat(),
                        'bucket':        bucket,
                        'zone':          zone,
                    }
                    files.append(file_info)
                    total_size  += obj['Size']
                    total_files += 1

            result[zone] = {
                'bucket':     bucket,
                'file_count': len(files),
                'total_size_kb': round(sum(f['size_bytes'] for f in files) / 1024, 2),
                'files':      files,
            }
        except Exception as e:
            result[zone] = {
                'bucket': bucket,
                'error':  str(e),
                'files':  [],
            }

    return {
        'zones':          result,
        'total_files':    total_files,
        'total_size_kb':  round(total_size / 1024, 2),
        'fetched_at':     datetime.now().isoformat(),
    }


@app.get("/system/s3/preview/{zone}/{path:path}", tags=["System"])
def preview_s3_file(zone: str, path: str):
    """
    Preview first 10 rows of a Parquet file from S3.
    Powers the file preview feature in the S3 Explorer panel.
    """
    import pyarrow.parquet as pq
    import io

    bucket_map = {
        'raw':     S3_RAW_BUCKET,
        'curated': S3_CURATED_BUCKET,
        'golden':  S3_GOLDEN_BUCKET,
    }

    if zone not in bucket_map:
        raise HTTPException(status_code=400, detail=f"Unknown zone '{zone}'")

    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=bucket_map[zone], Key=path)
        buffer   = io.BytesIO(response['Body'].read())
        table    = pq.read_table(buffer)
        df       = table.to_pandas().head(10)
        return {
            'file':        path,
            'total_rows':  len(table),
            'columns':     list(df.columns),
            'preview':     df.fillna('').to_dict(orient='records'),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# SYSTEM — KAFKA STATS
# ============================================================

@app.get("/system/kafka/stats", tags=["System"])
def get_kafka_stats():
    """
    Get Kafka topic stats and consumer group lag.
    Powers the Kafka Monitor panel in the dashboard.
    """
    result = {
        'topics':          {},
        'consumer_groups': {},
        'recent_events':   [],
    }

    try:
        admin  = get_kafka_admin()
        topics = admin.list_topics(timeout=5)

        # Get message counts per topic
        our_topics = [KAFKA_TOPIC_RAW, KAFKA_TOPIC_UPD]
        for topic_name in our_topics:
            if topic_name in topics.topics:
                partitions = topics.topics[topic_name].partitions
                result['topics'][topic_name] = {
                    'partitions': len(partitions),
                    'status':     'available',
                }
            else:
                result['topics'][topic_name] = {
                    'status': 'not_found'
                }

        # Get consumer group lag
        try:
            consumer = Consumer({
                'bootstrap.servers':  KAFKA_SERVERS,
                'group.id':           'dashboard-monitor',
                'auto.offset.reset':  'latest',
                'enable.auto.commit': False,
            })

            tps = [TopicPartition(KAFKA_TOPIC_RAW, 0)]
            committed = consumer.committed(tps, timeout=5)
            end_offsets = consumer.get_watermark_offsets(
                TopicPartition(KAFKA_TOPIC_RAW, 0), timeout=5
            )

            committed_offset = committed[0].offset if committed[0].offset >= 0 else 0
            end_offset       = end_offsets[1]
            lag              = max(0, end_offset - committed_offset)

            result['consumer_groups']['party-master-consumer'] = {
                'topic':            KAFKA_TOPIC_RAW,
                'committed_offset': committed_offset,
                'end_offset':       end_offset,
                'lag':              lag,
                'status':           'up_to_date' if lag == 0 else 'lagging',
            }
            result['topics'][KAFKA_TOPIC_RAW]['total_messages'] = end_offset
            consumer.close()

        except Exception as e:
            result['consumer_groups']['party-master-consumer'] = {
                'status': 'error',
                'detail': str(e),
            }

        # Get recent events from lineage as proxy for Kafka activity
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT golden_id::text, event_type, triggered_by,
                           event_at, changed_fields
                    FROM party_master.party_lineage
                    WHERE triggered_by = 'party_consumer.py'
                    ORDER BY event_at DESC
                    LIMIT 10
                """)
                result['recent_events'] = [serialize_row(r) for r in cur.fetchall()]
        finally:
            conn.close()

    except Exception as e:
        result['error'] = str(e)

    result['fetched_at'] = datetime.now().isoformat()
    return result


# ============================================================
# DASHBOARD — CHART DATA
# ============================================================

@app.get("/dashboard/charts", tags=["Dashboard"])
def get_chart_data():
    """
    Aggregated data for all dashboard charts.
    Returns data formatted for Recharts components.
    """
    conn = get_db_connection()
    try:
        charts = {}
        with conn.cursor() as cur:

            # Chart 1 — Golden records by party type (pie chart)
            cur.execute("""
                SELECT party_type as name, COUNT(*) as value
                FROM party_master.golden_parties
                GROUP BY party_type
            """)
            charts['golden_by_type'] = [dict(r) for r in cur.fetchall()]

            # Chart 2 — Source records by system (bar chart)
            cur.execute("""
                SELECT source_system as name, COUNT(*) as value
                FROM party_master.source_parties
                GROUP BY source_system
            """)
            charts['source_by_system'] = [dict(r) for r in cur.fetchall()]

            # Chart 3 — Confidence score distribution (histogram)
            cur.execute("""
                SELECT
                    CASE
                        WHEN confidence_score >= 0.99 THEN '0.99-1.00 (Exact)'
                        WHEN confidence_score >= 0.90 THEN '0.90-0.99 (High)'
                        WHEN confidence_score >= 0.80 THEN '0.80-0.90 (Good)'
                        ELSE 'Below 0.80 (Low)'
                    END as range,
                    COUNT(*) as count
                FROM party_master.golden_parties
                GROUP BY range
                ORDER BY range DESC
            """)
            charts['confidence_distribution'] = [dict(r) for r in cur.fetchall()]

            # Chart 4 — Data quality pass rates (bar chart)
            cur.execute("""
                SELECT expectation_suite as suite,
                       ROUND(AVG(pass_rate)::numeric * 100, 1) as pass_rate
                FROM party_master.dq_results
                GROUP BY expectation_suite
            """)
            charts['quality_pass_rates'] = [dict(r) for r in cur.fetchall()]

            # Chart 5 — Lineage events by type (bar chart)
            cur.execute("""
                SELECT event_type as name, COUNT(*) as value
                FROM party_master.party_lineage
                GROUP BY event_type
                ORDER BY value DESC
            """)
            charts['lineage_events'] = [dict(r) for r in cur.fetchall()]

            # Chart 6 — Entity status breakdown (pie chart)
            cur.execute("""
                SELECT entity_status as name, COUNT(*) as value
                FROM party_master.golden_parties
                GROUP BY entity_status
            """)
            charts['entity_status'] = [dict(r) for r in cur.fetchall()]

            # Chart 7 — Duplicate resolution summary (single stats)
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM party_master.source_parties) as total_source,
                    (SELECT COUNT(*) FROM party_master.golden_parties) as total_golden,
                    (SELECT COUNT(*) FROM party_master.source_parties) -
                    (SELECT COUNT(*) FROM party_master.golden_parties) as duplicates_resolved,
                    (SELECT ROUND(AVG(confidence_score)::numeric * 100, 1)
                     FROM party_master.golden_parties) as avg_confidence_pct
            """)
            charts['summary_stats'] = dict(cur.fetchone())

        return {
            'charts':     charts,
            'fetched_at': datetime.now().isoformat(),
        }
    finally:
        conn.close()


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    host = os.getenv('API_HOST', '0.0.0.0')
    port = int(os.getenv('API_PORT', '8000'))

    logger.info("=" * 60)
    logger.info("  Party Master Control Center — API Server")
    logger.info("=" * 60)
    logger.info(f"  Starting on http://{host}:{port}")
    logger.info(f"  Swagger UI:  http://localhost:{port}/docs")
    logger.info("=" * 60)

    uvicorn.run(
        "dashboard_api:app",
        host=host,
        port=port,
        reload=True,
        app_dir=str(Path(__file__).parent)
    )
