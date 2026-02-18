"""
health_check.py
---------------
Verifies all local services (Postgres, Kafka, LocalStack) are up and ready.
Run this after docker-compose up and before starting any pipeline.

Usage:
    python src/utils/health_check.py
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from dotenv import load_dotenv
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=BASE_DIR / 'config' / '.env')

import psycopg2
import boto3
import requests
from confluent_kafka.admin import AdminClient
from loguru import logger


def check_postgres() -> bool:
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0].split(',')[0]
        cur.close()
        conn.close()
        logger.success(f"[OK] PostgreSQL connected - {version}")
        return True
    except Exception as e:
        logger.error(f"[FAIL] PostgreSQL: {e}")
        return False


def check_postgres_schema() -> bool:
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'party_master'
        """)
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        if count >= 4:
            logger.success(f"[OK] Database schema 'party_master' exists ({count} tables)")
            return True
        else:
            logger.warning(f"[WARN] Schema exists but only {count} tables found. Run init_db.py first.")
            return False
    except Exception as e:
        logger.error(f"[FAIL] Schema check: {e}")
        return False


def check_kafka() -> bool:
    try:
        admin = AdminClient({
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'socket.timeout.ms': 5000
        })
        metadata = admin.list_topics(timeout=5)
        logger.success(f"[OK] Kafka broker reachable at {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
        return True
    except Exception as e:
        logger.error(f"[FAIL] Kafka: {e}")
        return False


def check_localstack() -> bool:
    try:
        endpoint = os.getenv('AWS_ENDPOINT_URL', 'http://localhost:4566')
        response = requests.get(f"{endpoint}/_localstack/health", timeout=5)
        if response.status_code == 200:
            services = response.json().get('services', {})
            s3_status = services.get('s3', 'unknown')
            logger.success(f"[OK] LocalStack reachable at {endpoint} (S3: {s3_status})")
            return True
        else:
            logger.error(f"[FAIL] LocalStack returned status {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"[FAIL] LocalStack: {e}")
        return False


def check_s3_buckets() -> bool:
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=os.getenv('AWS_ENDPOINT_URL'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        )
        response = s3.list_buckets()
        bucket_names = [b['Name'] for b in response['Buckets']]

        expected = [
            os.getenv('S3_RAW_BUCKET', 'party-raw-zone'),
            os.getenv('S3_CURATED_BUCKET', 'party-curated-zone'),
            os.getenv('S3_GOLDEN_BUCKET', 'party-golden-zone'),
        ]

        all_ok = True
        for bucket in expected:
            if bucket in bucket_names:
                logger.success(f"[OK] S3 bucket '{bucket}' exists")
            else:
                logger.warning(f"[WARN] S3 bucket '{bucket}' NOT found. Run init_localstack.py first.")
                all_ok = False
        return all_ok
    except Exception as e:
        logger.error(f"[FAIL] S3 bucket check: {e}")
        return False


def run_health_check():
    logger.info("=" * 55)
    logger.info("  Party Master Data Hub — Health Check")
    logger.info("=" * 55)

    results = {
        "PostgreSQL":       check_postgres(),
        "DB Schema":        check_postgres_schema(),
        "Kafka":            check_kafka(),
        "LocalStack":       check_localstack(),
        "S3 Buckets":       check_s3_buckets(),
    }

    logger.info("=" * 55)
    all_passed = all(results.values())

    if all_passed:
        logger.success("[OK] All systems ready. Proceed to Step 2 — Data Generation!")
    else:
        failed = [k for k, v in results.items() if not v]
        logger.error(f"[!!] Some checks failed: {failed}")
        logger.info("Refer to SETUP_GUIDE.md Troubleshooting section.")
        sys.exit(1)


if __name__ == "__main__":
    run_health_check()
