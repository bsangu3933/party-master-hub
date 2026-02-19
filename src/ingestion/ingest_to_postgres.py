"""
ingest_to_postgres.py
---------------------
Ingestion pipeline that:
  1. Reads raw CSV files from each source system
  2. Normalizes them into a common schema
  3. Uploads raw files to LocalStack S3 (raw zone)
  4. Loads normalized records into PostgreSQL source_parties table

This mirrors what AWS Glue would do in production at LPL Financial.

Usage:
    python src/ingestion/ingest_to_postgres.py
"""

import sys
import os
import csv
import json
import boto3
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(dotenv_path=BASE_DIR / 'config' / '.env')

import psycopg2
from psycopg2.extras import execute_batch
from loguru import logger


# ============================================================
# CONFIG
# ============================================================
DB_URL        = os.getenv('DATABASE_URL')
S3_ENDPOINT   = os.getenv('AWS_ENDPOINT_URL', 'http://localhost:4566')
S3_RAW_BUCKET = os.getenv('S3_RAW_BUCKET', 'party-raw-zone')
AWS_KEY       = os.getenv('AWS_ACCESS_KEY_ID', 'test')
AWS_SECRET    = os.getenv('AWS_SECRET_ACCESS_KEY', 'test')
AWS_REGION    = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

RAW_DATA_DIR  = BASE_DIR / 'data' / 'raw'


# ============================================================
# S3 CLIENT
# ============================================================
def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION
    )


# ============================================================
# UPLOAD RAW FILE TO S3
# ============================================================
def upload_to_s3(s3_client, local_path: Path, source_system: str):
    """Upload raw CSV to S3 raw zone before any transformation."""
    s3_key = f"{source_system}/{local_path.name}"
    try:
        s3_client.upload_file(
            Filename=str(local_path),
            Bucket=S3_RAW_BUCKET,
            Key=s3_key
        )
        logger.success(f"  Uploaded to S3: s3://{S3_RAW_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"  S3 upload failed: {e}")


# ============================================================
# NORMALIZERS — one per source system
# Each source has different field names, we map to common schema
# ============================================================

def normalize_crm_record(row: dict) -> dict:
    """
    Normalize Internal CRM individual record to common schema.
    CRM uses: first_name, last_name, advisor_id, kyc_status etc.
    """
    return {
        'source_system':    'internal_crm',
        'source_party_id':  row.get('source_party_id', ''),
        'party_type':       row.get('party_type', 'INDIVIDUAL'),
        'full_name':        row.get('full_name', '').strip(),
        'first_name':       row.get('first_name', '').strip(),
        'last_name':        row.get('last_name', '').strip(),
        'date_of_birth':    row.get('date_of_birth') or None,
        'tax_id':           row.get('tax_id', '').strip(),
        'lei':              None,  # Individuals don't have LEIs
        'address_line1':    row.get('address_line1', '').strip(),
        'address_line2':    row.get('address_line2', '').strip() or None,
        'city':             row.get('city', '').strip(),
        'state':            row.get('state', '').strip(),
        'postal_code':      row.get('postal_code', '').strip(),
        'country':          row.get('country', 'USA').strip(),
        'email':            row.get('email', '').strip() or None,
        'phone':            row.get('phone', '').strip() or None,
        'entity_status':    row.get('entity_status', 'ACTIVE').strip(),
        'raw_payload':      json.dumps(row),
    }


def normalize_dnb_record(row: dict) -> dict:
    """
    Normalize D&B organization record to common schema.
    D&B uses: duns_number, employee_count, annual_revenue, industry_code etc.
    These are stored in raw_payload but not in the standard columns.
    """
    return {
        'source_system':    'dnb_feed',
        'source_party_id':  row.get('source_party_id', ''),
        'party_type':       row.get('party_type', 'ORGANIZATION'),
        'full_name':        row.get('full_name', '').strip(),
        'first_name':       None,
        'last_name':        None,
        'date_of_birth':    None,
        'tax_id':           row.get('tax_id', '').strip(),
        'lei':              None,
        'address_line1':    row.get('address_line1', '').strip(),
        'address_line2':    row.get('address_line2', '').strip() or None,
        'city':             row.get('city', '').strip(),
        'state':            row.get('state', '').strip(),
        'postal_code':      row.get('postal_code', '').strip(),
        'country':          row.get('country', 'USA').strip(),
        'email':            None,
        'phone':            row.get('phone', '').strip() or None,
        'entity_status':    row.get('entity_status', 'ACTIVE').strip(),
        'raw_payload':      json.dumps(row),  # Preserve D&B-specific fields
    }


def normalize_markit_record(row: dict) -> dict:
    """
    Normalize S&P Markit legal entity record to common schema.
    Markit uses: lei, regulatory_status, jurisdiction, parent_lei etc.
    """
    return {
        'source_system':    'markit_feed',
        'source_party_id':  row.get('source_party_id', ''),
        'party_type':       row.get('party_type', 'LEGAL_ENTITY'),
        'full_name':        row.get('full_name', '').strip(),
        'first_name':       None,
        'last_name':        None,
        'date_of_birth':    None,
        'tax_id':           row.get('tax_id', '').strip(),
        'lei':              row.get('lei', '').strip() or None,
        'address_line1':    row.get('address_line1', '').strip(),
        'address_line2':    row.get('address_line2', '').strip() or None,
        'city':             row.get('city', '').strip(),
        'state':            row.get('state', '').strip(),
        'postal_code':      row.get('postal_code', '').strip(),
        'country':          row.get('country', 'USA').strip(),
        'email':            None,
        'phone':            None,
        'entity_status':    row.get('entity_status', 'ACTIVE').strip(),
        'raw_payload':      json.dumps(row),  # Preserve Markit-specific fields
    }


# ============================================================
# READ CSV
# ============================================================
def read_csv(filepath: Path) -> list:
    """Read CSV file and return list of row dicts."""
    records = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
    return records


# ============================================================
# LOAD TO POSTGRESQL
# ============================================================
INSERT_SQL = """
    INSERT INTO party_master.source_parties (
        source_system, source_party_id, party_type,
        full_name, first_name, last_name, date_of_birth,
        tax_id, lei, address_line1, address_line2,
        city, state, postal_code, country,
        email, phone, entity_status, raw_payload,
        ingested_at, updated_at
    ) VALUES (
        %(source_system)s, %(source_party_id)s, %(party_type)s,
        %(full_name)s, %(first_name)s, %(last_name)s, %(date_of_birth)s,
        %(tax_id)s, %(lei)s, %(address_line1)s, %(address_line2)s,
        %(city)s, %(state)s, %(postal_code)s, %(country)s,
        %(email)s, %(phone)s, %(entity_status)s, %(raw_payload)s,
        NOW(), NOW()
    )
    ON CONFLICT (source_system, source_party_id)
    DO UPDATE SET
        full_name       = EXCLUDED.full_name,
        tax_id          = EXCLUDED.tax_id,
        entity_status   = EXCLUDED.entity_status,
        raw_payload     = EXCLUDED.raw_payload,
        updated_at      = NOW();
"""


def load_to_postgres(conn, records: list, source_system: str) -> int:
    """
    Bulk load normalized records into source_parties table.
    Uses INSERT ... ON CONFLICT to handle reruns gracefully (upsert).
    """
    if not records:
        logger.warning(f"  No records to load for {source_system}")
        return 0

    try:
        with conn.cursor() as cur:
            execute_batch(cur, INSERT_SQL, records, page_size=100)
        conn.commit()
        logger.success(f"  Loaded {len(records)} records from {source_system} → PostgreSQL")
        return len(records)
    except Exception as e:
        conn.rollback()
        logger.error(f"  Failed loading {source_system}: {e}")
        raise


# ============================================================
# VERIFY LOADED RECORDS
# ============================================================
def verify_counts(conn):
    """Print record counts per source system after loading."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT source_system, party_type, COUNT(*) as count
            FROM party_master.source_parties
            GROUP BY source_system, party_type
            ORDER BY source_system
        """)
        rows = cur.fetchall()

    logger.info("  Records in source_parties table:")
    total = 0
    for source, ptype, count in rows:
        logger.info(f"    {source:<20} {ptype:<15} {count:>5} records")
        total += count
    logger.info(f"    {'TOTAL':<20} {'':15} {total:>5} records")


# ============================================================
# MAIN PIPELINE
# ============================================================
def run_ingestion():
    logger.info("=" * 60)
    logger.info("  Party Master Data Hub — Ingestion Pipeline")
    logger.info("=" * 60)

    # --- Connect to PostgreSQL ---
    logger.info("Connecting to PostgreSQL...")
    conn = psycopg2.connect(DB_URL)
    logger.success("  PostgreSQL connected")

    # --- Connect to S3 ---
    logger.info("Connecting to LocalStack S3...")
    s3 = get_s3_client()
    logger.success("  S3 connected")

    total_loaded = 0

    # -------------------------------------------------------
    # SOURCE 1 — Internal CRM Individuals
    # -------------------------------------------------------
    logger.info("\n[1/3] Processing Internal CRM feed...")
    crm_file = RAW_DATA_DIR / 'internal_crm' / 'individuals.csv'

    if crm_file.exists():
        # Upload raw file to S3 first
        upload_to_s3(s3, crm_file, 'internal_crm')

        # Read, normalize, load
        raw_records  = read_csv(crm_file)
        normalized   = [normalize_crm_record(r) for r in raw_records]
        count        = load_to_postgres(conn, normalized, 'internal_crm')
        total_loaded += count
    else:
        logger.warning(f"  File not found: {crm_file}")

    # -------------------------------------------------------
    # SOURCE 2 — D&B Organizations
    # -------------------------------------------------------
    logger.info("\n[2/3] Processing D&B feed...")
    dnb_file = RAW_DATA_DIR / 'dnb_feed' / 'organizations.csv'

    if dnb_file.exists():
        upload_to_s3(s3, dnb_file, 'dnb_feed')

        raw_records  = read_csv(dnb_file)
        normalized   = [normalize_dnb_record(r) for r in raw_records]
        count        = load_to_postgres(conn, normalized, 'dnb_feed')
        total_loaded += count
    else:
        logger.warning(f"  File not found: {dnb_file}")

    # -------------------------------------------------------
    # SOURCE 3 — Markit Legal Entities
    # -------------------------------------------------------
    logger.info("\n[3/3] Processing S&P Markit feed...")
    markit_file = RAW_DATA_DIR / 'markit_feed' / 'legal_entities.csv'

    if markit_file.exists():
        upload_to_s3(s3, markit_file, 'markit_feed')

        raw_records  = read_csv(markit_file)
        normalized   = [normalize_markit_record(r) for r in raw_records]
        count        = load_to_postgres(conn, normalized, 'markit_feed')
        total_loaded += count
    else:
        logger.warning(f"  File not found: {markit_file}")

    # -------------------------------------------------------
    # VERIFY
    # -------------------------------------------------------
    logger.info("\nVerifying loaded records...")
    verify_counts(conn)

    conn.close()

    logger.info("=" * 60)
    logger.success(f"Ingestion complete! {total_loaded} total records loaded.")
    logger.info("Next: Run src/mastering/master_parties.py")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_ingestion()
