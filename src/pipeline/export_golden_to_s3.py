"""
export_golden_to_s3.py
----------------------
Exports golden party records from PostgreSQL to LocalStack S3
as Parquet files — organized by party type.

This mirrors what AWS Glue would do in production:
  - Read golden records from RDS
  - Write as Parquet to S3 golden zone
  - Downstream systems (reporting, analytics, CRM) read from S3

Output structure:
  s3://party-golden-zone/golden_records/party_type=INDIVIDUAL/golden_records.parquet
  s3://party-golden-zone/golden_records/party_type=ORGANIZATION/golden_records.parquet
  s3://party-golden-zone/golden_records/party_type=LEGAL_ENTITY/golden_records.parquet
  s3://party-golden-zone/golden_records/full/golden_records.parquet

Usage:
    python src/pipeline/export_golden_to_s3.py
"""

import sys
import os
import io
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(dotenv_path=BASE_DIR / 'config' / '.env')

import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from loguru import logger


# ============================================================
# CONFIG
# ============================================================
DB_URL          = os.getenv('DATABASE_URL')
S3_ENDPOINT     = os.getenv('AWS_ENDPOINT_URL', 'http://localhost:4566')
S3_GOLDEN_BUCKET = os.getenv('S3_GOLDEN_BUCKET', 'party-golden-zone')
AWS_KEY         = os.getenv('AWS_ACCESS_KEY_ID', 'test')
AWS_SECRET      = os.getenv('AWS_SECRET_ACCESS_KEY', 'test')
AWS_REGION      = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')


# ============================================================
# CLIENTS
# ============================================================
def get_connection():
    return psycopg2.connect(DB_URL)


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION
    )


# ============================================================
# LOAD GOLDEN RECORDS WITH LINEAGE
# ============================================================
def load_golden_records(conn) -> pd.DataFrame:
    """
    Load golden records joined with cluster stats.
    Includes how many source records contributed to each golden record.
    """
    query = """
        SELECT
            g.golden_id,
            g.party_type,
            g.full_name,
            g.first_name,
            g.last_name,
            g.date_of_birth,
            g.tax_id,
            g.lei,
            g.address_line1,
            g.address_line2,
            g.city,
            g.state,
            g.postal_code,
            g.country,
            g.email,
            g.phone,
            g.entity_status,
            g.confidence_score,
            g.created_at,
            g.updated_at,
            COUNT(c.source_party_id)        AS source_record_count,
            STRING_AGG(DISTINCT sp.source_system, ',')  AS contributing_sources
        FROM party_master.golden_parties g
        LEFT JOIN party_master.party_clusters c
            ON g.golden_id = c.golden_id
        LEFT JOIN party_master.source_parties sp
            ON c.source_party_id = sp.id
        GROUP BY
            g.golden_id, g.party_type, g.full_name, g.first_name,
            g.last_name, g.date_of_birth, g.tax_id, g.lei,
            g.address_line1, g.address_line2, g.city, g.state,
            g.postal_code, g.country, g.email, g.phone,
            g.entity_status, g.confidence_score,
            g.created_at, g.updated_at
        ORDER BY g.party_type, g.full_name
    """
    df = pd.read_sql(query, conn)

    # Add export metadata
    df['exported_at']    = datetime.now().isoformat()
    df['export_version'] = '1.0'

    logger.info(f"  Loaded {len(df)} golden records for export")
    return df


# ============================================================
# CONVERT TO PARQUET AND UPLOAD TO S3
# ============================================================
def df_to_parquet_buffer(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to Parquet bytes in memory."""
    # Convert to PyArrow table
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Write to in-memory buffer
    buffer = io.BytesIO()
    pq.write_table(
        table,
        buffer,
        compression='snappy',       # Industry standard compression
        row_group_size=10000,
    )
    buffer.seek(0)
    return buffer.getvalue()


def upload_parquet(s3_client, df: pd.DataFrame, s3_key: str):
    """Upload DataFrame as Parquet to S3."""
    parquet_bytes = df_to_parquet_buffer(df)
    size_kb = round(len(parquet_bytes) / 1024, 2)

    s3_client.put_object(
        Bucket=S3_GOLDEN_BUCKET,
        Key=s3_key,
        Body=parquet_bytes,
        ContentType='application/octet-stream',
        Metadata={
            'record-count': str(len(df)),
            'exported-at':  datetime.now().isoformat(),
            'format':       'parquet',
            'compression':  'snappy',
        }
    )
    logger.success(f"  Uploaded → s3://{S3_GOLDEN_BUCKET}/{s3_key} ({size_kb} KB, {len(df)} records)")


# ============================================================
# VERIFY S3 EXPORTS
# ============================================================
def verify_exports(s3_client):
    """List all exported files in the golden zone."""
    response = s3_client.list_objects_v2(
        Bucket=S3_GOLDEN_BUCKET,
        Prefix='golden_records/'
    )

    if 'Contents' not in response:
        logger.warning("  No files found in golden zone!")
        return

    logger.info("  Files in s3://party-golden-zone/golden_records/:")
    total_size = 0
    for obj in response['Contents']:
        size_kb = round(obj['Size'] / 1024, 2)
        total_size += obj['Size']
        logger.info(f"    {obj['Key']:<70} {size_kb} KB")

    logger.info(f"  Total size: {round(total_size/1024, 2)} KB")


# ============================================================
# MAIN
# ============================================================
def run_export():
    logger.info("=" * 60)
    logger.info("  Party Master Data Hub — Golden Records S3 Export")
    logger.info("=" * 60)

    conn = get_connection()
    s3   = get_s3_client()

    # --- Load golden records ---
    logger.info("\nStep 1: Loading golden records from PostgreSQL...")
    df = load_golden_records(conn)
    conn.close()

    # --- Export full dataset ---
    logger.info("\nStep 2: Exporting full golden records dataset...")
    upload_parquet(
        s3, df,
        s3_key='golden_records/full/golden_records.parquet'
    )

    # --- Export partitioned by party type ---
    logger.info("\nStep 3: Exporting partitioned by party type...")
    for party_type, group_df in df.groupby('party_type'):
        s3_key = f"golden_records/party_type={party_type}/golden_records.parquet"
        upload_parquet(s3, group_df.reset_index(drop=True), s3_key)

    # --- Export only multi-source records (the interesting ones) ---
    logger.info("\nStep 4: Exporting multi-source golden records...")
    multi_source = df[df['source_record_count'] > 1].reset_index(drop=True)
    if not multi_source.empty:
        upload_parquet(
            s3, multi_source,
            s3_key='golden_records/multi_source/golden_records.parquet'
        )
        logger.info(f"  {len(multi_source)} records came from multiple sources (duplicates resolved)")

    # --- Verify ---
    logger.info("\nStep 5: Verifying exports...")
    verify_exports(s3)

    # --- Summary ---
    logger.info("\n" + "=" * 60)
    logger.success("Golden records exported to S3 successfully!")
    logger.info(f"  Total golden records:        {len(df)}")
    logger.info(f"  Multi-source records:        {len(multi_source)}")
    logger.info(f"  Party types exported:        {df['party_type'].nunique()}")
    logger.info(f"  S3 bucket:                   {S3_GOLDEN_BUCKET}")
    logger.info("=" * 60)
    logger.info("Next: Run src/api/main.py to start the FastAPI server")


if __name__ == "__main__":
    run_export()
