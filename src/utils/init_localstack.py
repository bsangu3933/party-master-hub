"""
init_localstack.py
------------------
Creates S3 buckets on LocalStack to simulate the AWS data lake zones.
Run once after docker-compose up.

Usage:
    python src/utils/init_localstack.py
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from dotenv import load_dotenv
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=BASE_DIR / 'config' / '.env')

import boto3
from botocore.exceptions import ClientError
from loguru import logger


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=os.getenv('AWS_ENDPOINT_URL', 'http://localhost:4566'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )


def create_bucket(s3_client, bucket_name: str):
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        logger.success(f"  Created S3 bucket: {bucket_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            logger.info(f"  Bucket already exists: {bucket_name}")
        else:
            raise


def create_folder_structure(s3_client, bucket_name: str, prefixes: list):
    """Create logical 'folder' structure inside S3 bucket."""
    for prefix in prefixes:
        s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/")
        logger.info(f"    Created folder: s3://{bucket_name}/{prefix}/")


def init_localstack():
    logger.info("Connecting to LocalStack S3...")

    s3 = get_s3_client()

    buckets = {
        os.getenv('S3_RAW_BUCKET', 'party-raw-zone'): [
            'internal_crm',
            'dnb_feed',
            'markit_feed',
            'kyc_vendor'
        ],
        os.getenv('S3_CURATED_BUCKET', 'party-curated-zone'): [
            'individuals',
            'organizations',
            'legal_entities'
        ],
        os.getenv('S3_GOLDEN_BUCKET', 'party-golden-zone'): [
            'golden_records',
            'rejected',
            'quarantine'
        ]
    }

    logger.info("Creating S3 buckets and folder structure...")
    for bucket_name, prefixes in buckets.items():
        create_bucket(s3, bucket_name)
        create_folder_structure(s3, bucket_name, prefixes)

    # Verify
    response = s3.list_buckets()
    created = [b['Name'] for b in response['Buckets']]
    logger.success(f"\nLocalStack S3 initialized. Buckets available: {created}")


if __name__ == "__main__":
    init_localstack()
