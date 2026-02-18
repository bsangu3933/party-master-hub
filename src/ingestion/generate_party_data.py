"""
generate_party_data.py
----------------------
Generates realistic fake party data simulating 3 source systems:
  1. internal_crm   — individual clients/advisors
  2. dnb_feed       — companies/organizations (Dun & Bradstreet style)
  3. markit_feed    — legal entities (S&P Global Markit style)

Intentional duplicates are created across sources to simulate
real-world data mastering challenges.

Usage:
    python src/ingestion/generate_party_data.py

Output:
    - data/raw/internal_crm/individuals.csv
    - data/raw/dnb_feed/organizations.csv
    - data/raw/markit_feed/legal_entities.csv
    - Summary printed to console
"""

import sys
import os
import csv
import random
import json
from pathlib import Path
from datetime import datetime

# Fix path so we can find config/.env
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(dotenv_path=BASE_DIR / 'config' / '.env')

from faker import Faker
from loguru import logger

fake = Faker('en_US')
Faker.seed(42)          # Fixed seed = reproducible data every run
random.seed(42)

# ============================================================
# CONFIG
# ============================================================
NUM_INDIVIDUALS     = 200   # Records in internal CRM
NUM_ORGANIZATIONS   = 150   # Records in D&B feed
NUM_LEGAL_ENTITIES  = 100   # Records in Markit feed
DUPLICATE_RATE      = 0.20  # 20% of records will be duplicates across sources

OUTPUT_DIR = BASE_DIR / 'data' / 'raw'

# ============================================================
# HELPERS
# ============================================================

ENTITY_SUFFIXES = ['LLC', 'Inc.', 'Corp.', 'L.L.C.', 'Corporation',
                   'Limited', 'Ltd.', 'LP', 'LLP', 'Holdings']

STREET_SUFFIXES_VARIANTS = {
    'Street': ['Street', 'St', 'St.'],
    'Avenue': ['Avenue', 'Ave', 'Ave.'],
    'Boulevard': ['Boulevard', 'Blvd', 'Blvd.'],
    'Drive':   ['Drive', 'Dr', 'Dr.'],
    'Road':    ['Road', 'Rd', 'Rd.'],
    'Lane':    ['Lane', 'Ln', 'Ln.'],
}

US_STATES = [
    'NY', 'CA', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI',
    'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI'
]

KYC_STATUSES = ['VERIFIED', 'PENDING', 'FAILED', 'EXPIRED']
ENTITY_TYPES  = ['LLC', 'CORPORATION', 'PARTNERSHIP', 'TRUST', 'FUND']
LEI_CHARS     = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'


def generate_tax_id(party_type='individual'):
    """Generate realistic SSN or EIN."""
    if party_type == 'individual':
        # SSN format: XXX-XX-XXXX
        return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"
    else:
        # EIN format: XX-XXXXXXX
        return f"{random.randint(10,99)}-{random.randint(1000000,9999999)}"


def generate_lei():
    """Generate a fake 20-character LEI (Legal Entity Identifier)."""
    return ''.join(random.choices(LEI_CHARS, k=20))


def vary_address(address: str) -> str:
    """Introduce realistic address variations."""
    for full, variants in STREET_SUFFIXES_VARIANTS.items():
        if full in address:
            address = address.replace(full, random.choice(variants))
            break
    return address


def vary_name(name: str) -> str:
    """Introduce realistic name variations (typos, abbreviations)."""
    variations = [
        name,
        name.upper(),
        name.lower().title(),
        name.replace('.', ''),
        name.replace('-', ' '),
    ]
    return random.choice(variations)


def vary_company_name(name: str) -> str:
    """Introduce company name variations across sources."""
    # Sometimes drop the suffix, sometimes use a different one
    for suffix in ENTITY_SUFFIXES:
        if name.endswith(suffix):
            base = name[:-len(suffix)].strip()
            return f"{base} {random.choice(ENTITY_SUFFIXES)}"
    return name


# ============================================================
# SOURCE 1 — INTERNAL CRM (Individuals)
# ============================================================

def generate_crm_individuals(num_records: int) -> list:
    """
    Simulates individual party records from an internal CRM system.
    Fields reflect what a wealth management CRM would store.
    """
    records = []
    shared_pool = []  # Pool of records to duplicate into other sources

    for i in range(num_records):
        first_name = fake.first_name()
        last_name  = fake.last_name()
        state      = random.choice(US_STATES)
        dob        = fake.date_of_birth(minimum_age=25, maximum_age=85)

        record = {
            'source_system':    'internal_crm',
            'source_party_id':  f"CRM-{10000 + i}",
            'party_type':       'INDIVIDUAL',
            'first_name':       first_name,
            'last_name':        last_name,
            'full_name':        f"{first_name} {last_name}",
            'date_of_birth':    dob.strftime('%Y-%m-%d'),
            'tax_id':           generate_tax_id('individual'),
            'address_line1':    fake.building_number() + ' ' + fake.street_name(),
            'address_line2':    random.choice(['', '', '', f"Apt {random.randint(1,999)}"]),
            'city':             fake.city(),
            'state':            state,
            'postal_code':      fake.zipcode(),
            'country':          'USA',
            'email':            fake.email(),
            'phone':            fake.phone_number(),
            'entity_status':    random.choice(['ACTIVE', 'ACTIVE', 'ACTIVE', 'INACTIVE']),
            'kyc_status':       random.choice(KYC_STATUSES),
            'advisor_id':       f"ADV-{random.randint(1000,9999)}",
            'account_type':     random.choice(['BROKERAGE', 'IRA', 'ROTH_IRA', 'ADVISORY']),
            'ingested_at':      datetime.now().isoformat(),
        }

        records.append(record)

        # Add some to shared pool for cross-source duplication
        if random.random() < DUPLICATE_RATE:
            shared_pool.append(record)

    logger.info(f"  Generated {len(records)} CRM individuals ({len(shared_pool)} flagged for duplication)")
    return records, shared_pool


# ============================================================
# SOURCE 2 — D&B FEED (Organizations)
# ============================================================

def generate_dnb_organizations(num_records: int, shared_pool: list) -> list:
    """
    Simulates organization records from a Dun & Bradstreet style feed.
    D&B focuses on companies with DUNS numbers, employee counts, etc.
    Some records are intentional duplicates of CRM records with variations.
    """
    records = []

    # First add genuine new organizations
    for i in range(num_records):
        company_name = fake.company() + ' ' + random.choice(ENTITY_SUFFIXES)
        state        = random.choice(US_STATES)

        record = {
            'source_system':    'dnb_feed',
            'source_party_id':  f"DNB-{20000 + i}",
            'party_type':       'ORGANIZATION',
            'full_name':        company_name,
            'tax_id':           generate_tax_id('organization'),
            'duns_number':      f"{random.randint(100000000, 999999999)}",  # D&B specific
            'address_line1':    fake.building_number() + ' ' + fake.street_name(),
            'address_line2':    random.choice(['', '', f"Suite {random.randint(100,999)}"]),
            'city':             fake.city(),
            'state':            state,
            'postal_code':      fake.zipcode(),
            'country':          'USA',
            'phone':            fake.phone_number(),
            'employee_count':   random.randint(1, 50000),
            'annual_revenue':   random.randint(100000, 1000000000),
            'industry_code':    f"SIC-{random.randint(1000,9999)}",
            'entity_status':    random.choice(['ACTIVE', 'ACTIVE', 'INACTIVE', 'MERGED']),
            'entity_type':      random.choice(ENTITY_TYPES),
            'ingested_at':      datetime.now().isoformat(),
        }
        records.append(record)

    # Now add duplicate individuals from CRM as organizations (with variations)
    # This simulates when an advisor is also registered as a business entity
    dup_count = 0
    for crm_record in shared_pool[:20]:  # Take first 20 from shared pool
        record = {
            'source_system':    'dnb_feed',
            'source_party_id':  f"DNB-DUP-{dup_count + 1}",
            'party_type':       'ORGANIZATION',
            'full_name':        vary_name(crm_record['full_name']) + ' Financial Services LLC',
            'tax_id':           crm_record['tax_id'],    # Same tax ID — key for matching!
            'duns_number':      f"{random.randint(100000000, 999999999)}",
            'address_line1':    vary_address(crm_record['address_line1']),
            'address_line2':    crm_record['address_line2'],
            'city':             crm_record['city'],
            'state':            crm_record['state'],
            'postal_code':      crm_record['postal_code'],
            'country':          'USA',
            'phone':            crm_record['phone'],
            'employee_count':   random.randint(1, 10),
            'annual_revenue':   random.randint(50000, 500000),
            'industry_code':    'SIC-6282',  # Investment Advice
            'entity_status':    'ACTIVE',
            'entity_type':      'LLC',
            'ingested_at':      datetime.now().isoformat(),
        }
        records.append(record)
        dup_count += 1

    logger.info(f"  Generated {len(records)} D&B organizations ({dup_count} intentional duplicates)")
    return records


# ============================================================
# SOURCE 3 — MARKIT FEED (Legal Entities)
# ============================================================

def generate_markit_legal_entities(num_records: int, shared_pool: list) -> list:
    """
    Simulates legal entity records from an S&P Global Markit style feed.
    Markit focuses on LEIs, regulatory classifications, and entity hierarchies.
    Some records are intentional duplicates with name/address variations.
    """
    records = []

    # Genuine new legal entities
    for i in range(num_records):
        company_name = fake.company() + ' ' + random.choice(ENTITY_SUFFIXES)
        state        = random.choice(US_STATES)

        record = {
            'source_system':    'markit_feed',
            'source_party_id':  f"MKT-{30000 + i}",
            'party_type':       'LEGAL_ENTITY',
            'full_name':        company_name,
            'lei':              generate_lei(),           # Markit specific — LEI
            'tax_id':           generate_tax_id('organization'),
            'address_line1':    fake.building_number() + ' ' + fake.street_name(),
            'address_line2':    random.choice(['', f"Floor {random.randint(1,50)}"]),
            'city':             fake.city(),
            'state':            state,
            'postal_code':      fake.zipcode(),
            'country':          'USA',
            'entity_type':      random.choice(ENTITY_TYPES),
            'entity_status':    random.choice(['ACTIVE', 'ACTIVE', 'INACTIVE']),
            'regulatory_status': random.choice(['REGISTERED', 'EXEMPT', 'PENDING']),
            'parent_lei':       generate_lei() if random.random() < 0.3 else '',
            'jurisdiction':     random.choice(['US-NY', 'US-DE', 'US-CA', 'US-TX']),
            'ingested_at':      datetime.now().isoformat(),
        }
        records.append(record)

    # Add duplicates from D&B/CRM with name variations
    dup_count = 0
    for crm_record in shared_pool[20:35]:  # Take next 15 from shared pool
        record = {
            'source_system':    'markit_feed',
            'source_party_id':  f"MKT-DUP-{dup_count + 1}",
            'party_type':       'LEGAL_ENTITY',
            'full_name':        vary_name(crm_record['full_name']),
            'lei':              generate_lei(),
            'tax_id':           crm_record['tax_id'],    # Same tax ID — key for matching!
            'address_line1':    vary_address(crm_record['address_line1']),
            'address_line2':    crm_record.get('address_line2', ''),
            'city':             crm_record['city'],
            'state':            crm_record['state'],
            'postal_code':      crm_record['postal_code'],
            'country':          'USA',
            'entity_type':      'LLC',
            'entity_status':    'ACTIVE',
            'regulatory_status': 'REGISTERED',
            'parent_lei':       '',
            'jurisdiction':     f"US-{crm_record['state']}",
            'ingested_at':      datetime.now().isoformat(),
        }
        records.append(record)
        dup_count += 1

    logger.info(f"  Generated {len(records)} Markit legal entities ({dup_count} intentional duplicates)")
    return records


# ============================================================
# WRITE TO CSV
# ============================================================

def write_csv(records: list, filepath: Path, fieldnames: list):
    """Write records to CSV file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(records)
    logger.success(f"  Saved {len(records)} records → {filepath}")


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("=" * 55)
    logger.info("  Party Master Data Hub — Data Generator")
    logger.info("=" * 55)

    # --- Generate CRM Individuals ---
    logger.info("Generating Internal CRM individuals...")
    crm_records, shared_pool = generate_crm_individuals(NUM_INDIVIDUALS)

    crm_fields = [
        'source_system', 'source_party_id', 'party_type',
        'first_name', 'last_name', 'full_name', 'date_of_birth',
        'tax_id', 'address_line1', 'address_line2', 'city', 'state',
        'postal_code', 'country', 'email', 'phone', 'entity_status',
        'kyc_status', 'advisor_id', 'account_type', 'ingested_at'
    ]
    write_csv(crm_records, OUTPUT_DIR / 'internal_crm' / 'individuals.csv', crm_fields)

    # --- Generate D&B Organizations ---
    logger.info("Generating D&B organization feed...")
    dnb_records = generate_dnb_organizations(NUM_ORGANIZATIONS, shared_pool)

    dnb_fields = [
        'source_system', 'source_party_id', 'party_type', 'full_name',
        'tax_id', 'duns_number', 'address_line1', 'address_line2',
        'city', 'state', 'postal_code', 'country', 'phone',
        'employee_count', 'annual_revenue', 'industry_code',
        'entity_status', 'entity_type', 'ingested_at'
    ]
    write_csv(dnb_records, OUTPUT_DIR / 'dnb_feed' / 'organizations.csv', dnb_fields)

    # --- Generate Markit Legal Entities ---
    logger.info("Generating S&P Markit legal entity feed...")
    markit_records = generate_markit_legal_entities(NUM_LEGAL_ENTITIES, shared_pool)

    markit_fields = [
        'source_system', 'source_party_id', 'party_type', 'full_name',
        'lei', 'tax_id', 'address_line1', 'address_line2', 'city',
        'state', 'postal_code', 'country', 'entity_type', 'entity_status',
        'regulatory_status', 'parent_lei', 'jurisdiction', 'ingested_at'
    ]
    write_csv(markit_records, OUTPUT_DIR / 'markit_feed' / 'legal_entities.csv', markit_fields)

    # --- Summary ---
    total = len(crm_records) + len(dnb_records) + len(markit_records)
    logger.info("=" * 55)
    logger.success(f"Data generation complete!")
    logger.info(f"  CRM individuals:      {len(crm_records)} records")
    logger.info(f"  D&B organizations:    {len(dnb_records)} records")
    logger.info(f"  Markit legal entities:{len(markit_records)} records")
    logger.info(f"  Total records:        {total}")
    logger.info(f"  Intentional duplicates seeded for mastering tests")
    logger.info("=" * 55)
    logger.info("Next: Run src/ingestion/ingest_to_postgres.py")


if __name__ == "__main__":
    main()
