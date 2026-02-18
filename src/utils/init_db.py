"""
init_db.py
----------
Initializes the PostgreSQL database schema for the Party Master Data Hub.
Run once after docker-compose up to create all tables.

Usage:
    python src/utils/init_db.py
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from dotenv import load_dotenv
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=BASE_DIR / 'config' / '.env')


import psycopg2
from loguru import logger

DATABASE_URL = os.getenv("DATABASE_URL")


DDL = """
-- ============================================================
-- PARTY MASTER SCHEMA
-- Mirrors what a financial firm like LPL would have in RDS
-- ============================================================

CREATE SCHEMA IF NOT EXISTS party_master;

-- -------------------------------------------------------
-- SOURCE PARTIES
-- Raw ingested records from each source system
-- Each source (CSV, API, Kafka) lands records here first
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_master.source_parties (
    id                  SERIAL PRIMARY KEY,
    source_system       VARCHAR(50) NOT NULL,       -- e.g. 'dnb', 'markit', 'internal_crm'
    source_party_id     VARCHAR(100) NOT NULL,       -- ID in the source system
    party_type          VARCHAR(30),                 -- 'INDIVIDUAL' or 'ORGANIZATION'
    full_name           VARCHAR(255),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    date_of_birth       DATE,
    tax_id              VARCHAR(20),                 -- SSN or EIN
    lei                 VARCHAR(20),                 -- Legal Entity Identifier (companies)
    address_line1       VARCHAR(255),
    address_line2       VARCHAR(100),
    city                VARCHAR(100),
    state               VARCHAR(50),
    postal_code         VARCHAR(20),
    country             VARCHAR(3),                  -- ISO 3-letter country code
    email               VARCHAR(150),
    phone               VARCHAR(30),
    entity_status       VARCHAR(20),                 -- 'ACTIVE', 'INACTIVE', 'MERGED'
    raw_payload         JSONB,                       -- Full original record stored as JSON
    ingested_at         TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (source_system, source_party_id)
);

-- -------------------------------------------------------
-- GOLDEN PARTIES
-- The master/golden record — one row per unique real-world party
-- This is the output of deduplication and mastering
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_master.golden_parties (
    golden_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    party_type          VARCHAR(30),
    full_name           VARCHAR(255),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    date_of_birth       DATE,
    tax_id              VARCHAR(20),
    lei                 VARCHAR(20),
    address_line1       VARCHAR(255),
    address_line2       VARCHAR(100),
    city                VARCHAR(100),
    state               VARCHAR(50),
    postal_code         VARCHAR(20),
    country             VARCHAR(3),
    email               VARCHAR(150),
    phone               VARCHAR(30),
    entity_status       VARCHAR(20) DEFAULT 'ACTIVE',
    confidence_score    DECIMAL(5,4),                -- How confident the mastering was (0-1)
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- -------------------------------------------------------
-- PARTY CLUSTERS
-- Links source records to their golden record
-- One golden record can have many source contributors
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_master.party_clusters (
    id                  SERIAL PRIMARY KEY,
    golden_id           UUID REFERENCES party_master.golden_parties(golden_id),
    source_party_id     INTEGER REFERENCES party_master.source_parties(id),
    is_survivor         BOOLEAN DEFAULT FALSE,       -- TRUE = this source won survivorship
    match_score         DECIMAL(5,4),                -- Match confidence for this link
    linked_at           TIMESTAMP DEFAULT NOW()
);

-- -------------------------------------------------------
-- PARTY LINEAGE
-- Audit trail: every time a golden record changes, log it
-- Supports the "data lineage" requirement from the JD
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_master.party_lineage (
    id                  SERIAL PRIMARY KEY,
    golden_id           UUID REFERENCES party_master.golden_parties(golden_id),
    event_type          VARCHAR(50),                 -- 'CREATED', 'UPDATED', 'MERGED', 'SPLIT'
    changed_fields      JSONB,                       -- Which fields changed and old/new values
    triggered_by        VARCHAR(100),                -- Which pipeline/source triggered this
    event_at            TIMESTAMP DEFAULT NOW()
);

-- -------------------------------------------------------
-- DATA QUALITY RESULTS
-- Stores output of Great Expectations validation runs
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_master.dq_results (
    id                  SERIAL PRIMARY KEY,
    run_id              VARCHAR(100),
    source_system       VARCHAR(50),
    expectation_suite   VARCHAR(100),
    total_records       INTEGER,
    passed_records      INTEGER,
    failed_records      INTEGER,
    pass_rate           DECIMAL(5,4),
    details             JSONB,
    run_at              TIMESTAMP DEFAULT NOW()
);

-- -------------------------------------------------------
-- INDEXES for performance
-- -------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_source_parties_tax_id ON party_master.source_parties(tax_id);
CREATE INDEX IF NOT EXISTS idx_source_parties_lei ON party_master.source_parties(lei);
CREATE INDEX IF NOT EXISTS idx_source_parties_name ON party_master.source_parties(full_name);
CREATE INDEX IF NOT EXISTS idx_golden_parties_tax_id ON party_master.golden_parties(tax_id);
CREATE INDEX IF NOT EXISTS idx_golden_parties_lei ON party_master.golden_parties(lei);
CREATE INDEX IF NOT EXISTS idx_golden_parties_name ON party_master.golden_parties(full_name);
CREATE INDEX IF NOT EXISTS idx_clusters_golden_id ON party_master.party_clusters(golden_id);
CREATE INDEX IF NOT EXISTS idx_lineage_golden_id ON party_master.party_lineage(golden_id);
"""


def init_database():
    logger.info("Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        cur = conn.cursor()

        logger.info("Creating schema and tables...")
        cur.execute(DDL)

        logger.success("Database initialized successfully!")
        logger.info("Tables created:")
        logger.info("  - party_master.source_parties")
        logger.info("  - party_master.golden_parties")
        logger.info("  - party_master.party_clusters")
        logger.info("  - party_master.party_lineage")
        logger.info("  - party_master.dq_results")

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    init_database()
