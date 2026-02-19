"""
master_parties.py
-----------------
Data Mastering Pipeline:
  1. Load all source_parties records from PostgreSQL
  2. Identity Resolution — find duplicate records across sources
     using Tax ID exact match and fuzzy name+address matching
  3. Survivorship Rules — determine which source wins each field
  4. Golden Record Creation — write master records to golden_parties
  5. Party Clusters — link source records to their golden record
  6. Party Lineage — log every golden record creation/update

This is the core of what LPL Financial's Reference Data Master team does.

Usage:
    python src/mastering/master_parties.py
"""

import sys
import os
import uuid
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(dotenv_path=BASE_DIR / 'config' / '.env')

import psycopg2
import psycopg2.extras
import pandas as pd
import recordlinkage
from recordlinkage.preprocessing import clean, phonetic
from loguru import logger


# ============================================================
# CONFIG
# ============================================================
DB_URL               = os.getenv('DATABASE_URL')
MATCH_THRESHOLD      = float(os.getenv('DEDUP_MATCH_THRESHOLD', '0.85'))
FUZZY_NAME_THRESHOLD = float(os.getenv('FUZZY_NAME_THRESHOLD', '0.80'))

# Survivorship priority — higher number = higher priority
SOURCE_PRIORITY = {
    'markit_feed':  3,   # Most authoritative for legal entities
    'dnb_feed':     2,   # Most authoritative for companies
    'internal_crm': 1,   # Most authoritative for individuals
}

# Which source wins for each field
FIELD_SURVIVORSHIP = {
    'full_name':     ['markit_feed', 'dnb_feed', 'internal_crm'],
    'lei':           ['markit_feed'],
    'tax_id':        ['internal_crm', 'dnb_feed', 'markit_feed'],
    'address_line1': ['dnb_feed', 'markit_feed', 'internal_crm'],
    'city':          ['dnb_feed', 'markit_feed', 'internal_crm'],
    'state':         ['dnb_feed', 'markit_feed', 'internal_crm'],
    'postal_code':   ['dnb_feed', 'markit_feed', 'internal_crm'],
    'email':         ['internal_crm'],
    'phone':         ['internal_crm', 'dnb_feed'],
    'entity_status': ['markit_feed', 'dnb_feed', 'internal_crm'],
}


# ============================================================
# DATABASE HELPERS
# ============================================================
def get_connection():
    return psycopg2.connect(DB_URL)


def load_source_parties(conn) -> pd.DataFrame:
    """Load all source party records into a DataFrame."""
    query = """
        SELECT id, source_system, source_party_id, party_type,
               full_name, first_name, last_name, date_of_birth,
               tax_id, lei, address_line1, address_line2,
               city, state, postal_code, country,
               email, phone, entity_status
        FROM party_master.source_parties
        ORDER BY source_system, id
    """
    df = pd.read_sql(query, conn)
    logger.info(f"  Loaded {len(df)} source party records")
    return df


# ============================================================
# STEP 1 — EXACT MATCHING (Tax ID and LEI)
# ============================================================
def find_exact_matches(df: pd.DataFrame) -> list:
    """
    Find duplicate records using exact Tax ID and LEI matching.
    This is the strongest signal — same Tax ID = same real-world entity.
    Returns list of (id1, id2, score, match_type) tuples.
    """
    matches = []

    # --- Tax ID exact match ---
    tax_groups = df[df['tax_id'].notna() & (df['tax_id'] != '')].groupby('tax_id')
    tax_match_count = 0
    for tax_id, group in tax_groups:
        if len(group) > 1:
            ids = group['id'].tolist()
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    matches.append((ids[i], ids[j], 1.0, 'TAX_ID_EXACT'))
                    tax_match_count += 1

    logger.info(f"  Exact Tax ID matches found: {tax_match_count}")

    # --- LEI exact match ---
    lei_groups = df[df['lei'].notna() & (df['lei'] != '')].groupby('lei')
    lei_match_count = 0
    for lei, group in lei_groups:
        if len(group) > 1:
            ids = group['id'].tolist()
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    pair = (ids[i], ids[j], 1.0, 'LEI_EXACT')
                    if pair not in matches:
                        matches.append(pair)
                        lei_match_count += 1

    logger.info(f"  Exact LEI matches found: {lei_match_count}")
    return matches


# ============================================================
# STEP 2 — FUZZY MATCHING (Name + Address)
# ============================================================
def find_fuzzy_matches(df: pd.DataFrame, exact_match_ids: set) -> list:
    """
    Find duplicate records using fuzzy name and address matching.
    Uses the recordlinkage library to compare string similarity.
    Only runs on records NOT already matched by exact matching.
    """
    matches = []

    # Only fuzzy match records from different source systems
    # (no point matching CRM against CRM)
    df_clean = df.copy()
    df_clean['full_name_clean'] = df_clean['full_name'].fillna('').str.lower().str.strip()
    df_clean['address_clean']   = df_clean['address_line1'].fillna('').str.lower().str.strip()
    df_clean['city_clean']      = df_clean['city'].fillna('').str.lower().str.strip()

    # Split by source for cross-source comparison
    crm_df     = df_clean[df_clean['source_system'] == 'internal_crm'].set_index('id')
    dnb_df     = df_clean[df_clean['source_system'] == 'dnb_feed'].set_index('id')
    markit_df  = df_clean[df_clean['source_system'] == 'markit_feed'].set_index('id')

    source_pairs = [
        (crm_df, dnb_df, 'CRM vs DNB'),
        (crm_df, markit_df, 'CRM vs Markit'),
        (dnb_df, markit_df, 'DNB vs Markit'),
    ]

    for df_left, df_right, label in source_pairs:
        if df_left.empty or df_right.empty:
            continue

        try:
            # Build candidate pairs using blocking on state
            # (only compare records in the same state — performance optimization)
            indexer = recordlinkage.Index()
            indexer.block('state')
            candidate_pairs = indexer.index(df_left, df_right)

            if len(candidate_pairs) == 0:
                continue

            # Compare fields
            compare = recordlinkage.Compare()
            compare.string('full_name_clean', 'full_name_clean',
                          method='jarowinkler', threshold=0.0,
                          label='name_score')
            compare.string('address_clean', 'address_clean',
                          method='jarowinkler', threshold=0.0,
                          label='address_score')
            compare.exact('city_clean', 'city_clean', label='city_match')
            compare.exact('postal_code', 'postal_code', label='zip_match')

            features = compare.compute(candidate_pairs, df_left, df_right)

            # Calculate weighted total score
            features['total_score'] = (
                features['name_score']    * 0.50 +
                features['address_score'] * 0.25 +
                features['city_match']    * 0.15 +
                features['zip_match']     * 0.10
            )

            # Filter to matches above threshold
            strong_matches = features[features['total_score'] >= FUZZY_NAME_THRESHOLD]

            fuzzy_count = 0
            for (id1, id2), row in strong_matches.iterrows():
                # Skip if already matched by exact matching
                if (id1, id2) not in exact_match_ids:
                    matches.append((id1, id2, round(row['total_score'], 4), 'FUZZY_NAME_ADDRESS'))
                    fuzzy_count += 1

            logger.info(f"  Fuzzy matches ({label}): {fuzzy_count}")

        except Exception as e:
            logger.warning(f"  Fuzzy matching error for {label}: {e}")
            continue

    return matches


# ============================================================
# STEP 3 — BUILD CLUSTERS
# Group matched records into clusters (each cluster = one real entity)
# ============================================================
def build_clusters(df: pd.DataFrame, all_matches: list) -> dict:
    """
    Group matched record IDs into clusters using union-find algorithm.
    Each cluster represents one real-world party entity.
    Returns dict: {cluster_id: [list of source party ids]}
    """
    # Union-Find implementation
    parent = {row_id: row_id for row_id in df['id'].tolist()}

    def find(x):
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    # Union all matched pairs
    for id1, id2, score, match_type in all_matches:
        if id1 in parent and id2 in parent:
            union(id1, id2)

    # Group by cluster root
    clusters = {}
    for row_id in df['id'].tolist():
        root = find(row_id)
        if root not in clusters:
            clusters[root] = []
        clusters[root].append(row_id)

    multi_record = {k: v for k, v in clusters.items() if len(v) > 1}
    single_record = {k: v for k, v in clusters.items() if len(v) == 1}

    logger.info(f"  Clusters with multiple records (duplicates): {len(multi_record)}")
    logger.info(f"  Clusters with single record (unique):        {len(single_record)}")
    logger.info(f"  Total clusters (= future golden records):    {len(clusters)}")

    return clusters


# ============================================================
# STEP 4 — SURVIVORSHIP RULES
# For each cluster, pick the best value for each field
# ============================================================
def apply_survivorship(cluster_records: pd.DataFrame) -> dict:
    """
    Apply survivorship rules to a cluster of records.
    For each field, pick the value from the highest-priority source
    that has a non-null value for that field.
    Returns a single golden record dict.
    """
    golden = {}

    for field, priority_sources in FIELD_SURVIVORSHIP.items():
        golden[field] = None
        for source in priority_sources:
            source_records = cluster_records[
                cluster_records['source_system'] == source
            ]
            if not source_records.empty:
                value = source_records.iloc[0][field]
                if value and str(value).strip() and str(value) != 'nan':
                    golden[field] = str(value).strip()
                    break

        # Fallback — if no priority source has the value, take any non-null
        if not golden[field]:
            for _, row in cluster_records.iterrows():
                value = row.get(field)
                if value and str(value).strip() and str(value) != 'nan':
                    golden[field] = str(value).strip()
                    break

    # Fields not in survivorship rules — take from highest priority source
    other_fields = ['first_name', 'last_name', 'date_of_birth',
                    'address_line2', 'country', 'party_type']

    sorted_records = cluster_records.copy()
    sorted_records['priority'] = sorted_records['source_system'].map(SOURCE_PRIORITY).fillna(0)
    sorted_records = sorted_records.sort_values('priority', ascending=False)

    for field in other_fields:
        golden[field] = None
        for _, row in sorted_records.iterrows():
            value = row.get(field)
            if value and str(value).strip() and str(value) != 'nan':
                golden[field] = str(value).strip()
                break

    return golden


# ============================================================
# STEP 5 — WRITE GOLDEN RECORDS TO POSTGRESQL
# ============================================================
GOLDEN_INSERT_SQL = """
    INSERT INTO party_master.golden_parties (
        golden_id, party_type, full_name, first_name, last_name,
        date_of_birth, tax_id, lei, address_line1, address_line2,
        city, state, postal_code, country, email, phone,
        entity_status, confidence_score, created_at, updated_at
    ) VALUES (
        %(golden_id)s, %(party_type)s, %(full_name)s, %(first_name)s, %(last_name)s,
        %(date_of_birth)s, %(tax_id)s, %(lei)s, %(address_line1)s, %(address_line2)s,
        %(city)s, %(state)s, %(postal_code)s, %(country)s, %(email)s, %(phone)s,
        %(entity_status)s, %(confidence_score)s, NOW(), NOW()
    )
    ON CONFLICT (golden_id) DO UPDATE SET
        full_name      = EXCLUDED.full_name,
        tax_id         = EXCLUDED.tax_id,
        entity_status  = EXCLUDED.entity_status,
        updated_at     = NOW();
"""

CLUSTER_INSERT_SQL = """
    INSERT INTO party_master.party_clusters
        (golden_id, source_party_id, is_survivor, match_score, linked_at)
    VALUES
        (%(golden_id)s, %(source_party_id)s, %(is_survivor)s, %(match_score)s, NOW())
    ON CONFLICT DO NOTHING;
"""

LINEAGE_INSERT_SQL = """
    INSERT INTO party_master.party_lineage
        (golden_id, event_type, changed_fields, triggered_by, event_at)
    VALUES
        (%(golden_id)s, %(event_type)s, %(changed_fields)s, %(triggered_by)s, NOW());
"""


def write_golden_records(conn, clusters: dict, df: pd.DataFrame,
                          match_scores: dict) -> int:
    """
    For each cluster:
    1. Apply survivorship to get golden field values
    2. Insert into golden_parties
    3. Link source records in party_clusters
    4. Log creation in party_lineage
    """
    golden_count = 0

    with conn.cursor() as cur:
        for cluster_root, source_ids in clusters.items():
            # Get all records in this cluster
            cluster_df = df[df['id'].isin(source_ids)].copy()

            # Apply survivorship rules
            golden_fields = apply_survivorship(cluster_df)

            # Calculate confidence score
            # Single source = 1.0, multiple sources with exact match = 0.99
            # Multiple sources with fuzzy match = match score
            if len(source_ids) == 1:
                confidence = 1.0
            elif any(match_scores.get((min(a,b), max(a,b)), {}).get('type') == 'TAX_ID_EXACT'
                    for a in source_ids for b in source_ids if a != b):
                confidence = 0.99
            else:
                scores = [match_scores.get((min(a,b), max(a,b)), {}).get('score', 0.85)
                         for a in source_ids for b in source_ids if a != b]
                confidence = round(sum(scores) / len(scores), 4) if scores else 0.85

            # Build golden record
            golden_id = str(uuid.uuid4())
            golden_record = {
                'golden_id':        golden_id,
                'party_type':       golden_fields.get('party_type', 'UNKNOWN'),
                'full_name':        golden_fields.get('full_name'),
                'first_name':       golden_fields.get('first_name'),
                'last_name':        golden_fields.get('last_name'),
                'date_of_birth':    golden_fields.get('date_of_birth'),
                'tax_id':           golden_fields.get('tax_id'),
                'lei':              golden_fields.get('lei'),
                'address_line1':    golden_fields.get('address_line1'),
                'address_line2':    golden_fields.get('address_line2'),
                'city':             golden_fields.get('city'),
                'state':            golden_fields.get('state'),
                'postal_code':      golden_fields.get('postal_code'),
                'country':          golden_fields.get('country', 'USA'),
                'email':            golden_fields.get('email'),
                'phone':            golden_fields.get('phone'),
                'entity_status':    golden_fields.get('entity_status', 'ACTIVE'),
                'confidence_score': confidence,
            }

            # Insert golden record
            cur.execute(GOLDEN_INSERT_SQL, golden_record)

            # Determine survivor (highest priority source in cluster)
            survivor_source = max(
                cluster_df['source_system'].unique(),
                key=lambda s: SOURCE_PRIORITY.get(s, 0)
            )

            # Link source records to golden record
            for _, source_row in cluster_df.iterrows():
                score_key = None
                for a in source_ids:
                    for b in source_ids:
                        if a != b:
                            key = (min(a,b), max(a,b))
                            if key in match_scores:
                                score_key = key
                                break

                cluster_record = {
                    'golden_id':       golden_id,
                    'source_party_id': source_row['id'],
                    'is_survivor':     source_row['source_system'] == survivor_source,
                    'match_score':     match_scores.get(score_key, {}).get('score', 1.0)
                                       if score_key else 1.0,
                }
                cur.execute(CLUSTER_INSERT_SQL, cluster_record)

            # Log lineage
            lineage_record = {
                'golden_id':     golden_id,
                'event_type':    'CREATED',
                'changed_fields': psycopg2.extras.Json({
                    'source_count':   len(source_ids),
                    'sources':        cluster_df['source_system'].tolist(),
                    'confidence':     confidence,
                    'match_type':     'EXACT' if confidence >= 0.99 else 'FUZZY',
                }),
                'triggered_by':  'master_parties.py',
            }
            cur.execute(LINEAGE_INSERT_SQL, lineage_record)

            golden_count += 1

    conn.commit()
    return golden_count


# ============================================================
# VERIFY RESULTS
# ============================================================
def verify_results(conn):
    """Print summary of mastering results."""
    with conn.cursor() as cur:
        # Golden record counts
        cur.execute("""
            SELECT party_type, COUNT(*) as count,
                   ROUND(AVG(confidence_score)::numeric, 3) as avg_confidence
            FROM party_master.golden_parties
            GROUP BY party_type ORDER BY party_type
        """)
        golden_rows = cur.fetchall()

        # Cluster stats
        cur.execute("""
            SELECT COUNT(DISTINCT golden_id) as golden_count,
                   COUNT(*) as total_links,
                   SUM(CASE WHEN is_survivor THEN 1 ELSE 0 END) as survivors
            FROM party_master.party_clusters
        """)
        cluster_stats = cur.fetchone()

        # Lineage count
        cur.execute("SELECT COUNT(*) FROM party_master.party_lineage")
        lineage_count = cur.fetchone()[0]

    logger.info("  Golden Records by party type:")
    for ptype, count, avg_conf in golden_rows:
        logger.info(f"    {ptype:<20} {count:>5} records  (avg confidence: {avg_conf})")

    logger.info(f"  Party clusters: {cluster_stats[0]} golden records linked to {cluster_stats[1]} source records")
    logger.info(f"  Lineage events: {lineage_count} events logged")


# ============================================================
# MAIN
# ============================================================
def run_mastering():
    logger.info("=" * 60)
    logger.info("  Party Master Data Hub — Data Mastering Pipeline")
    logger.info("=" * 60)

    conn = get_connection()

    # --- Load source records ---
    logger.info("\nStep 1: Loading source party records...")
    df = load_source_parties(conn)

    # --- Exact matching ---
    logger.info("\nStep 2: Running exact matching (Tax ID + LEI)...")
    exact_matches = find_exact_matches(df)
    exact_match_ids = {(min(a,b), max(a,b)) for a, b, _, _ in exact_matches}

    # --- Fuzzy matching ---
    logger.info("\nStep 3: Running fuzzy matching (Name + Address)...")
    fuzzy_matches = find_fuzzy_matches(df, exact_match_ids)

    all_matches = exact_matches + fuzzy_matches
    logger.info(f"\n  Total matches found: {len(all_matches)}")

    # Build match scores lookup
    match_scores = {}
    for id1, id2, score, mtype in all_matches:
        key = (min(id1, id2), max(id1, id2))
        match_scores[key] = {'score': score, 'type': mtype}

    # --- Build clusters ---
    logger.info("\nStep 4: Building entity clusters...")
    clusters = build_clusters(df, all_matches)

    # --- Write golden records ---
    logger.info("\nStep 5: Applying survivorship rules and creating golden records...")
    golden_count = write_golden_records(conn, clusters, df, match_scores)
    logger.success(f"  Created {golden_count} golden records")

    # --- Verify ---
    logger.info("\nStep 6: Verifying results...")
    verify_results(conn)

    conn.close()

    logger.info("\n" + "=" * 60)
    logger.success("Data mastering complete!")
    logger.info("Next: Run src/quality/run_quality_checks.py")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_mastering()
