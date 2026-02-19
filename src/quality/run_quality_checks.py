"""
run_quality_checks.py
---------------------
Data Quality Pipeline using Great Expectations.
Validates both source_parties and golden_parties tables.

Checks include:
  - No null Tax IDs on active records
  - Valid party type values
  - Name completeness
  - Address completeness
  - Confidence score ranges
  - No duplicate golden records

Results are saved to party_master.dq_results table.

Usage:
    python src/quality/run_quality_checks.py
"""

import sys
import os
import json
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
from loguru import logger


# ============================================================
# CONFIG
# ============================================================
DB_URL  = os.getenv('DATABASE_URL')


# ============================================================
# DATABASE HELPERS
# ============================================================
def get_connection():
    return psycopg2.connect(DB_URL)


def load_table(conn, table: str) -> pd.DataFrame:
    """Load a full table into a DataFrame."""
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    logger.info(f"  Loaded {len(df)} records from {table}")
    return df


def make_serializable(obj):
    """Recursively convert numpy/pandas types to native Python types for JSON."""
    import numpy as np
    if isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_serializable(i) for i in obj]
    elif isinstance(obj, (np.bool_)):
        return bool(obj)
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    else:
        return obj


def save_dq_result(conn, run_id: str, source_system: str,
                   suite_name: str, results: dict):
    """Save data quality results to dq_results table."""
    total    = results['total_records']
    passed   = results['passed_records']
    failed   = results['failed_records']
    pass_rate = round(passed / total, 4) if total > 0 else 0.0

    sql = """
        INSERT INTO party_master.dq_results
            (run_id, source_system, expectation_suite,
             total_records, passed_records, failed_records,
             pass_rate, details, run_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            run_id, source_system, suite_name,
            total, passed, failed, pass_rate,
            psycopg2.extras.Json(make_serializable(results['details']))
        ))
    conn.commit()


# ============================================================
# EXPECTATION RUNNER
# Simple custom expectations without needing GE context setup
# ============================================================
class ExpectationSuite:
    """
    Lightweight expectation runner that mirrors Great Expectations
    concepts without requiring full GE project setup.
    Produces the same output structure as GE for compatibility.
    """

    def __init__(self, name: str, df: pd.DataFrame):
        self.name    = name
        self.df      = df
        self.results = []
        self.total   = len(df)

    def expect_column_values_to_not_be_null(self, column: str,
                                             filter_expr: str = None,
                                             mostly: float = 1.0):
        """Check that a column has no null values."""
        df = self.df.query(filter_expr) if filter_expr else self.df
        null_count   = df[column].isna().sum() if column in df.columns else 0
        total        = len(df)
        passed       = total - null_count
        pass_rate    = round(passed / total, 4) if total > 0 else 1.0
        success      = pass_rate >= mostly

        self.results.append({
            'expectation':  f'expect_{column}_not_null',
            'column':       column,
            'filter':       filter_expr,
            'total':        total,
            'passed':       int(passed),
            'failed':       int(null_count),
            'pass_rate':    pass_rate,
            'mostly':       mostly,
            'success':      success,
        })
        status = '✅' if success else '❌'
        logger.info(f"  {status} {column} not null: {passed}/{total} ({pass_rate*100:.1f}%)")
        return success

    def expect_column_values_to_be_in_set(self, column: str,
                                           value_set: list,
                                           mostly: float = 1.0):
        """Check that column values are within an allowed set."""
        if column not in self.df.columns:
            return False
        valid        = self.df[column].isin(value_set).sum()
        total        = self.df[column].notna().sum()
        failed       = total - valid
        pass_rate    = round(valid / total, 4) if total > 0 else 1.0
        success      = pass_rate >= mostly

        self.results.append({
            'expectation':  f'expect_{column}_in_set',
            'column':       column,
            'value_set':    value_set,
            'total':        int(total),
            'passed':       int(valid),
            'failed':       int(failed),
            'pass_rate':    pass_rate,
            'mostly':       mostly,
            'success':      success,
        })
        status = '✅' if success else '❌'
        logger.info(f"  {status} {column} in allowed set: {valid}/{total} ({pass_rate*100:.1f}%)")
        return success

    def expect_column_value_lengths_to_be_between(self, column: str,
                                                    min_len: int,
                                                    max_len: int,
                                                    mostly: float = 1.0):
        """Check that string column values have length within range."""
        if column not in self.df.columns:
            return False
        lengths      = self.df[column].dropna().astype(str).str.len()
        valid        = ((lengths >= min_len) & (lengths <= max_len)).sum()
        total        = len(lengths)
        failed       = total - valid
        pass_rate    = round(valid / total, 4) if total > 0 else 1.0
        success      = pass_rate >= mostly

        self.results.append({
            'expectation':  f'expect_{column}_length_{min_len}_to_{max_len}',
            'column':       column,
            'min_len':      min_len,
            'max_len':      max_len,
            'total':        int(total),
            'passed':       int(valid),
            'failed':       int(failed),
            'pass_rate':    pass_rate,
            'mostly':       mostly,
            'success':      success,
        })
        status = '✅' if success else '❌'
        logger.info(f"  {status} {column} length {min_len}-{max_len}: {valid}/{total} ({pass_rate*100:.1f}%)")
        return success

    def expect_column_values_to_be_between(self, column: str,
                                            min_val: float,
                                            max_val: float,
                                            mostly: float = 1.0):
        """Check that numeric column values are within range."""
        if column not in self.df.columns:
            return False
        values       = pd.to_numeric(self.df[column], errors='coerce').dropna()
        valid        = ((values >= min_val) & (values <= max_val)).sum()
        total        = len(values)
        failed       = total - valid
        pass_rate    = round(valid / total, 4) if total > 0 else 1.0
        success      = pass_rate >= mostly

        self.results.append({
            'expectation':  f'expect_{column}_between_{min_val}_{max_val}',
            'column':       column,
            'min_val':      min_val,
            'max_val':      max_val,
            'total':        int(total),
            'passed':       int(valid),
            'failed':       int(failed),
            'pass_rate':    pass_rate,
            'mostly':       mostly,
            'success':      success,
        })
        status = '✅' if success else '❌'
        logger.info(f"  {status} {column} between {min_val}-{max_val}: {valid}/{total} ({pass_rate*100:.1f}%)")
        return success

    def expect_column_values_to_be_unique(self, column: str):
        """Check that column values are unique (no duplicates)."""
        if column not in self.df.columns:
            return False
        total        = self.df[column].notna().sum()
        unique       = self.df[column].nunique()
        duplicates   = total - unique
        success      = duplicates == 0

        self.results.append({
            'expectation':  f'expect_{column}_unique',
            'column':       column,
            'total':        int(total),
            'unique':       int(unique),
            'duplicates':   int(duplicates),
            'success':      success,
        })
        status = '✅' if success else '❌'
        logger.info(f"  {status} {column} unique: {unique}/{total} ({duplicates} duplicates)")
        return success

    def get_summary(self) -> dict:
        """Return summary of all expectation results."""
        total_checks  = len(self.results)
        passed_checks = sum(1 for r in self.results if r['success'])
        failed_checks = total_checks - passed_checks

        return {
            'suite_name':      self.name,
            'total_records':   self.total,
            'passed_records':  sum(r.get('passed', 0) for r in self.results),
            'failed_records':  sum(r.get('failed', 0) for r in self.results),
            'total_checks':    total_checks,
            'passed_checks':   passed_checks,
            'failed_checks':   failed_checks,
            'overall_success': failed_checks == 0,
            'details':         self.results,
        }


# ============================================================
# SUITE 1 — SOURCE PARTIES VALIDATION
# ============================================================
def validate_source_parties(df: pd.DataFrame) -> dict:
    """
    Validate raw source party records.
    These checks run BEFORE mastering to catch bad source data.
    """
    logger.info("\n[Suite 1] Validating source_parties...")
    suite = ExpectationSuite('source_parties_suite', df)

    # Party type must be valid
    suite.expect_column_values_to_be_in_set(
        'party_type',
        ['INDIVIDUAL', 'ORGANIZATION', 'LEGAL_ENTITY'],
        mostly=1.0
    )

    # Full name must not be null
    suite.expect_column_values_to_not_be_null('full_name', mostly=0.99)

    # Tax ID should be present for most records
    suite.expect_column_values_to_not_be_null('tax_id', mostly=0.90)

    # Tax ID length check (SSN = 11 chars with dashes, EIN = 10 chars)
    suite.expect_column_value_lengths_to_be_between(
        'tax_id', min_len=9, max_len=12, mostly=0.95
    )

    # Source system must be valid
    suite.expect_column_values_to_be_in_set(
        'source_system',
        ['internal_crm', 'dnb_feed', 'markit_feed'],
        mostly=1.0
    )

    # Entity status must be valid
    suite.expect_column_values_to_be_in_set(
        'entity_status',
        ['ACTIVE', 'INACTIVE', 'MERGED', 'PENDING'],
        mostly=0.99
    )

    # Address line 1 should be present for most records
    suite.expect_column_values_to_not_be_null('address_line1', mostly=0.95)

    # City should be present
    suite.expect_column_values_to_not_be_null('city', mostly=0.95)

    # State should be present and short (2-char code)
    suite.expect_column_value_lengths_to_be_between(
        'state', min_len=2, max_len=2, mostly=0.95
    )

    # Source party ID must be unique per source system
    df['source_key'] = df['source_system'] + '|' + df['source_party_id']
    suite.expect_column_values_to_be_unique('source_key')

    return suite.get_summary()


# ============================================================
# SUITE 2 — GOLDEN PARTIES VALIDATION
# ============================================================
def validate_golden_parties(df: pd.DataFrame) -> dict:
    """
    Validate golden (master) party records.
    These checks run AFTER mastering to ensure quality of output.
    """
    logger.info("\n[Suite 2] Validating golden_parties...")
    suite = ExpectationSuite('golden_parties_suite', df)

    # Golden ID must be unique — no duplicate master records
    suite.expect_column_values_to_be_unique('golden_id')

    # Full name must not be null
    suite.expect_column_values_to_not_be_null('full_name', mostly=0.99)

    # Party type must be valid
    suite.expect_column_values_to_be_in_set(
        'party_type',
        ['INDIVIDUAL', 'ORGANIZATION', 'LEGAL_ENTITY'],
        mostly=1.0
    )

    # Tax ID should be present for most golden records
    suite.expect_column_values_to_not_be_null('tax_id', mostly=0.90)

    # Confidence score must be between 0 and 1
    suite.expect_column_values_to_be_between(
        'confidence_score', min_val=0.0, max_val=1.0, mostly=1.0
    )

    # Confidence score should be high (above 0.80) for most records
    suite.expect_column_values_to_be_between(
        'confidence_score', min_val=0.80, max_val=1.0, mostly=0.95
    )

    # Entity status must be valid
    suite.expect_column_values_to_be_in_set(
        'entity_status',
        ['ACTIVE', 'INACTIVE', 'MERGED', 'PENDING'],
        mostly=0.99
    )

    # Address should be present for most golden records
    suite.expect_column_values_to_not_be_null('address_line1', mostly=0.90)

    # City should be present
    suite.expect_column_values_to_not_be_null('city', mostly=0.90)

    return suite.get_summary()


# ============================================================
# SUITE 3 — CLUSTERS VALIDATION
# ============================================================
def validate_clusters(conn) -> dict:
    """
    Validate party cluster integrity.
    Every source record should be linked to exactly one golden record.
    """
    logger.info("\n[Suite 3] Validating party_clusters integrity...")

    # Check every source record has a cluster entry
    query = """
        SELECT
            (SELECT COUNT(*) FROM party_master.source_parties) as total_source,
            (SELECT COUNT(DISTINCT source_party_id) FROM party_master.party_clusters) as linked_source,
            (SELECT COUNT(*) FROM party_master.golden_parties) as total_golden,
            (SELECT COUNT(DISTINCT golden_id) FROM party_master.party_clusters) as linked_golden,
            (SELECT COUNT(*) FROM party_master.party_clusters WHERE is_survivor = true) as survivors
    """
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()

    total_source, linked_source, total_golden, linked_golden, survivors = row

    unlinked     = total_source - linked_source
    orphan_golden = total_golden - linked_golden

    results = [
        {
            'expectation': 'all_source_records_linked',
            'total':       int(total_source),
            'linked':      int(linked_source),
            'unlinked':    int(unlinked),
            'success':     unlinked == 0,
        },
        {
            'expectation': 'all_golden_records_have_clusters',
            'total':       int(total_golden),
            'linked':      int(linked_golden),
            'orphans':     int(orphan_golden),
            'success':     orphan_golden == 0,
        },
        {
            'expectation': 'each_cluster_has_one_survivor',
            'survivors':   int(survivors),
            'golden':      int(total_golden),
            'success':     survivors == total_golden,
        },
    ]

    for r in results:
        status = '✅' if r['success'] else '❌'
        logger.info(f"  {status} {r['expectation']}: {r}")

    passed = sum(1 for r in results if r['success'])
    return {
        'suite_name':      'cluster_integrity_suite',
        'total_records':   int(total_source),
        'passed_records':  int(linked_source),
        'failed_records':  int(unlinked),
        'total_checks':    len(results),
        'passed_checks':   passed,
        'failed_checks':   len(results) - passed,
        'overall_success': passed == len(results),
        'details':         results,
    }


# ============================================================
# PRINT FINAL REPORT
# ============================================================
def print_report(all_results: list):
    """Print a clean summary report of all DQ checks."""
    logger.info("\n" + "=" * 60)
    logger.info("  DATA QUALITY REPORT")
    logger.info("=" * 60)

    overall_pass = True
    for result in all_results:
        suite_pass = result['overall_success']
        overall_pass = overall_pass and suite_pass
        status = '✅ PASSED' if suite_pass else '❌ FAILED'

        logger.info(f"\n  {result['suite_name']}")
        logger.info(f"  Status:         {status}")
        logger.info(f"  Total records:  {result['total_records']}")
        logger.info(f"  Checks passed:  {result['passed_checks']}/{result['total_checks']}")
        if result['failed_checks'] > 0:
            failed = [d['expectation'] for d in result['details'] if not d.get('success')]
            logger.warning(f"  Failed checks:  {failed}")

    logger.info("\n" + "=" * 60)
    if overall_pass:
        logger.success("  OVERALL: ALL DATA QUALITY CHECKS PASSED ✅")
    else:
        logger.warning("  OVERALL: SOME DATA QUALITY CHECKS FAILED ❌")
    logger.info("=" * 60)


# ============================================================
# MAIN
# ============================================================
def run_quality_checks():
    logger.info("=" * 60)
    logger.info("  Party Master Data Hub — Data Quality Pipeline")
    logger.info("=" * 60)

    conn     = get_connection()
    run_id   = str(uuid.uuid4())
    logger.info(f"  Run ID: {run_id}")

    all_results = []

    # --- Suite 1: Source Parties ---
    source_df = load_table(conn, 'party_master.source_parties')
    result1   = validate_source_parties(source_df)
    save_dq_result(conn, run_id, 'source_parties',
                   'source_parties_suite', result1)
    all_results.append(result1)

    # --- Suite 2: Golden Parties ---
    golden_df = load_table(conn, 'party_master.golden_parties')
    result2   = validate_golden_parties(golden_df)
    save_dq_result(conn, run_id, 'golden_parties',
                   'golden_parties_suite', result2)
    all_results.append(result2)

    # --- Suite 3: Cluster Integrity ---
    result3   = validate_clusters(conn)
    save_dq_result(conn, run_id, 'party_clusters',
                   'cluster_integrity_suite', result3)
    all_results.append(result3)

    # --- Print Report ---
    print_report(all_results)

    conn.close()
    logger.info("\nNext: Run src/quality/export_golden_to_s3.py")


if __name__ == "__main__":
    run_quality_checks()
