"""
main.py
-------
FastAPI application that serves golden party records.
Mirrors what LPL Financial would expose to downstream systems
like CRM, trading, compliance, and reporting platforms.

Endpoints:
    GET  /health                          — Health check
    GET  /parties/{golden_id}             — Fetch single golden record
    GET  /parties/search                  — Search golden records
    GET  /parties/{golden_id}/lineage     — Full audit trail
    GET  /parties/{golden_id}/sources     — Contributing source records
    GET  /parties/stats                   — Summary statistics

Usage:
    python src/api/main.py
    Then open: http://localhost:8000/docs
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, List

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(dotenv_path=BASE_DIR / 'config' / '.env')

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from loguru import logger


# ============================================================
# APP SETUP
# ============================================================
app = FastAPI(
    title="Party Master Data Hub API",
    description="""
    REST API for serving golden party reference data.
    Provides access to master party records including individuals,
    organizations, and legal entities — with full lineage tracking.

    Built to mirror enterprise financial data services like those
    used at LPL Financial for KYC/AML, client onboarding, and
    regulatory reporting.
    Bsangu
    """,
    version="1.0.0",
    contact={
        "name": "Party Master Data Hub",
    }
)

# Allow all origins for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_URL = os.getenv('DATABASE_URL')


# ============================================================
# DATABASE CONNECTION
# ============================================================
def get_db_connection():
    """Get a database connection with dict cursor."""
    conn = psycopg2.connect(DB_URL)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn


# ============================================================
# PYDANTIC MODELS (Response Schemas)
# ============================================================
class GoldenParty(BaseModel):
    golden_id:          str
    party_type:         Optional[str]
    full_name:          Optional[str]
    first_name:         Optional[str]
    last_name:          Optional[str]
    date_of_birth:      Optional[str]
    tax_id:             Optional[str]
    lei:                Optional[str]
    address_line1:      Optional[str]
    address_line2:      Optional[str]
    city:               Optional[str]
    state:              Optional[str]
    postal_code:        Optional[str]
    country:            Optional[str]
    email:              Optional[str]
    phone:              Optional[str]
    entity_status:      Optional[str]
    confidence_score:   Optional[float]
    created_at:         Optional[str]
    updated_at:         Optional[str]

    class Config:
        from_attributes = True


class LineageEvent(BaseModel):
    id:             int
    golden_id:      str
    event_type:     str
    triggered_by:   Optional[str]
    event_at:       Optional[str]
    changed_fields: Optional[dict]


class SourceRecord(BaseModel):
    id:               int
    source_system:    str
    source_party_id:  str
    party_type:       Optional[str]
    full_name:        Optional[str]
    tax_id:           Optional[str]
    entity_status:    Optional[str]
    is_survivor:      Optional[bool]
    match_score:      Optional[float]
    ingested_at:      Optional[str]


class SearchResult(BaseModel):
    total:   int
    results: List[GoldenParty]


class StatsResult(BaseModel):
    total_golden_records:   int
    total_source_records:   int
    total_duplicates_resolved: int
    by_party_type:          dict
    avg_confidence_score:   float
    last_mastering_run:     Optional[str]


class HealthResponse(BaseModel):
    status:     str
    database:   str
    timestamp:  str
    version:    str


# ============================================================
# HELPER — serialize rows
# ============================================================
def serialize_row(row) -> dict:
    """Convert psycopg2 RealDictRow to serializable dict."""
    result = {}
    for key, value in dict(row).items():
        if hasattr(value, 'isoformat'):
            result[key] = value.isoformat()
        elif value is None:
            result[key] = None
        else:
            result[key] = value
    return result


# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/health", response_model=HealthResponse, tags=["System"])
def health_check():
    """Check API and database health."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM party_master.golden_parties")
            count = cur.fetchone()['count']
        conn.close()
        db_status = f"connected ({count} golden records)"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return {
        "status":    "healthy",
        "database":  db_status,
        "timestamp": datetime.now().isoformat(),
        "version":   "1.0.0",
    }


@app.get("/parties/stats", response_model=StatsResult, tags=["Parties"])
def get_stats():
    """Get summary statistics about the party master data."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Total counts
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM party_master.golden_parties) as total_golden,
                    (SELECT COUNT(*) FROM party_master.source_parties) as total_source,
                    (SELECT COUNT(*) FROM party_master.party_clusters
                     WHERE golden_id IN (
                         SELECT golden_id FROM party_master.party_clusters
                         GROUP BY golden_id HAVING COUNT(*) > 1
                     )) as duplicates_resolved,
                    (SELECT ROUND(AVG(confidence_score)::numeric, 4)
                     FROM party_master.golden_parties) as avg_confidence,
                    (SELECT MAX(event_at) FROM party_master.party_lineage) as last_run
            """)
            stats = dict(cur.fetchone())

            # By party type
            cur.execute("""
                SELECT party_type, COUNT(*) as count,
                       ROUND(AVG(confidence_score)::numeric, 3) as avg_confidence
                FROM party_master.golden_parties
                GROUP BY party_type
            """)
            by_type = {
                row['party_type']: {
                    'count': row['count'],
                    'avg_confidence': float(row['avg_confidence'] or 0)
                }
                for row in cur.fetchall()
            }

        return {
            "total_golden_records":      stats['total_golden'],
            "total_source_records":      stats['total_source'],
            "total_duplicates_resolved": stats['duplicates_resolved'],
            "by_party_type":             by_type,
            "avg_confidence_score":      float(stats['avg_confidence'] or 0),
            "last_mastering_run":        stats['last_run'].isoformat()
                                         if stats['last_run'] else None,
        }
    finally:
        conn.close()


@app.get("/parties/search", response_model=SearchResult, tags=["Parties"])
def search_parties(
    name:       Optional[str] = Query(None, description="Search by full name (partial match)"),
    tax_id:     Optional[str] = Query(None, description="Search by exact Tax ID"),
    lei:        Optional[str] = Query(None, description="Search by exact LEI"),
    state:      Optional[str] = Query(None, description="Filter by state code (e.g. NY)"),
    party_type: Optional[str] = Query(None, description="Filter by party type: INDIVIDUAL, ORGANIZATION, LEGAL_ENTITY"),
    status:     Optional[str] = Query(None, description="Filter by entity status: ACTIVE, INACTIVE"),
    limit:      int           = Query(20, ge=1, le=100, description="Max results to return"),
    offset:     int           = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Search golden party records with flexible filtering.
    Supports partial name search, exact Tax ID/LEI lookup,
    and filtering by state, party type, and status.
    """
    conn = get_db_connection()
    try:
        conditions = []
        params     = []

        if name:
            conditions.append("full_name ILIKE %s")
            params.append(f"%{name}%")
        if tax_id:
            conditions.append("tax_id = %s")
            params.append(tax_id)
        if lei:
            conditions.append("lei = %s")
            params.append(lei)
        if state:
            conditions.append("state = %s")
            params.append(state.upper())
        if party_type:
            conditions.append("party_type = %s")
            params.append(party_type.upper())
        if status:
            conditions.append("entity_status = %s")
            params.append(status.upper())

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count total
        count_sql = f"SELECT COUNT(*) as count FROM party_master.golden_parties {where}"
        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            total = cur.fetchone()['count']

        # Fetch results
        search_sql = f"""
            SELECT * FROM party_master.golden_parties
            {where}
            ORDER BY full_name
            LIMIT %s OFFSET %s
        """
        with conn.cursor() as cur:
            cur.execute(search_sql, params + [limit, offset])
            rows = [serialize_row(r) for r in cur.fetchall()]

        return {"total": total, "results": rows}

    finally:
        conn.close()


@app.get("/parties/{golden_id}", response_model=GoldenParty, tags=["Parties"])
def get_party(golden_id: str):
    """
    Fetch a single golden party record by its golden ID.
    Returns the master record with all fields.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM party_master.golden_parties WHERE golden_id = %s",
                (golden_id,)
            )
            row = cur.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Party with golden_id '{golden_id}' not found"
            )

        return serialize_row(row)
    finally:
        conn.close()


@app.get("/parties/{golden_id}/lineage",
         response_model=List[LineageEvent], tags=["Lineage"])
def get_party_lineage(golden_id: str):
    """
    Get the full audit trail for a golden party record.
    Shows every change event — creation, updates, merges.
    This is the data lineage feature required for regulatory compliance.
    """
    conn = get_db_connection()
    try:
        # Verify party exists
        with conn.cursor() as cur:
            cur.execute(
                "SELECT golden_id FROM party_master.golden_parties WHERE golden_id = %s",
                (golden_id,)
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404,
                                    detail=f"Party '{golden_id}' not found")

        # Get lineage events
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, golden_id::text, event_type, triggered_by,
                       event_at, changed_fields
                FROM party_master.party_lineage
                WHERE golden_id = %s
                ORDER BY event_at DESC
            """, (golden_id,))
            rows = [serialize_row(r) for r in cur.fetchall()]

        return rows
    finally:
        conn.close()


@app.get("/parties/{golden_id}/sources",
         response_model=List[SourceRecord], tags=["Lineage"])
def get_party_sources(golden_id: str):
    """
    Get all source records that contributed to a golden party record.
    Shows which source systems the golden record was built from,
    which record was the survivor, and the match confidence score.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    sp.id,
                    sp.source_system,
                    sp.source_party_id,
                    sp.party_type,
                    sp.full_name,
                    sp.tax_id,
                    sp.entity_status,
                    pc.is_survivor,
                    pc.match_score,
                    sp.ingested_at
                FROM party_master.party_clusters pc
                JOIN party_master.source_parties sp
                    ON pc.source_party_id = sp.id
                WHERE pc.golden_id = %s
                ORDER BY pc.is_survivor DESC, sp.source_system
            """, (golden_id,))
            rows = [serialize_row(r) for r in cur.fetchall()]

        if not rows:
            raise HTTPException(status_code=404,
                                detail=f"Party '{golden_id}' not found")

        return rows
    finally:
        conn.close()


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    host = os.getenv('API_HOST', '0.0.0.0')
    port = int(os.getenv('API_PORT', '8000'))

    logger.info("=" * 55)
    logger.info("  Party Master Data Hub — FastAPI Server")
    logger.info("=" * 55)
    logger.info(f"  Starting on http://{host}:{port}")
    logger.info(f"  Swagger UI:  http://localhost:{port}/docs")
    logger.info(f"  ReDoc:       http://localhost:{port}/redoc")
    logger.info("=" * 55)

    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=True,
        app_dir=str(Path(__file__).parent)
    )
