"""
party_consumer.py
-----------------
Kafka Consumer that processes real-time party update events
and updates golden records in PostgreSQL accordingly.

Listens to 'party.events.raw' topic and handles:
  - ADDRESS_CHANGE    — updates address fields on golden record
  - KYC_STATUS_UPDATE — logs KYC status in lineage
  - NEW_PARTY         — creates new source + golden record
  - STATUS_CHANGE     — updates entity_status on golden record
  - ENTITY_MERGE      — merges two golden records into one

After processing, publishes confirmation to 'party.events.updates'

Usage:
    python src/streaming/party_consumer.py
    (Run this in a separate terminal while producer is running)
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

from confluent_kafka import Consumer, Producer, KafkaError
import psycopg2
import psycopg2.extras
from loguru import logger


# ============================================================
# CONFIG
# ============================================================
KAFKA_SERVERS   = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_RAW       = os.getenv('KAFKA_TOPIC_PARTY_EVENTS', 'party.events.raw')
TOPIC_UPDATES   = os.getenv('KAFKA_TOPIC_PARTY_UPDATES', 'party.events.updates')
CONSUMER_GROUP  = os.getenv('KAFKA_CONSUMER_GROUP', 'party-master-consumer')
DB_URL          = os.getenv('DATABASE_URL')


# ============================================================
# DATABASE HELPERS
# ============================================================
def get_connection():
    conn = psycopg2.connect(DB_URL)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn


def log_lineage(cur, golden_id: str, event_type: str,
                changed_fields: dict, triggered_by: str):
    """Log a lineage event for a golden record change."""
    cur.execute("""
        INSERT INTO party_master.party_lineage
            (golden_id, event_type, changed_fields, triggered_by, event_at)
        VALUES (%s, %s, %s, %s, NOW())
    """, (
        golden_id,
        event_type,
        psycopg2.extras.Json(changed_fields),
        triggered_by
    ))


# ============================================================
# EVENT HANDLERS
# ============================================================

def handle_address_change(cur, event: dict) -> dict:
    """Update address fields on the golden record."""
    golden_id = event.get('golden_id')
    if not golden_id:
        return {'status': 'skipped', 'reason': 'no golden_id'}

    payload = event.get('payload', {})

    # Get current address for lineage logging
    cur.execute("""
        SELECT address_line1, city, state, postal_code
        FROM party_master.golden_parties
        WHERE golden_id = %s
    """, (golden_id,))
    current = cur.fetchone()
    if not current:
        return {'status': 'skipped', 'reason': 'golden_id not found'}

    # Update address
    cur.execute("""
        UPDATE party_master.golden_parties SET
            address_line1 = %s,
            address_line2 = %s,
            city          = %s,
            state         = %s,
            postal_code   = %s,
            country       = %s,
            updated_at    = NOW()
        WHERE golden_id = %s
    """, (
        payload.get('address_line1'),
        payload.get('address_line2'),
        payload.get('city'),
        payload.get('state'),
        payload.get('postal_code'),
        payload.get('country', 'USA'),
        golden_id
    ))

    # Log lineage
    log_lineage(cur, golden_id, 'ADDRESS_UPDATED', {
        'old_address': dict(current),
        'new_address': payload,
        'source':      event.get('source_system'),
        'event_id':    event.get('event_id'),
    }, 'party_consumer.py')

    return {'status': 'success', 'golden_id': golden_id, 'action': 'address_updated'}


def handle_kyc_update(cur, event: dict) -> dict:
    """Log KYC status update in lineage (no direct field update)."""
    golden_id = event.get('golden_id')
    if not golden_id:
        return {'status': 'skipped', 'reason': 'no golden_id'}

    payload = event.get('payload', {})

    # Verify party exists
    cur.execute(
        "SELECT golden_id FROM party_master.golden_parties WHERE golden_id = %s",
        (golden_id,)
    )
    if not cur.fetchone():
        return {'status': 'skipped', 'reason': 'golden_id not found'}

    # Log KYC event in lineage
    log_lineage(cur, golden_id, 'KYC_STATUS_UPDATED', {
        'kyc_status':        payload.get('kyc_status'),
        'kyc_provider':      payload.get('kyc_provider'),
        'risk_score':        payload.get('risk_score'),
        'verification_date': payload.get('verification_date'),
        'notes':             payload.get('notes'),
        'event_id':          event.get('event_id'),
    }, 'party_consumer.py')

    return {'status': 'success', 'golden_id': golden_id, 'action': 'kyc_logged'}


def handle_new_party(cur, event: dict) -> dict:
    """Create a new source party and golden record."""
    payload    = event.get('payload', {})
    party_type = event.get('party_type', 'INDIVIDUAL')
    source     = event.get('source_system', 'internal_crm')
    event_id   = event.get('event_id', str(uuid.uuid4()))

    # Insert into source_parties
    cur.execute("""
        INSERT INTO party_master.source_parties (
            source_system, source_party_id, party_type,
            full_name, first_name, last_name,
            tax_id, address_line1, address_line2,
            city, state, postal_code, country,
            email, phone, entity_status, raw_payload,
            ingested_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
        )
        ON CONFLICT (source_system, source_party_id) DO NOTHING
        RETURNING id
    """, (
        source, f"STREAM-{event_id}", party_type,
        payload.get('full_name'),
        payload.get('first_name'),
        payload.get('last_name'),
        payload.get('tax_id'),
        payload.get('address_line1'),
        payload.get('address_line2'),
        payload.get('city'),
        payload.get('state'),
        payload.get('postal_code'),
        payload.get('country', 'USA'),
        payload.get('email'),
        payload.get('phone'),
        payload.get('entity_status', 'ACTIVE'),
        psycopg2.extras.Json(payload),
    ))
    result = cur.fetchone()
    if not result:
        return {'status': 'skipped', 'reason': 'duplicate source record'}

    source_id = result['id']

    # Create golden record
    golden_id = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO party_master.golden_parties (
            golden_id, party_type, full_name, first_name, last_name,
            tax_id, address_line1, address_line2, city, state,
            postal_code, country, email, phone,
            entity_status, confidence_score, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, NOW(), NOW()
        )
    """, (
        golden_id, party_type,
        payload.get('full_name'),
        payload.get('first_name'),
        payload.get('last_name'),
        payload.get('tax_id'),
        payload.get('address_line1'),
        payload.get('address_line2'),
        payload.get('city'),
        payload.get('state'),
        payload.get('postal_code'),
        payload.get('country', 'USA'),
        payload.get('email'),
        payload.get('phone'),
        payload.get('entity_status', 'ACTIVE'),
        1.0,  # Single source = perfect confidence
    ))

    # Link in clusters
    cur.execute("""
        INSERT INTO party_master.party_clusters
            (golden_id, source_party_id, is_survivor, match_score, linked_at)
        VALUES (%s, %s, true, 1.0, NOW())
    """, (golden_id, source_id))

    # Log lineage
    log_lineage(cur, golden_id, 'CREATED', {
        'source':      source,
        'event_id':    event_id,
        'party_type':  party_type,
        'full_name':   payload.get('full_name'),
        'via':         'kafka_stream',
    }, 'party_consumer.py')

    return {
        'status':    'success',
        'golden_id': golden_id,
        'action':    'new_party_created'
    }


def handle_status_change(cur, event: dict) -> dict:
    """Update entity status on the golden record."""
    golden_id = event.get('golden_id')
    if not golden_id:
        return {'status': 'skipped', 'reason': 'no golden_id'}

    payload    = event.get('payload', {})
    new_status = payload.get('entity_status')
    if not new_status:
        return {'status': 'skipped', 'reason': 'no entity_status in payload'}

    # Get current status for lineage
    cur.execute(
        "SELECT entity_status FROM party_master.golden_parties WHERE golden_id = %s",
        (golden_id,)
    )
    current = cur.fetchone()
    if not current:
        return {'status': 'skipped', 'reason': 'golden_id not found'}

    old_status = current['entity_status']

    cur.execute("""
        UPDATE party_master.golden_parties SET
            entity_status = %s,
            updated_at    = NOW()
        WHERE golden_id = %s
    """, (new_status, golden_id))

    log_lineage(cur, golden_id, 'STATUS_CHANGED', {
        'old_status': old_status,
        'new_status': new_status,
        'reason':     payload.get('reason'),
        'event_id':   event.get('event_id'),
    }, 'party_consumer.py')

    return {
        'status':    'success',
        'golden_id': golden_id,
        'action':    f'status_changed_{old_status}_to_{new_status}'
    }


def handle_entity_merge(cur, event: dict) -> dict:
    """Merge two golden records — mark secondary as merged."""
    payload      = event.get('payload', {})
    primary_id   = payload.get('primary_golden_id')
    secondary_id = payload.get('secondary_golden_id')

    if not primary_id or not secondary_id:
        return {'status': 'skipped', 'reason': 'missing golden IDs for merge'}

    # Mark secondary as merged
    cur.execute("""
        UPDATE party_master.golden_parties SET
            entity_status = 'MERGED',
            updated_at    = NOW()
        WHERE golden_id = %s
    """, (secondary_id,))

    # Move secondary's clusters to primary
    cur.execute("""
        UPDATE party_master.party_clusters SET
            golden_id  = %s,
            is_survivor = false
        WHERE golden_id = %s
    """, (primary_id, secondary_id))

    # Log lineage on primary
    log_lineage(cur, primary_id, 'MERGED', {
        'primary_id':   primary_id,
        'secondary_id': secondary_id,
        'reason':       payload.get('reason'),
        'merged_by':    payload.get('merged_by'),
        'event_id':     event.get('event_id'),
    }, 'party_consumer.py')

    return {
        'status':    'success',
        'golden_id': primary_id,
        'action':    f'merged_{secondary_id}_into_{primary_id}'
    }


# ============================================================
# MAIN CONSUMER LOOP
# ============================================================
def run_consumer(max_messages: int = 50, timeout: float = 30.0):
    """
    Consume party events from Kafka and process them.
    Args:
        max_messages: Stop after processing this many messages
        timeout:      Stop if no messages received for this many seconds
    """
    logger.info("=" * 60)
    logger.info("  Party Master Data Hub — Kafka Consumer")
    logger.info("=" * 60)
    logger.info(f"  Topic:      {TOPIC_RAW}")
    logger.info(f"  Group:      {CONSUMER_GROUP}")
    logger.info(f"  Max msgs:   {max_messages}")
    logger.info("\nWaiting for events... (Press Ctrl+C to stop)\n")

    # Create consumer
    consumer = Consumer({
        'bootstrap.servers':  KAFKA_SERVERS,
        'group.id':           CONSUMER_GROUP,
        'auto.offset.reset':  'earliest',
        'enable.auto.commit': True,
    })
    consumer.subscribe([TOPIC_RAW])

    # Create confirmation producer
    confirm_producer = Producer({'bootstrap.servers': KAFKA_SERVERS})

    # Stats
    processed     = 0
    errors        = 0
    skipped       = 0
    counts        = {}
    empty_polls   = 0
    MAX_EMPTY     = 3   # Exit after 3 consecutive empty polls (15 seconds)

    try:
        while processed < max_messages:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                empty_polls += 1
                logger.info(f"  Waiting for messages... ({empty_polls}/{MAX_EMPTY})")
                if empty_polls >= MAX_EMPTY:
                    logger.info("  No more messages — exiting consumer loop")
                    break
                continue

            # Reset empty poll counter when a message arrives
            empty_polls = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("  End of partition reached")
                    break
                logger.error(f"  Kafka error: {msg.error()}")
                errors += 1
                continue

            # Parse event
            try:
                event = json.loads(msg.value().decode('utf-8'))
            except json.JSONDecodeError as e:
                logger.error(f"  Failed to parse message: {e}")
                errors += 1
                continue

            event_type = event.get('event_type', 'UNKNOWN')
            event_id   = event.get('event_id', 'N/A')

            logger.info(f"  Processing [{processed+1}] {event_type} | {event_id}")

            # Process event
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    if event_type == 'ADDRESS_CHANGE':
                        result = handle_address_change(cur, event)
                    elif event_type == 'KYC_STATUS_UPDATE':
                        result = handle_kyc_update(cur, event)
                    elif event_type == 'NEW_PARTY':
                        result = handle_new_party(cur, event)
                    elif event_type == 'STATUS_CHANGE':
                        result = handle_status_change(cur, event)
                    elif event_type == 'ENTITY_MERGE':
                        result = handle_entity_merge(cur, event)
                    else:
                        result = {'status': 'skipped', 'reason': f'unknown event type: {event_type}'}

                conn.commit()

                # Publish confirmation event
                confirmation = {
                    'event_id':    event_id,
                    'event_type':  event_type,
                    'result':      result,
                    'processed_at': datetime.now().isoformat(),
                }
                confirm_producer.produce(
                    topic=TOPIC_UPDATES,
                    key=event_id,
                    value=json.dumps(confirmation)
                )
                confirm_producer.poll(0)

                if result.get('status') == 'success':
                    processed += 1
                    counts[event_type] = counts.get(event_type, 0) + 1
                    logger.success(f"    ✅ {result.get('action', 'processed')}")
                else:
                    skipped += 1
                    logger.warning(f"    ⚠️  Skipped: {result.get('reason')}")

            except Exception as e:
                conn.rollback()
                logger.error(f"    ❌ Error processing {event_type}: {e}")
                errors += 1
            finally:
                conn.close()

    except KeyboardInterrupt:
        logger.info("\nConsumer stopped by user")
    finally:
        consumer.close()
        confirm_producer.flush()

    # Summary
    logger.info("\n" + "=" * 60)
    logger.success(f"Consumer complete!")
    logger.info(f"  Processed: {processed} events")
    logger.info(f"  Skipped:   {skipped} events")
    logger.info(f"  Errors:    {errors} events")
    logger.info("\n  Event breakdown:")
    for etype, count in counts.items():
        logger.info(f"    {etype:<25} {count}")
    logger.info("=" * 60)

    # Refresh S3 after processing
    refresh_s3_after_processing(processed)


def refresh_s3_after_processing(processed: int):
    """Refresh S3 Parquet files if any events were processed."""
    if processed <= 0:
        return

    logger.info("\nRefreshing S3 golden records export...")
    try:
        import importlib.util
        export_path = BASE_DIR / 'src' / 'pipeline' / 'export_golden_to_s3.py'
        spec   = importlib.util.spec_from_file_location("export_golden_to_s3", export_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.run_export()
        logger.success("  S3 Parquet files updated with latest golden records")
    except Exception as e:
        logger.warning(f"  S3 export skipped: {e}")

if __name__ == "__main__":
    run_consumer(max_messages=50, timeout=30.0)
