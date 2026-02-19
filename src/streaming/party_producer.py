"""
party_producer.py
-----------------
Kafka Producer that simulates real-time party update events.
Publishes events to the 'party.events.raw' topic.

Simulates events like:
  - ADDRESS_CHANGE    — client moves to a new address
  - KYC_STATUS_UPDATE — KYC vendor sends new verification status
  - NEW_PARTY         — new client/entity registered
  - ENTITY_MERGE      — two entities are merged
  - STATUS_CHANGE     — entity becomes inactive/active

In production at LPL, these events would come from:
  - CRM systems when advisors update client info
  - KYC/AML vendors sending verification results
  - Counterparty systems sending entity updates

Usage:
    python src/streaming/party_producer.py
"""

import sys
import os
import json
import time
import random
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(dotenv_path=BASE_DIR / 'config' / '.env')

from confluent_kafka import Producer
from faker import Faker
from loguru import logger
import psycopg2
import psycopg2.extras


# ============================================================
# CONFIG
# ============================================================
KAFKA_SERVERS  = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_RAW      = os.getenv('KAFKA_TOPIC_PARTY_EVENTS', 'party.events.raw')
DB_URL         = os.getenv('DATABASE_URL')

fake = Faker('en_US')
Faker.seed(99)
random.seed(99)

# Event types with weighted probability
EVENT_TYPES = [
    ('ADDRESS_CHANGE',     0.35),
    ('KYC_STATUS_UPDATE',  0.25),
    ('NEW_PARTY',          0.20),
    ('STATUS_CHANGE',      0.12),
    ('ENTITY_MERGE',       0.08),
]

KYC_STATUSES = ['VERIFIED', 'PENDING', 'FAILED', 'EXPIRED']
ENTITY_STATUSES = ['ACTIVE', 'INACTIVE']
US_STATES = ['NY', 'CA', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'NJ']


# ============================================================
# LOAD EXISTING GOLDEN IDS FROM POSTGRES
# So our events reference real records in the database
# ============================================================
def load_golden_ids() -> list:
    """Load existing golden party IDs to reference in events."""
    conn = psycopg2.connect(DB_URL)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    with conn.cursor() as cur:
        cur.execute("""
            SELECT golden_id, party_type, full_name, tax_id
            FROM party_master.golden_parties
            WHERE entity_status = 'ACTIVE'
            LIMIT 100
        """)
        rows = cur.fetchall()
    conn.close()
    logger.info(f"  Loaded {len(rows)} active golden party IDs")
    return [dict(r) for r in rows]


# ============================================================
# EVENT GENERATORS
# ============================================================
def generate_address_change_event(party: dict) -> dict:
    """Simulate a client updating their address."""
    return {
        'event_id':     f"EVT-{fake.uuid4()[:8].upper()}",
        'event_type':   'ADDRESS_CHANGE',
        'golden_id':    party['golden_id'],
        'party_type':   party['party_type'],
        'source_system': random.choice(['internal_crm', 'dnb_feed']),
        'timestamp':    datetime.now().isoformat(),
        'payload': {
            'address_line1': fake.building_number() + ' ' + fake.street_name(),
            'address_line2': random.choice(['', f"Apt {random.randint(1,99)}"]),
            'city':         fake.city(),
            'state':        random.choice(US_STATES),
            'postal_code':  fake.zipcode(),
            'country':      'USA',
        }
    }


def generate_kyc_update_event(party: dict) -> dict:
    """Simulate a KYC vendor sending a new verification status."""
    return {
        'event_id':     f"EVT-{fake.uuid4()[:8].upper()}",
        'event_type':   'KYC_STATUS_UPDATE',
        'golden_id':    party['golden_id'],
        'party_type':   party['party_type'],
        'source_system': 'kyc_vendor',
        'timestamp':    datetime.now().isoformat(),
        'payload': {
            'kyc_status':       random.choice(KYC_STATUSES),
            'kyc_provider':     random.choice(['LexisNexis', 'Refinitiv', 'Dow Jones']),
            'verification_date': datetime.now().isoformat(),
            'risk_score':       round(random.uniform(0.0, 1.0), 3),
            'notes':            random.choice([
                'Annual review completed',
                'Enhanced due diligence required',
                'PEP screening passed',
                'Sanctions check cleared',
            ])
        }
    }


def generate_new_party_event() -> dict:
    """Simulate a brand new party being registered."""
    party_type = random.choice(['INDIVIDUAL', 'ORGANIZATION'])
    if party_type == 'INDIVIDUAL':
        first = fake.first_name()
        last  = fake.last_name()
        name  = f"{first} {last}"
    else:
        name  = fake.company() + ' LLC'
        first = None
        last  = None

    return {
        'event_id':     f"EVT-{fake.uuid4()[:8].upper()}",
        'event_type':   'NEW_PARTY',
        'golden_id':    None,   # No golden ID yet — will be assigned by consumer
        'party_type':   party_type,
        'source_system': 'internal_crm',
        'timestamp':    datetime.now().isoformat(),
        'payload': {
            'full_name':     name,
            'first_name':    first,
            'last_name':     last,
            'tax_id':        f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}",
            'address_line1': fake.building_number() + ' ' + fake.street_name(),
            'city':          fake.city(),
            'state':         random.choice(US_STATES),
            'postal_code':   fake.zipcode(),
            'country':       'USA',
            'email':         fake.email() if party_type == 'INDIVIDUAL' else None,
            'phone':         fake.phone_number(),
            'entity_status': 'ACTIVE',
        }
    }


def generate_status_change_event(party: dict) -> dict:
    """Simulate an entity status change (active/inactive)."""
    return {
        'event_id':     f"EVT-{fake.uuid4()[:8].upper()}",
        'event_type':   'STATUS_CHANGE',
        'golden_id':    party['golden_id'],
        'party_type':   party['party_type'],
        'source_system': random.choice(['internal_crm', 'markit_feed']),
        'timestamp':    datetime.now().isoformat(),
        'payload': {
            'entity_status': random.choice(ENTITY_STATUSES),
            'reason':        random.choice([
                'Account closed by client',
                'Regulatory requirement',
                'Annual review',
                'Reactivation request',
            ])
        }
    }


def generate_entity_merge_event(parties: list) -> dict:
    """Simulate two entities being merged."""
    if len(parties) < 2:
        return None
    party1, party2 = random.sample(parties, 2)
    return {
        'event_id':     f"EVT-{fake.uuid4()[:8].upper()}",
        'event_type':   'ENTITY_MERGE',
        'golden_id':    party1['golden_id'],
        'party_type':   party1['party_type'],
        'source_system': 'internal_crm',
        'timestamp':    datetime.now().isoformat(),
        'payload': {
            'primary_golden_id':   party1['golden_id'],
            'secondary_golden_id': party2['golden_id'],
            'reason':              'Duplicate entity identified during review',
            'merged_by':           'data_steward',
        }
    }


# ============================================================
# PRODUCE EVENTS
# ============================================================
def delivery_callback(err, msg):
    """Called when a message is delivered or fails."""
    if err:
        logger.error(f"  Delivery failed: {err}")
    else:
        logger.info(f"  Delivered → topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def run_producer(num_events: int = 20, delay_seconds: float = 0.5):
    """
    Produce a stream of party update events to Kafka.
    Args:
        num_events: Number of events to produce
        delay_seconds: Delay between events (simulates real-time stream)
    """
    logger.info("=" * 60)
    logger.info("  Party Master Data Hub — Kafka Producer")
    logger.info("=" * 60)
    logger.info(f"  Topic:      {TOPIC_RAW}")
    logger.info(f"  Events:     {num_events}")
    logger.info(f"  Delay:      {delay_seconds}s between events")

    # Load existing parties to reference
    logger.info("\nLoading active golden party IDs...")
    parties = load_golden_ids()

    if not parties:
        logger.error("No active golden parties found. Run the mastering pipeline first.")
        return

    # Create producer
    producer = Producer({
        'bootstrap.servers': KAFKA_SERVERS,
        'client.id':         'party-master-producer',
    })

    logger.info(f"\nStarting event stream to topic '{TOPIC_RAW}'...")
    logger.info("Press Ctrl+C to stop early\n")

    event_counts = {et: 0 for et, _ in EVENT_TYPES}
    produced     = 0

    try:
        for i in range(num_events):
            # Pick event type based on weighted probability
            event_type = random.choices(
                [et for et, _ in EVENT_TYPES],
                weights=[w for _, w in EVENT_TYPES]
            )[0]

            # Generate event
            event = None
            if event_type == 'ADDRESS_CHANGE':
                event = generate_address_change_event(random.choice(parties))
            elif event_type == 'KYC_STATUS_UPDATE':
                event = generate_kyc_update_event(random.choice(parties))
            elif event_type == 'NEW_PARTY':
                event = generate_new_party_event()
            elif event_type == 'STATUS_CHANGE':
                event = generate_status_change_event(random.choice(parties))
            elif event_type == 'ENTITY_MERGE':
                event = generate_entity_merge_event(parties)

            if not event:
                continue

            # Publish to Kafka
            producer.produce(
                topic=TOPIC_RAW,
                key=event.get('golden_id') or event['event_id'],
                value=json.dumps(event),
                callback=delivery_callback
            )
            producer.poll(0)  # Trigger callbacks

            event_counts[event_type] += 1
            produced += 1

            logger.info(f"  [{i+1}/{num_events}] Published {event_type} | "
                       f"event_id={event['event_id']}")

            time.sleep(delay_seconds)

    except KeyboardInterrupt:
        logger.info("\nProducer stopped by user")
    finally:
        # Wait for all messages to be delivered
        logger.info("\nFlushing remaining messages...")
        producer.flush()

    # Summary
    logger.info("\n" + "=" * 60)
    logger.success(f"Producer complete! Published {produced} events")
    logger.info("Event breakdown:")
    for event_type, count in event_counts.items():
        if count > 0:
            logger.info(f"  {event_type:<25} {count} events")
    logger.info("=" * 60)
    logger.info("Now run: python src/streaming/party_consumer.py")


if __name__ == "__main__":
    run_producer(num_events=20, delay_seconds=0.5)
