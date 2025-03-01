"""
Tests for database operations including matching and merging.
"""
import os
import pytest
from datetime import datetime
from typing import Generator
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from openmatch.connectors import (
    init_database,
    DatabaseConnector,
    MasterRecord,
    SourceRecord,
    MatchResult,
    MergeHistory
)
from openmatch.match import MatchEngine, MatchType, MatchConfig, BlockingConfig, MatchRuleConfig, FieldConfig, MetadataConfig
from .setup_test_db import setup_test_database
from collections import defaultdict
from sqlalchemy.sql import func
import time
import random
from tqdm import tqdm

@pytest.fixture(scope="session")
def db_config() -> dict:
    """Database configuration for tests."""
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        pytest.skip("POSTGRES_PASSWORD environment variable not set")
        
    return {
        "host": "localhost",
        "port": 5432,
        "database": "openmatch_test",
        "username": "postgres",
        "password": password,
        "schema": "mdm"
    }

@pytest.fixture(scope="session")
def db_connector(db_config) -> DatabaseConnector:
    """Initialize database connector."""
    return init_database(**db_config)

@pytest.fixture(scope="session")
def setup_db(db_config) -> None:
    """Set up test database with sample data."""
    setup_test_database(**db_config)

@pytest.fixture
def db_session(db_connector) -> Generator[Session, None, None]:
    """Provide a database session."""
    with db_connector.session() as session:
        yield session

def test_database_connection(db_session, setup_db):
    """Test database connection and basic operations."""
    # Check if we can execute a simple query
    result = db_session.execute(text("SELECT 1"))
    assert result.scalar() == 1

def test_source_records_loaded(db_session, setup_db):
    """Test that source records were loaded correctly."""
    # Count total records
    total_records = db_session.query(SourceRecord).count()
    assert total_records > 0, "No source records found"
    
    # Check record structure
    sample_record = db_session.query(SourceRecord).first()
    assert sample_record is not None
    assert sample_record.record_data is not None
    assert "first_name" in sample_record.record_data
    assert "last_name" in sample_record.record_data

def test_match_and_merge_workflow(db_session, setup_db):
    """Test complete match and merge workflow using database records."""
    # Initialize metrics
    metrics = {
        "processed_records": 0,
        "total_comparisons": 0,
        "total_matches": 0,
        "avg_confidence": 0.0,
        "match_types": defaultdict(int),
        "match_rules": defaultdict(int),
        "confidence_ranges": defaultdict(int),
        "field_match_rates": defaultdict(lambda: {"matches": 0, "total": 0})
    }
    
    # Set up match engine with rules
    engine = MatchEngine(
        MatchConfig(
            blocking=BlockingConfig(
                blocking_keys=["ssn", "birth_date"]
            ),
            rules=[
                MatchRuleConfig(
                    name="Exact SSN Match",
                    rule_id="exact_ssn_match",
                    fields=[
                        FieldConfig(
                            name="ssn",
                            weight=1.0,
                            threshold=0.7,
                            match_type=MatchType.EXACT
                        )
                    ],
                    min_confidence=0.7
                ),
                MatchRuleConfig(
                    name="Fuzzy Name and DOB Match",
                    rule_id="fuzzy_name_dob_match",
                    fields=[
                        FieldConfig(
                            name="first_name",
                            weight=0.3,
                            threshold=0.4,
                            match_type=MatchType.FUZZY,
                            fuzzy_method="jaro_winkler"
                        ),
                        FieldConfig(
                            name="last_name",
                            weight=0.3,
                            threshold=0.4,
                            match_type=MatchType.FUZZY,
                            fuzzy_method="jaro_winkler"
                        ),
                        FieldConfig(
                            name="birth_date",
                            weight=0.4,
                            threshold=0.7,
                            match_type=MatchType.EXACT
                        )
                    ],
                    min_confidence=0.5
                )
            ],
            metadata=MetadataConfig()
        )
    )
    
    # Get sample records for testing
    start_time = time.time()
    batch_size = 200
    total_records = 2000

    # Sample records that are more likely to be duplicates
    # First, get records with duplicate SSNs
    duplicate_ssns = db_session.execute(
        text("""
            WITH duplicate_ssns AS (
                SELECT record_data->>'ssn' as ssn, COUNT(*) as cnt
                FROM mdm.source_records
                WHERE record_data->>'ssn' IS NOT NULL
                GROUP BY record_data->>'ssn'
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
                LIMIT 100
            ),
            record_pairs AS (
                SELECT DISTINCT s1.id, RANDOM() as rand
                FROM mdm.source_records s1
                JOIN mdm.source_records s2 ON s1.record_data->>'ssn' = s2.record_data->>'ssn'
                AND s1.id != s2.id
                JOIN duplicate_ssns d ON s1.record_data->>'ssn' = d.ssn
            )
            SELECT id
            FROM record_pairs
            ORDER BY rand
            LIMIT :limit
        """),
        {"limit": total_records // 2}
    ).fetchall()

    # Then, get records with duplicate birth dates for the remaining slots
    duplicate_birth_dates = db_session.execute(
        text("""
            WITH duplicate_birth_dates AS (
                SELECT record_data->>'birth_date' as birth_date, COUNT(*) as cnt
                FROM mdm.source_records
                WHERE record_data->>'birth_date' IS NOT NULL
                GROUP BY record_data->>'birth_date'
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
                LIMIT 100
            ),
            record_pairs AS (
                SELECT DISTINCT s1.id, RANDOM() as rand
                FROM mdm.source_records s1
                JOIN mdm.source_records s2 ON s1.record_data->>'birth_date' = s2.record_data->>'birth_date'
                AND s1.id != s2.id
                JOIN duplicate_birth_dates d ON s1.record_data->>'birth_date' = d.birth_date
                WHERE s1.id NOT IN :duplicate_ssn_ids
            )
            SELECT id
            FROM record_pairs
            ORDER BY rand
            LIMIT :limit
        """),
        {
            "duplicate_ssn_ids": tuple(r[0] for r in duplicate_ssns) or (0,),
            "limit": total_records - len(duplicate_ssns)
        }
    ).fetchall()

    # Print debug info about selected records
    print(f"\nFound {len(duplicate_ssns)} records with duplicate SSNs")
    print(f"Found {len(duplicate_birth_dates)} records with duplicate birth dates")

    # Combine and shuffle the records
    record_ids = [r[0] for r in duplicate_ssns] + [r[0] for r in duplicate_birth_dates]
    random.shuffle(record_ids)

    # Get the actual records
    sample_records = db_session.query(SourceRecord).filter(SourceRecord.id.in_(record_ids)).all()
    
    total_batches = (len(sample_records) + batch_size - 1) // batch_size
    
    print("\nStarting match and merge process...")
    for batch_start in tqdm(range(0, len(sample_records), batch_size), total=total_batches, desc="Processing batches"):
        batch = sample_records[batch_start:batch_start + batch_size]
        
        # Debug blocking
        print("\nAnalyzing batch blocking...")
        block_stats = defaultdict(int)
        for i, record1 in enumerate(batch):
            for record2 in batch[i+1:]:
                # Check if records share any blocking keys
                r1_data = record1.record_data
                r2_data = record2.record_data
                if r1_data.get("ssn") == r2_data.get("ssn"):
                    block_stats["ssn_match"] += 1
                if r1_data.get("birth_date") == r2_data.get("birth_date"):
                    block_stats["birth_date_match"] += 1
                if r1_data.get("ssn") == r2_data.get("ssn") or r1_data.get("birth_date") == r2_data.get("birth_date"):
                    block_stats["total_blocked"] += 1
                    # Print example of blocked records
                    if block_stats["total_blocked"] <= 3:  # Only print first 3 examples
                        print(f"\nBlocked Records Example {block_stats['total_blocked']}:")
                        print(f"Record 1: {r1_data}")
                        print(f"Record 2: {r2_data}")
                        print(f"SSN Match: {r1_data.get('ssn') == r2_data.get('ssn')}")
                        print(f"Birth Date Match: {r1_data.get('birth_date') == r2_data.get('birth_date')}")

        print("\nBlocking Statistics:")
        print(f"Total comparisons in batch: {len(batch) * (len(batch) - 1) // 2}")
        print(f"Records blocked by SSN: {block_stats['ssn_match']}")
        print(f"Records blocked by birth_date: {block_stats['birth_date_match']}")
        print(f"Total blocked records: {block_stats['total_blocked']}")

        # Process matches within batch
        matches = []
        for i, record1 in enumerate(batch):
            metrics["processed_records"] += 1
            
            # Compare with all subsequent records in batch
            for record2 in batch[i+1:]:
                metrics["total_comparisons"] += 1
                
                # Debug blocking
                if i == 0 and len(matches) < 5:  # Only print first 5 potential matches
                    print("\nPotential match pair:")
                    print(f"Record 1: {record1.record_data}")
                    print(f"Record 2: {record2.record_data}")
                    print(f"Same SSN: {record1.record_data.get('ssn') == record2.record_data.get('ssn')}")
                    print(f"Same birth_date: {record1.record_data.get('birth_date') == record2.record_data.get('birth_date')}")
                
                match_type, confidence, rule_id = engine.match_records(
                    record1.record_data,
                    record2.record_data
                )
                
                if i == 0 and len(matches) < 5:  # Debug first 5 comparisons
                    print(f"Match type: {match_type}")
                    print(f"Confidence: {confidence}")
                    print(f"Rule: {rule_id}")
                
                metrics["match_types"][match_type.value] += 1
                if match_type not in [MatchType.NO_MATCH, MatchType.ERROR]:
                    metrics["total_matches"] += 1
                    metrics["avg_confidence"] = (
                        (metrics["avg_confidence"] * (metrics["total_matches"] - 1) + confidence)
                        / metrics["total_matches"]
                    )
                    
                    # Create match result
                    match_result = MatchResult(
                        source_record_id=record1.id,
                        matched_record_id=record2.id,
                        match_score=confidence,
                        match_details={
                            "match_type": match_type.value,
                            "matched_fields": ["first_name", "last_name", "birth_date"],
                            "rule_id": rule_id
                        },
                        status="CONFIRMED" if confidence >= 0.7 else "PENDING"
                    )
                    matches.append(match_result)
        
        # Save match results
        if matches:
            db_session.add_all(matches)
            db_session.commit()
    
    # Create master records
    print("\nCreating master records...")
    db_session.execute(text("""
        WITH RECURSIVE 
        -- First, identify connected components (clusters)
        match_clusters AS (
            -- Start with confirmed matches
            SELECT 
                source_record_id as record_id,
                source_record_id as cluster_id,
                ARRAY[source_record_id] as cluster_members,
                1 as depth
            FROM mdm.match_results
            WHERE status = 'CONFIRMED'
            
            UNION
            
            -- Recursively find connected records
            SELECT 
                CASE
                    WHEN m.source_record_id = c.record_id THEN m.matched_record_id
                    ELSE m.source_record_id
                END as record_id,
                c.cluster_id,
                array_append(c.cluster_members, 
                    CASE
                        WHEN m.source_record_id = c.record_id THEN m.matched_record_id
                        ELSE m.source_record_id
                    END
                ) as cluster_members,
                c.depth + 1
            FROM match_clusters c
            JOIN mdm.match_results m ON 
                (m.source_record_id = c.record_id OR m.matched_record_id = c.record_id)
            WHERE m.status = 'CONFIRMED'
            AND c.depth < 10  -- Limit cluster depth to prevent cycles
            AND NOT (
                CASE
                    WHEN m.source_record_id = c.record_id THEN m.matched_record_id
                    ELSE m.source_record_id
                END = ANY(c.cluster_members)
            )
        ),
        -- Aggregate cluster information
        cluster_stats AS (
            SELECT DISTINCT ON (record_id)
                record_id,
                cluster_id,
                cluster_members,
                array_length(cluster_members, 1) as cluster_size
            FROM match_clusters
            ORDER BY record_id, depth DESC
        ),
        -- Calculate golden records
        golden_records AS (
            SELECT 
                cs.cluster_id,
                'person' as entity_type,
                jsonb_build_object(
                    'sources', array_agg(DISTINCT s.source_system),
                    'first_name', mode() WITHIN GROUP (ORDER BY s.record_data->>'first_name'),
                    'last_name', mode() WITHIN GROUP (ORDER BY s.record_data->>'last_name'),
                    'birth_date', mode() WITHIN GROUP (ORDER BY s.record_data->>'birth_date'),
                    'ssn', mode() WITHIN GROUP (ORDER BY s.record_data->>'ssn'),
                    'email', mode() WITHIN GROUP (ORDER BY s.record_data->>'email'),
                    'phone', mode() WITHIN GROUP (ORDER BY s.record_data->>'phone'),
                    'address', mode() WITHIN GROUP (ORDER BY s.record_data->>'address'),
                    'updated_at', CURRENT_TIMESTAMP,
                    'cluster_size', cs.cluster_size,
                    'match_confidence', avg(mr.match_score)
                ) as golden_record,
                avg(mr.match_score) as confidence_score,
                cs.cluster_size as record_count,
                to_jsonb(cs.cluster_members) as source_record_ids
            FROM cluster_stats cs
            JOIN mdm.source_records s ON s.id = cs.record_id
            JOIN mdm.match_results mr ON 
                (mr.source_record_id = cs.record_id OR mr.matched_record_id = cs.record_id)
            WHERE mr.status = 'CONFIRMED'
            GROUP BY cs.cluster_id, cs.cluster_size, cs.cluster_members
        )
        -- Insert golden records
        INSERT INTO mdm.master_records (
            entity_type,
            golden_record,
            confidence_score,
            record_count,
            source_record_ids,
            created_at,
            is_active
        )
        SELECT 
            entity_type,
            golden_record,
            confidence_score,
            record_count,
            source_record_ids,
            CURRENT_TIMESTAMP,
            true
        FROM golden_records
        ON CONFLICT (source_record_ids) 
        DO UPDATE SET
            golden_record = 
                mdm.master_records.golden_record || 
                EXCLUDED.golden_record || 
                jsonb_build_object('updated_at', CURRENT_TIMESTAMP),
            confidence_score = GREATEST(
                mdm.master_records.confidence_score,
                EXCLUDED.confidence_score
            ),
            record_count = EXCLUDED.record_count,
            updated_at = CURRENT_TIMESTAMP,
            is_active = true
        WHERE mdm.master_records.confidence_score < EXCLUDED.confidence_score
    """))
    db_session.commit()
    
    # Print metrics
    elapsed = time.time() - start_time
    print("\nMatching complete!")
    print(f"Time taken: {elapsed:.2f} seconds")
    print(f"Records processed: {metrics['processed_records']}")
    print(f"Total comparisons: {metrics['total_comparisons']}")
    print(f"Total matches: {metrics['total_matches']}")
    print("\nMatch types:")
    for match_type, count in metrics["match_types"].items():
        print(f"  {match_type}: {count}")
    print(f"\nAverage confidence: {metrics['avg_confidence']:.2f}")
    
    # Verify results
    assert metrics["total_matches"] > 0, "No matches found"
    assert metrics["avg_confidence"] > 0.5, "Average confidence too low"

def test_match_statistics(db_session, setup_db):
    """Test match statistics and quality metrics."""
    # Get match statistics using newer SQLAlchemy API
    result = db_session.execute(
        text("""
            SELECT
                status,
                COUNT(*) as count,
                AVG(match_score) as avg_score,
                MIN(match_score) as min_score,
                MAX(match_score) as max_score
            FROM mdm.match_results
            GROUP BY status
        """)
    )
    stats = pd.DataFrame(result.fetchall(), columns=["status", "count", "avg_score", "min_score", "max_score"])
    
    # Verify we have both confirmed and pending matches
    assert not stats.empty, "No match statistics found"
    assert "CONFIRMED" in stats["status"].values, "No confirmed matches found"
    assert "PENDING" in stats["status"].values, "No pending matches found"
    
    # Verify match scores are within expected ranges
    confirmed_stats = stats[stats["status"] == "CONFIRMED"]
    assert confirmed_stats["avg_score"].iloc[0] >= 0.7, "Average score for confirmed matches too low"

def test_master_record_quality(db_session, setup_db):
    """Test quality of master records."""
    # Get master record statistics using newer SQLAlchemy API
    result = db_session.execute(
        text("""
            SELECT
                COUNT(*) as total_masters,
                AVG(record_count) as avg_sources,
                AVG(confidence_score) as avg_confidence
            FROM mdm.master_records
            WHERE is_active = true
        """)
    )
    stats = pd.DataFrame(result.fetchall(), columns=["total_masters", "avg_sources", "avg_confidence"])
    
    # Verify we have master records
    assert not stats.empty, "No master records found"
    assert stats.iloc[0]["total_masters"] > 0, "No active master records"
    assert stats.iloc[0]["avg_sources"] >= 1.0, "Average sources per master record too low"
    assert stats.iloc[0]["avg_confidence"] >= 0.7, "Average confidence score too low" 