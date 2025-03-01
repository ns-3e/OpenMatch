"""
Optimized database operations for matching and merging records at scale.
"""
from typing import List, Dict, Any, Optional, Tuple, Set
import pandas as pd
import numpy as np
from sqlalchemy import text, bindparam, create_engine
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import time
from datetime import datetime, timedelta
from .engine import MatchEngine
from ..connectors import SourceRecord, MatchResult, MasterRecord, MergeHistory

class DatabaseOptimizer:
    """Handles database optimizations for large-scale matching."""
    
    def __init__(self, session: Session):
        self.session = session
    
    def setup_job_tracking_tables(self):
        """Set up tables for job tracking and state management."""
        self.session.execute(text("""
            -- Job instances table
            CREATE TABLE IF NOT EXISTS mdm.job_instances (
                job_id SERIAL PRIMARY KEY,
                job_type VARCHAR(50) NOT NULL,  -- 'MATCH' or 'MERGE'
                status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',  -- 'RUNNING', 'COMPLETED', 'FAILED'
                start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                config JSONB,  -- Store job configuration
                metrics JSONB,  -- Store job metrics
                created_by VARCHAR(100),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            -- Match state tracking table
            CREATE TABLE IF NOT EXISTS mdm.record_states (
                record_id BIGINT PRIMARY KEY REFERENCES mdm.source_records(id),
                match_state VARCHAR(20) NOT NULL DEFAULT 'UNMATCHED',  -- 'UNMATCHED', 'MATCHED', 'MERGED'
                last_job_id BIGINT REFERENCES mdm.job_instances(job_id),
                last_matched_at TIMESTAMP,
                last_merged_at TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            -- Match pairs logging table
            CREATE TABLE IF NOT EXISTS mdm.match_pairs (
                pair_id SERIAL PRIMARY KEY,
                job_id BIGINT NOT NULL REFERENCES mdm.job_instances(job_id),
                record_id_1 BIGINT NOT NULL REFERENCES mdm.source_records(id),
                record_id_2 BIGINT NOT NULL REFERENCES mdm.source_records(id),
                match_model_id VARCHAR(100),  -- Reference to the model used
                match_rule_id VARCHAR(100),   -- Reference to the rule that caused the match
                match_score FLOAT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(record_id_1, record_id_2)  -- Prevent duplicate pairs
            );

            -- Create indexes for performance
            CREATE INDEX IF NOT EXISTS idx_job_instances_type_status 
            ON mdm.job_instances (job_type, status, start_time DESC);

            CREATE INDEX IF NOT EXISTS idx_record_states_match_state 
            ON mdm.record_states (match_state)
            INCLUDE (last_job_id, last_matched_at);

            CREATE INDEX IF NOT EXISTS idx_match_pairs_job 
            ON mdm.match_pairs (job_id, match_score DESC);

            CREATE INDEX IF NOT EXISTS idx_match_pairs_records 
            ON mdm.match_pairs (record_id_1, record_id_2);
        """))
        self.session.commit()
    
    def setup_partitions(self):
        """Set up table partitioning for better performance."""
        self.session.execute(text("""
            -- Convert source_records to partitioned table if not already
            CREATE TABLE IF NOT EXISTS mdm.source_records_new (
                LIKE mdm.source_records INCLUDING ALL
            ) PARTITION BY RANGE (created_at);
            
            -- Create partitions by month for the last year
            DO $$
            BEGIN
                FOR i IN 0..11 LOOP
                    EXECUTE format(
                        'CREATE TABLE IF NOT EXISTS mdm.source_records_p%s 
                         PARTITION OF mdm.source_records_new 
                         FOR VALUES FROM (%L) TO (%L)',
                        to_char(CURRENT_DATE - (interval '1 month' * i), 'YYYYMM'),
                        quote_literal(date_trunc('month', CURRENT_DATE - (interval '1 month' * i))),
                        quote_literal(date_trunc('month', CURRENT_DATE - (interval '1 month' * (i-1))))
                    );
                END LOOP;
            END $$;
            
            -- Create default partition for older data
            CREATE TABLE IF NOT EXISTS mdm.source_records_default 
            PARTITION OF mdm.source_records_new DEFAULT;
        """))
        
        # Create partitioned match results table
        self.session.execute(text("""
            CREATE TABLE IF NOT EXISTS mdm.match_results_new (
                LIKE mdm.match_results INCLUDING ALL
            ) PARTITION BY RANGE (created_at);
            
            DO $$
            BEGIN
                FOR i IN 0..11 LOOP
                    EXECUTE format(
                        'CREATE TABLE IF NOT EXISTS mdm.match_results_p%s 
                         PARTITION OF mdm.match_results_new 
                         FOR VALUES FROM (%L) TO (%L)',
                        to_char(CURRENT_DATE - (interval '1 month' * i), 'YYYYMM'),
                        quote_literal(date_trunc('month', CURRENT_DATE - (interval '1 month' * i))),
                        quote_literal(date_trunc('month', CURRENT_DATE - (interval '1 month' * (i-1))))
                    );
                END LOOP;
            END $$;
            
            CREATE TABLE IF NOT EXISTS mdm.match_results_default 
            PARTITION OF mdm.match_results_new DEFAULT;
        """))
        
        self.session.commit()
    
    def create_materialized_views(self):
        """Create materialized views for frequently accessed data patterns."""
        self.session.execute(text("""
            -- Materialized view for match statistics
            CREATE MATERIALIZED VIEW IF NOT EXISTS mdm.match_statistics AS
            SELECT 
                date_trunc('day', created_at) as match_date,
                match_type,
                status,
                COUNT(*) as match_count,
                AVG(match_score) as avg_score,
                MIN(match_score) as min_score,
                MAX(match_score) as max_score
            FROM mdm.match_results
            GROUP BY 1, 2, 3;
            
            -- Materialized view for blocking effectiveness
            CREATE MATERIALIZED VIEW IF NOT EXISTS mdm.blocking_statistics AS
            SELECT 
                block_key,
                COUNT(*) as block_size,
                COUNT(DISTINCT source_system) as distinct_sources,
                MIN(created_at) as first_record,
                MAX(created_at) as last_record
            FROM mdm.source_records
            GROUP BY block_key;
            
            -- Create indexes on materialized views
            CREATE INDEX IF NOT EXISTS idx_match_stats_date 
            ON mdm.match_statistics (match_date);
            
            CREATE INDEX IF NOT EXISTS idx_blocking_stats_size 
            ON mdm.blocking_statistics (block_size DESC);
        """))
        
        self.session.commit()
    
    def refresh_materialized_views(self, concurrent: bool = True):
        """Refresh materialized views."""
        refresh_query = "REFRESH MATERIALIZED VIEW CONCURRENTLY" if concurrent else "REFRESH MATERIALIZED VIEW"
        
        self.session.execute(text(f"{refresh_query} mdm.match_statistics"))
        self.session.execute(text(f"{refresh_query} mdm.blocking_statistics"))
        self.session.commit()
    
    def optimize_tables(self):
        """Optimize database tables and indexes."""
        # Add optimized indexes
        self.session.execute(text("""
            -- Partial index for active records
            CREATE INDEX IF NOT EXISTS idx_source_records_active 
            ON mdm.source_records (created_at DESC)
            WHERE is_active = true;
            
            -- Covering index for blocking
            CREATE INDEX IF NOT EXISTS idx_source_records_blocking 
            ON mdm.source_records (block_key, id)
            INCLUDE (record_data, source_system)
            WHERE is_active = true;
            
            -- BRIN index for date ranges (very efficient for time-series data)
            CREATE INDEX IF NOT EXISTS idx_source_records_brin 
            ON mdm.source_records USING BRIN (created_at)
            WITH (pages_per_range = 128);
            
            -- GiST index for fuzzy text search
            CREATE INDEX IF NOT EXISTS idx_source_records_fuzzy
            ON mdm.source_records USING gist (
                (lower(record_data->>'first_name') || ' ' || 
                 lower(record_data->>'last_name')) gist_trgm_ops
            );
            
            -- Composite index for match results
            CREATE INDEX IF NOT EXISTS idx_match_results_composite
            ON mdm.match_results (status, match_type, match_score DESC)
            WHERE status = 'PENDING';
            
            -- Hash index for quick lookups
            CREATE INDEX IF NOT EXISTS idx_master_records_hash
            ON mdm.master_records USING hash (entity_type);
        """))
        
        # Update table statistics
        self.session.execute(text("""
            ANALYZE mdm.source_records;
            ANALYZE mdm.match_results;
            ANALYZE mdm.master_records;
            ANALYZE mdm.merge_history;
        """))
        
        # Configure table autovacuum
        self.session.execute(text("""
            ALTER TABLE mdm.source_records SET (
                autovacuum_vacuum_scale_factor = 0.05,
                autovacuum_analyze_scale_factor = 0.02
            );
            
            ALTER TABLE mdm.match_results SET (
                autovacuum_vacuum_scale_factor = 0.05,
                autovacuum_analyze_scale_factor = 0.02
            );
        """))
        
        self.session.commit()

class BatchProcessor:
    """Handles batch processing of records with optimizations for large datasets."""
    
    def __init__(
        self,
        session: Session,
        match_engine: MatchEngine,
        batch_size: int = 10000,
        max_workers: int = None,
        use_processes: bool = False,
        job_config: Dict[str, Any] = None,
        created_by: str = None
    ):
        self.session = session
        self.match_engine = match_engine
        self.batch_size = batch_size
        self.max_workers = max_workers or min(32, multiprocessing.cpu_count() * 2)
        self.use_processes = use_processes
        self.stats = {"processed": 0, "matches": 0, "start_time": time.time()}
        self.job_id = None
        self.job_config = job_config or {}
        self.created_by = created_by
    
    def _create_temp_tables(self):
        """Create optimized temporary tables for batch processing."""
        self.session.execute(text("""
            -- Temporary table for match results with optimized structure
            CREATE UNLOGGED TABLE IF NOT EXISTS temp_matches (
                source_record_id BIGINT,
                matched_record_id BIGINT,
                match_score FLOAT,
                match_type VARCHAR(20),
                match_details JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source_record_id, matched_record_id)
            ) WITH (autovacuum_enabled = false);
            
            -- Create indexes optimized for our access patterns
            CREATE INDEX IF NOT EXISTS temp_matches_score_idx 
            ON temp_matches (match_score DESC, match_type)
            WHERE match_score >= 0.7;
            
            -- Hash index for quick lookups
            CREATE INDEX IF NOT EXISTS temp_matches_source_hash
            ON temp_matches USING hash (source_record_id);
            
            -- BRIN index for time-based queries
            CREATE INDEX IF NOT EXISTS temp_matches_time_brin
            ON temp_matches USING brin (created_at);
        """))
    
    def _get_blocking_query(self, blocking_keys: List[str]) -> str:
        """Generate optimized SQL query for blocking with improved performance."""
        key_conditions = []
        for key in blocking_keys:
            # Handle different field types with optimized expressions
            key_conditions.append(f"""
                CASE 
                    WHEN jsonb_typeof(record_data->'{key}') = 'number' 
                    THEN (record_data->'{key}')::numeric / 10 
                    WHEN jsonb_typeof(record_data->'{key}') = 'string' 
                    THEN LEFT(LOWER(record_data->>{key}), 3)
                    ELSE NULL 
                END
            """)
        
        return f"""
        WITH RECURSIVE 
        blocked_records AS (
            SELECT 
                id,
                record_data,
                source_system,
                CONCAT({', '.join(key_conditions)}) as block_key
            FROM mdm.source_records
            WHERE is_active = true
            AND created_at > CURRENT_DATE - INTERVAL '1 year'
        ),
        block_stats AS (
            SELECT 
                block_key,
                COUNT(*) as block_size
            FROM blocked_records
            GROUP BY block_key
            HAVING COUNT(*) > 1
            AND COUNT(*) <= :max_block_size
        )
        SELECT 
            r1.id as id1,
            r1.record_data as data1,
            r1.source_system as system1,
            r2.id as id2,
            r2.record_data as data2,
            r2.source_system as system2,
            r1.block_key
        FROM blocked_records r1
        JOIN block_stats bs ON r1.block_key = bs.block_key
        JOIN blocked_records r2 
            ON r1.block_key = r2.block_key 
            AND r1.id < r2.id
            AND r1.source_system <= r2.source_system
        WHERE r1.block_key IS NOT NULL
        ORDER BY bs.block_size, r1.block_key
        """
    
    def _process_record_batch(
        self,
        records: List[Tuple[int, int, Dict, Dict, str, str, str]]
    ) -> List[Dict[str, Any]]:
        """Process a batch of record pairs with optimized matching."""
        matches = []
        start_time = time.time()
        
        for id1, id2, data1, data2, system1, system2, block_key in records:
            # Quick pre-check using exact field matches
            if any(str(data1.get(field)) == str(data2.get(field))
                   for field in self.match_engine.config.blocking.blocking_keys):
                
                match_type, confidence = self.match_engine.match_records(
                    data1, data2, fast_mode=True
                )
                
                if match_type != MatchType.NO_MATCH:
                    # Verify with full matching for high-confidence pairs
                    if confidence >= 0.8:
                        match_type, confidence = self.match_engine.match_records(
                            data1, data2, fast_mode=False
                        )
                    
                    if match_type != MatchType.NO_MATCH:
                        matches.append({
                            "source_record_id": id1,
                            "matched_record_id": id2,
                            "match_score": float(confidence),
                            "match_type": match_type.value,
                            "match_details": {
                                "matched_fields": list(set(data1.keys()) & set(data2.keys())),
                                "blocking_matched": True,
                                "block_key": block_key,
                                "source_systems": [system1, system2]
                            }
                        })
        
        # Update statistics
        elapsed = time.time() - start_time
        with ThreadPoolExecutor() as executor:
            executor.submit(self._update_stats, len(records), len(matches), elapsed)
        
        return matches
    
    def _update_stats(self, records_processed: int, matches_found: int, elapsed: float):
        """Update processing statistics."""
        self.stats["processed"] += records_processed
        self.stats["matches"] += matches_found
        self.stats["last_batch_time"] = elapsed
        
        # Print progress every 100k records
        if self.stats["processed"] % 100000 == 0:
            total_elapsed = time.time() - self.stats["start_time"]
            print(f"\nProgress Update:")
            print(f"Processed: {self.stats['processed']:,} records")
            print(f"Found: {self.stats['matches']:,} matches")
            print(f"Rate: {self.stats['processed']/total_elapsed:,.0f} records/second")
            print(f"Match Rate: {(self.stats['matches']/max(1, self.stats['processed'])*100):.2f}%")
    
    def _start_job(self, job_type: str = 'MATCH') -> int:
        """Start a new job instance and return its ID."""
        result = self.session.execute(text("""
            INSERT INTO mdm.job_instances (
                job_type, config, created_by
            ) VALUES (
                :job_type, :config, :created_by
            ) RETURNING job_id
        """), {
            'job_type': job_type,
            'config': self.job_config,
            'created_by': self.created_by
        })
        job_id = result.scalar_one()
        self.session.commit()
        return job_id

    def _complete_job(self, success: bool = True):
        """Mark the current job as completed or failed."""
        if not self.job_id:
            return

        status = 'COMPLETED' if success else 'FAILED'
        self.session.execute(text("""
            UPDATE mdm.job_instances 
            SET status = :status,
                end_time = CURRENT_TIMESTAMP,
                metrics = :metrics
            WHERE job_id = :job_id
        """), {
            'status': status,
            'metrics': {
                'records_processed': self.stats['processed'],
                'matches_found': self.stats['matches'],
                'processing_time': time.time() - self.stats['start_time'],
                'batch_size': self.batch_size,
                'workers': self.max_workers
            },
            'job_id': self.job_id
        })
        self.session.commit()

    def _update_record_states(self, record_pairs: List[Dict[str, Any]]):
        """Update record states for matched pairs."""
        if not record_pairs:
            return

        # Extract all record IDs involved in matches
        record_ids = set()
        for pair in record_pairs:
            record_ids.add(pair['record_id_1'])
            record_ids.add(pair['record_id_2'])

        # Bulk upsert record states
        self.session.execute(text("""
            INSERT INTO mdm.record_states (
                record_id, match_state, last_job_id, last_matched_at
            )
            SELECT 
                id, 'MATCHED', :job_id, CURRENT_TIMESTAMP
            FROM unnest(:record_ids::bigint[]) AS id
            ON CONFLICT (record_id) DO UPDATE
            SET match_state = 'MATCHED',
                last_job_id = EXCLUDED.last_job_id,
                last_matched_at = EXCLUDED.last_matched_at,
                updated_at = CURRENT_TIMESTAMP
        """), {
            'job_id': self.job_id,
            'record_ids': list(record_ids)
        })

    def _log_match_pairs(self, matches: List[Dict[str, Any]]):
        """Log match pairs with their associated job, model, and rule IDs."""
        if not matches:
            return

        self.session.execute(text("""
            INSERT INTO mdm.match_pairs (
                job_id, record_id_1, record_id_2, 
                match_model_id, match_rule_id, match_score
            )
            SELECT 
                :job_id,
                (match.record_pair).record_id_1,
                (match.record_pair).record_id_2,
                match.model_id,
                match.rule_id,
                match.score
            FROM unnest(:matches::jsonb[]) AS match(record_pair, model_id, rule_id, score)
            ON CONFLICT (record_id_1, record_id_2) DO UPDATE
            SET match_score = EXCLUDED.match_score,
                match_model_id = EXCLUDED.match_model_id,
                match_rule_id = EXCLUDED.match_rule_id,
                created_at = CURRENT_TIMESTAMP
        """), {
            'job_id': self.job_id,
            'matches': [
                {
                    'record_pair': {'record_id_1': m['record_id_1'], 'record_id_2': m['record_id_2']},
                    'model_id': m.get('match_model_id'),
                    'rule_id': m.get('match_rule_id'),
                    'score': m['match_score']
                }
                for m in matches
            ]
        })

    def process_matches(self):
        """Enhanced process_matches with job tracking and state management."""
        try:
            self.job_id = self._start_job('MATCH')
            
            # Get unmatched or recently updated records for processing
            query = text("""
                SELECT r.* 
                FROM mdm.source_records r
                LEFT JOIN mdm.record_states s ON r.id = s.record_id
                WHERE s.record_id IS NULL  -- New unmatched records
                   OR (s.match_state = 'UNMATCHED')  -- Explicitly unmatched
                   OR (r.updated_at > s.updated_at)  -- Records updated since last match
                ORDER BY r.created_at DESC
                LIMIT :batch_size
            """)

            while True:
                records = self.session.execute(query, {'batch_size': self.batch_size}).fetchall()
                if not records:
                    break

                # Process batch and get match results
                match_results = self._process_record_batch(records)
                
                # Log match pairs and update record states
                self._log_match_pairs(match_results)
                self._update_record_states(match_results)
                
                # Update stats
                self.stats['processed'] += len(records)
                self.stats['matches'] += len(match_results)
                
                self.session.commit()

            self._complete_job(True)
            
        except Exception as e:
            self._complete_job(False)
            raise e

    def reset_matches(self, model_ids: List[str] = None, rule_ids: List[str] = None):
        """Reset match pairs and record states based on specified criteria."""
        try:
            self.job_id = self._start_job('RESET')
            
            conditions = []
            params = {}
            
            if model_ids:
                conditions.append("match_model_id = ANY(:model_ids)")
                params['model_ids'] = model_ids
                
            if rule_ids:
                conditions.append("match_rule_id = ANY(:rule_ids)")
                params['rule_ids'] = rule_ids
                
            where_clause = " OR ".join(conditions) if conditions else "TRUE"
            
            # Get affected record IDs
            affected_records_query = f"""
                SELECT DISTINCT record_id_1, record_id_2
                FROM mdm.match_pairs
                WHERE {where_clause}
            """
            
            affected_records = self.session.execute(text(affected_records_query), params).fetchall()
            record_ids = set()
            for r1, r2 in affected_records:
                record_ids.add(r1)
                record_ids.add(r2)
            
            if record_ids:
                # Reset record states
                self.session.execute(text("""
                    UPDATE mdm.record_states
                    SET match_state = 'UNMATCHED',
                        last_job_id = :job_id,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE record_id = ANY(:record_ids)
                """), {
                    'job_id': self.job_id,
                    'record_ids': list(record_ids)
                })
                
                # Delete match pairs
                self.session.execute(text(f"""
                    DELETE FROM mdm.match_pairs
                    WHERE {where_clause}
                """), params)
                
                self.session.commit()
            
            self._complete_job(True)
            
        except Exception as e:
            self._complete_job(False)
            raise e

    def create_match_results(self):
        """Create final match results with optimized bulk operations."""
        print("\nCreating match results...")
        self.session.execute(text("""
            INSERT INTO mdm.match_results
            (source_record_id, matched_record_id, match_score,
             match_type, match_details, status, created_at)
            SELECT 
                source_record_id,
                matched_record_id,
                match_score,
                match_type,
                match_details,
                CASE 
                    WHEN match_score >= 0.8 THEN 'CONFIRMED'
                    ELSE 'PENDING'
                END as status,
                created_at
            FROM temp_matches
            ON CONFLICT (source_record_id, matched_record_id) 
            DO UPDATE SET
                match_score = EXCLUDED.match_score,
                match_type = EXCLUDED.match_type,
                match_details = EXCLUDED.match_details || 
                              jsonb_build_object('updated_at', CURRENT_TIMESTAMP),
                status = EXCLUDED.status,
                updated_at = CURRENT_TIMESTAMP
            WHERE match_results.match_score < EXCLUDED.match_score
        """))
        
        self.session.commit()
    
    def create_master_records(self):
        """Create master records using optimized clustering."""
        print("\nCreating master records...")
        self.session.execute(text("""
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
                    cs.cluster_members as source_record_ids
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
                created_at
            )
            SELECT 
                entity_type,
                golden_record,
                confidence_score,
                record_count,
                source_record_ids,
                CURRENT_TIMESTAMP
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
                updated_at = CURRENT_TIMESTAMP
            WHERE mdm.master_records.confidence_score < EXCLUDED.confidence_score
        """))
        
        self.session.commit()
    
    def create_merge_history(self):
        """Create merge history with optimized operations."""
        print("\nCreating merge history...")
        self.session.execute(text("""
            INSERT INTO mdm.merge_history (
                master_record_id,
                merged_record_ids,
                merge_details,
                operator,
                created_at
            )
            SELECT 
                m.id as master_record_id,
                m.source_record_ids as merged_record_ids,
                jsonb_build_object(
                    'avg_match_score', m.confidence_score,
                    'merged_at', CURRENT_TIMESTAMP,
                    'merge_type', 'AUTOMATED',
                    'cluster_size', m.record_count,
                    'sources', m.golden_record->'sources'
                ) as merge_details,
                'system' as operator,
                CURRENT_TIMESTAMP
            FROM mdm.master_records m
            WHERE NOT EXISTS (
                SELECT 1 FROM mdm.merge_history h
                WHERE h.master_record_id = m.id
                AND h.merged_record_ids = m.source_record_ids
            )
        """))
        
        self.session.commit()

def process_matches(
    session: Session,
    match_engine: MatchEngine,
    batch_size: int = 10000,
    max_workers: int = None,
    use_processes: bool = False
) -> None:
    """
    Process matches using optimized batch operations for large datasets.
    
    Args:
        session: Database session
        match_engine: Configured match engine instance
        batch_size: Number of records to process in each batch
        max_workers: Number of parallel workers (default: 2 * CPU cores)
        use_processes: Whether to use processes instead of threads
    """
    start_time = time.time()
    
    # Initialize database optimizer
    print("Optimizing database...")
    optimizer = DatabaseOptimizer(session)
    optimizer.setup_partitions()
    optimizer.optimize_tables()
    optimizer.create_materialized_views()
    
    # Create processor and run matching pipeline
    print("\nInitializing batch processor...")
    processor = BatchProcessor(
        session, match_engine, batch_size, max_workers, use_processes
    )
    
    print("\nProcessing matches...")
    processor.process_matches()
    
    print("\nCreating match results...")
    processor.create_match_results()
    
    print("\nCreating master records...")
    processor.create_master_records()
    
    print("\nCreating merge history...")
    processor.create_merge_history()
    
    # Refresh materialized views
    print("\nRefreshing materialized views...")
    optimizer.refresh_materialized_views()
    
    # Print final statistics
    elapsed = time.time() - start_time
    stats = processor.stats
    print("\nProcessing Complete!")
    print(f"Total Time: {elapsed:.1f} seconds")
    print(f"Records Processed: {stats['processed']:,}")
    print(f"Matches Found: {stats['matches']:,}")
    print(f"Processing Rate: {stats['processed']/elapsed:,.0f} records/second")
    print(f"Match Rate: {(stats['matches']/max(1, stats['processed'])*100):.2f}%") 