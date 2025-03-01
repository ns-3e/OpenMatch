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
from .settings import DatabaseConfig, VectorBackend
from ..connectors import SourceRecord, MatchResult, MasterRecord, MergeHistory

class DatabaseOptimizer:
    """Handles database optimizations for large-scale matching."""
    
    def __init__(self, session: Session, config: DatabaseConfig):
        self.session = session
        self.config = config
        
    def setup_vector_extension(self):
        """Set up vector extension based on configured backend."""
        if self.config.vector_backend == VectorBackend.PGVECTOR:
            # Enable pgvector extension
            self.session.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            
            # Create vector operators index
            self.session.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {self.config.schema}.record_embeddings (
                    record_id BIGINT PRIMARY KEY REFERENCES {self.config.schema}.source_records(id),
                    embedding vector({self.config.vector_dimension}),
                    model_id VARCHAR(100),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create vector index based on configuration
            if self.config.vector_index_type == 'ivfflat':
                self.session.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS record_embeddings_vector_idx 
                    ON {self.config.schema}.record_embeddings 
                    USING ivfflat (embedding vector_cosine_ops)
                    WITH (lists = {self.config.vector_lists})
                """))
            elif self.config.vector_index_type == 'hnsw':
                self.session.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS record_embeddings_vector_idx 
                    ON {self.config.schema}.record_embeddings 
                    USING hnsw (embedding vector_cosine_ops)
                    WITH (m = 16, ef_construction = 64)
                """))
                
            # Create function for vector similarity search
            self.session.execute(text(f"""
                CREATE OR REPLACE FUNCTION {self.config.schema}.find_similar_records(
                    query_vector vector({self.config.vector_dimension}),
                    similarity_threshold float,
                    max_results integer
                )
                RETURNS TABLE (
                    record_id bigint,
                    similarity float
                )
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    RETURN QUERY
                    SELECT 
                        e.record_id,
                        1 - (e.embedding <=> query_vector) as similarity
                    FROM {self.config.schema}.record_embeddings e
                    WHERE 1 - (e.embedding <=> query_vector) >= similarity_threshold
                    ORDER BY similarity DESC
                    LIMIT max_results;
                END;
                $$;
            """))
            
            self.session.commit()
            
    def setup_job_tracking_tables(self):
        """Set up tables for job tracking and state management."""
        self.session.execute(text(f"""
            -- Job instances table
            CREATE TABLE IF NOT EXISTS {self.config.schema}.job_instances (
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
            CREATE TABLE IF NOT EXISTS {self.config.schema}.record_states (
                record_id BIGINT PRIMARY KEY REFERENCES {self.config.schema}.source_records(id),
                match_state VARCHAR(20) NOT NULL DEFAULT 'UNMATCHED',  -- 'UNMATCHED', 'MATCHED', 'MERGED'
                last_job_id BIGINT REFERENCES {self.config.schema}.job_instances(job_id),
                last_matched_at TIMESTAMP,
                last_merged_at TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            -- Match pairs logging table with vector support
            CREATE TABLE IF NOT EXISTS {self.config.schema}.match_pairs (
                pair_id SERIAL PRIMARY KEY,
                job_id BIGINT NOT NULL REFERENCES {self.config.schema}.job_instances(job_id),
                record_id_1 BIGINT NOT NULL REFERENCES {self.config.schema}.source_records(id),
                record_id_2 BIGINT NOT NULL REFERENCES {self.config.schema}.source_records(id),
                match_model_id VARCHAR(100),  -- Reference to the model used
                match_rule_id VARCHAR(100),   -- Reference to the rule that caused the match
                match_score FLOAT NOT NULL,
                vector_similarity FLOAT,  -- Store vector similarity score
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(record_id_1, record_id_2)  -- Prevent duplicate pairs
            );

            -- Create indexes for performance
            CREATE INDEX IF NOT EXISTS idx_job_instances_type_status 
            ON {self.config.schema}.job_instances (job_type, status, start_time DESC);

            CREATE INDEX IF NOT EXISTS idx_record_states_match_state 
            ON {self.config.schema}.record_states (match_state)
            INCLUDE (last_job_id, last_matched_at);

            CREATE INDEX IF NOT EXISTS idx_match_pairs_job 
            ON {self.config.schema}.match_pairs (job_id, match_score DESC);

            CREATE INDEX IF NOT EXISTS idx_match_pairs_records 
            ON {self.config.schema}.match_pairs (record_id_1, record_id_2);
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
        config: DatabaseConfig,
        batch_size: int = 10000,
        max_workers: int = None,
        use_processes: bool = False,
        job_config: Dict[str, Any] = None,
        created_by: str = None
    ):
        self.session = session
        self.match_engine = match_engine
        self.config = config
        self.batch_size = batch_size
        self.max_workers = max_workers or min(32, multiprocessing.cpu_count() * 2)
        self.use_processes = use_processes
        self.stats = {"processed": 0, "matches": 0, "start_time": time.time()}
        self.job_id = None
        self.job_config = job_config or {}
        self.created_by = created_by
    
    def store_embeddings(self, records: List[Dict[str, Any]], model_id: str):
        """Store record embeddings in the vector database."""
        embeddings = []
        for record in records:
            try:
                # Compute embedding tensor
                embedding = self.match_engine.compute_blocking_tensor(record)
                
                # Convert to list for database storage
                embeddings.append({
                    'record_id': record['id'],
                    'embedding': embedding.tolist(),
                    'model_id': model_id
                })
            except Exception as e:
                print(f"Warning: Failed to compute embedding for record {record.get('id')}: {str(e)}")
        
        if embeddings:
            # Batch insert embeddings
            self.session.execute(text(f"""
                INSERT INTO {self.config.schema}.record_embeddings (
                    record_id, embedding, model_id
                )
                SELECT 
                    data->>'record_id',
                    data->>'embedding',
                    data->>'model_id'
                FROM json_array_elements(:embeddings::json)
                ON CONFLICT (record_id) 
                DO UPDATE SET
                    embedding = EXCLUDED.embedding,
                    model_id = EXCLUDED.model_id,
                    created_at = CURRENT_TIMESTAMP
            """), {'embeddings': embeddings})
            self.session.commit()
    
    def find_vector_matches(
        self,
        query_record: Dict[str, Any],
        similarity_threshold: float = 0.8,
        max_results: int = 100
    ) -> List[Tuple[int, float]]:
        """Find similar records using vector similarity search."""
        try:
            # Compute query embedding
            query_embedding = self.match_engine.compute_blocking_tensor(query_record)
            
            # Use database vector search
            results = self.session.execute(text(f"""
                SELECT * FROM {self.config.schema}.find_similar_records(
                    :query_vector::vector,
                    :threshold,
                    :max_results
                )
            """), {
                'query_vector': query_embedding.tolist(),
                'threshold': similarity_threshold,
                'max_results': max_results
            })
            
            return [(row.record_id, float(row.similarity)) for row in results]
            
        except Exception as e:
            print(f"Warning: Vector search failed: {str(e)}")
            return []
    
    def process_matches(self):
        """Process matches using vector similarity search."""
        if not self.job_id:
            self.job_id = self._start_job()
        
        # Get unmatched records
        query = text(f"""
            SELECT id, record_data
            FROM {self.config.schema}.source_records sr
            WHERE NOT EXISTS (
                SELECT 1 
                FROM {self.config.schema}.record_states rs
                WHERE rs.record_id = sr.id
                AND rs.match_state IN ('MATCHED', 'MERGED')
            )
            ORDER BY id
            LIMIT :batch_size
        """)
        
        while True:
            records = self.session.execute(query, {'batch_size': self.batch_size}).fetchall()
            if not records:
                break
            
            # Store embeddings for batch
            self.store_embeddings(
                [{'id': r.id, **r.record_data} for r in records],
                self.match_engine.model_id
            )
            
            # Process each record
            for record in records:
                # Find vector matches
                vector_matches = self.find_vector_matches(
                    record.record_data,
                    similarity_threshold=self.config.match_threshold,
                    max_results=100
                )
                
                # Process matches
                for matched_id, similarity in vector_matches:
                    if matched_id != record.id:
                        matched_record = self.session.execute(text(f"""
                            SELECT record_data
                            FROM {self.config.schema}.source_records
                            WHERE id = :id
                        """), {'id': matched_id}).first()
                        
                        if matched_record:
                            # Apply match rules
                            match_type, score, rule_id = self.match_engine.match_records(
                                record.record_data,
                                matched_record.record_data
                            )
                            
                            if match_type != MatchType.NO_MATCH and score >= self.config.match_threshold:
                                # Store match result
                                self.session.execute(text(f"""
                                    INSERT INTO {self.config.schema}.match_pairs (
                                        job_id, record_id_1, record_id_2,
                                        match_model_id, match_rule_id,
                                        match_score, vector_similarity
                                    ) VALUES (
                                        :job_id, :id1, :id2,
                                        :model_id, :rule_id,
                                        :score, :similarity
                                    )
                                    ON CONFLICT (record_id_1, record_id_2) DO UPDATE SET
                                        match_score = GREATEST(
                                            {self.config.schema}.match_pairs.match_score,
                                            EXCLUDED.match_score
                                        ),
                                        vector_similarity = EXCLUDED.vector_similarity,
                                        updated_at = CURRENT_TIMESTAMP
                                """), {
                                    'job_id': self.job_id,
                                    'id1': min(record.id, matched_id),
                                    'id2': max(record.id, matched_id),
                                    'model_id': self.match_engine.model_id,
                                    'rule_id': rule_id,
                                    'score': float(score),
                                    'similarity': float(similarity)
                                })
                                
                                # Update record states
                                self.session.execute(text(f"""
                                    INSERT INTO {self.config.schema}.record_states (
                                        record_id, match_state, last_job_id, last_matched_at
                                    ) VALUES 
                                        (:id1, 'MATCHED', :job_id, CURRENT_TIMESTAMP),
                                        (:id2, 'MATCHED', :job_id, CURRENT_TIMESTAMP)
                                    ON CONFLICT (record_id) DO UPDATE SET
                                        match_state = EXCLUDED.match_state,
                                        last_job_id = EXCLUDED.last_job_id,
                                        last_matched_at = EXCLUDED.last_matched_at
                                """), {
                                    'id1': record.id,
                                    'id2': matched_id,
                                    'job_id': self.job_id
                                })
                                
                                self.stats["matches"] += 1
                
                self.stats["processed"] += 1
                if self.stats["processed"] % 1000 == 0:
                    self._update_stats(1000, self.stats["matches"], time.time() - self.stats["start_time"])
                    self.session.commit()
            
            # Commit batch
            self.session.commit()

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